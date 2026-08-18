#!/usr/bin/env python3
"""Performance log capture, parsing, and HTML reporting for CloudCP CLI transfers.

Adapted from schedular_test.py's capture/analysis infrastructure. Provides:

- JournalCapture: captures journalctl output during a transfer window
- CloudcpLogCapture: tails cloudcp.log during a transfer window
- Parsing functions for timeline, throughput, and batch completion data
- Self-contained HTML performance report generation with animated replay,
  throughput scatter/bars, histograms, and per-batch PERF data

Usage from cloud_cli_runner.py::

    collector = TransferPerfCollector(case_dir, cfg, dry_run)
    collector.start()
    # ... initiate_transfer, poll_until_terminal ...
    perf_data = collector.finish(transfer_id, csv_path=results_csv)
    # perf_data["html_report"] is the path to the generated report
"""
from __future__ import annotations

import ast
import csv
import datetime as _dt
import json
import logging
import os
import re
import signal
import subprocess
import threading
import time
import zipfile
from pathlib import Path
from typing import Any, Optional

LOG = logging.getLogger("cli_perf_capture")

# ---------------------------------------------------------------------------
# Regex patterns (from schedular_test.py)
# ---------------------------------------------------------------------------
# journalctl default timestamp: "Jul 31 05:15:33"
_TS_RE = re.compile(r"^([A-Z][a-z]{2}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2})\b")
_PENDING_RE = re.compile(r"Pending-(\d+)\s*:\s*(\{.*\})")
_RUNNING_RE = re.compile(r"Running with workers\s*:\s*(\{.*\})")
_FREE_RE = re.compile(r"free workers\s+(\d+)")

# cloudcp.log lines
_CLOUDCP_TS_RE = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+)")
_CLOUDCP_STATS_RE = re.compile(
    r"\[Stats\]\[(\d+)\]\s+SUMMARY\s+elapsed=([\d.]+)s\s+files=(\d+)\s+"
    r"small=(\d+)\s+large=(\d+)\s+skipped=(\d+)\s+bytes=(\d+)\s+\([^)]*\)\s+"
    r"files/sec=([\d.]+)\s+throughput=([\d.]+)\s+MiB/s"
)
_CLOUDCP_BATCH_RE = re.compile(r"\[Batch\]\[(\d+)\]\s+done\s+records=(\d+)")

# journal PERF lines from the broker
_PERF_RE = re.compile(
    r"PERF\s+batch=(\S+)\s+files_count:(\d+)\s+total_size:(\d+)\s+"
    r"upload=([\d.]+)s\s+total=([\d.]+)s\s+rc=(-?\d+)\s+batch_file:(\S+)"
)
_PERF_CAT_RE = re.compile(r"/completed/([^/]+)/")

TIER_COLORS = {
    "zero": "#7f8c8d",
    "tiny": "#3498db",
    "small": "#2ecc71",
    "medium": "#f39c12",
    "large": "#e74c3c",
}
DEFAULT_TIER_ORDER = ["zero", "tiny", "small", "medium", "large"]

FALLBACK_BATCH = {
    "ZERO": {"BATCH_SIZE": 2000, "TARGET_SIZE_MB": 0, "OPEN_BATCHES": 4},
    "TINY": {"BATCH_SIZE": 511, "TARGET_SIZE_MB": 256, "OPEN_BATCHES": 8},
    "SMALL": {"BATCH_SIZE": 317, "TARGET_SIZE_MB": 2048, "OPEN_BATCHES": 8},
    "MEDIUM": {"BATCH_SIZE": 50, "TARGET_SIZE_MB": 10240, "OPEN_BATCHES": 8},
    "LARGE": {"BATCH_SIZE": 5, "TARGET_SIZE_MB": 51200, "OPEN_BATCHES": 8},
}

# Approximate per-file byte sizes per tier (used to classify batches by tier).
TIER_FILE_SIZES: list[tuple[str, int]] = [
    ("zero", 0),
    ("tiny", 16384),
    ("small", 8388608),
    ("medium", 104857600),
    ("large", 1073741824),
]

DEF_JOURNAL_TAGS = ["bcloud", "bryckcloud"]
DEF_CLOUDCP_LOG = "/opt/bryck/bryckapi/downloads/cloud_transfer_logs/cloudcp.log"
DEF_CAPTURE_LEAD = 3
DEF_CAPTURE_DRAIN = 6


# ---------------------------------------------------------------------------
# Batch config helpers
# ---------------------------------------------------------------------------
def load_batch_config(path: str) -> dict:
    try:
        with open(path, encoding="utf-8") as fh:
            cfg = json.load(fh)
        batch = cfg.get("BATCH", {})
        tiers = {k: v for k, v in batch.items() if isinstance(v, dict) and "BATCH_SIZE" in v}
        if tiers:
            return tiers
    except Exception:  # noqa: BLE001
        LOG.debug("could not read BATCH config from %s, using fallback", path)
    return dict(FALLBACK_BATCH)


def tier_order_from_config(batch_cfg: dict) -> list[str]:
    order = [t.lower() for t in ("ZERO", "TINY", "SMALL", "MEDIUM", "LARGE") if t in batch_cfg]
    for k in batch_cfg:
        if k.lower() not in order:
            order.append(k.lower())
    return order or list(DEFAULT_TIER_ORDER)


# ---------------------------------------------------------------------------
# JournalCapture — captures all journal lines, filters during parse
# ---------------------------------------------------------------------------
class JournalCapture:
    """Spawn ``sudo journalctl -f`` and write all lines to a raw log file.

    Unlike the schedular_test.py version, filtering by transfer_id happens
    during parsing (since CLI runner learns the id only after initiate).
    """

    def __init__(self, tags: str | list[str], log_dir: Path, since: _dt.datetime, dry_run: bool,
                 lead_sec: float = DEF_CAPTURE_LEAD, drain_sec: float = DEF_CAPTURE_DRAIN):
        self.tags = [tags] if isinstance(tags, str) else list(tags)
        self.dry_run = dry_run
        self.lead_sec = lead_sec
        self.drain_sec = drain_sec
        self.raw_path = log_dir / "journal_raw.log"
        self._since = since
        self._proc: subprocess.Popen | None = None
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()

    def start(self) -> None:
        self.raw_path.write_text("", encoding="utf-8")
        if self.dry_run:
            LOG.info("[dry-run] would start journalctl follower for tags %s", self.tags)
            return
        since_dt = self._since - _dt.timedelta(seconds=max(self.lead_sec, 1))
        since = since_dt.strftime("%Y-%m-%d %H:%M:%S")
        tag_flags = [flag for tag in self.tags for flag in ("-t", tag)]
        cmd = ["sudo", "journalctl", "-f", *tag_flags, "--since", since, "-o", "short"]
        LOG.info("$ %s", " ".join(cmd))
        self._proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            text=True, bufsize=1, preexec_fn=os.setsid,
        )
        self._thread = threading.Thread(target=self._reader, daemon=True)
        self._thread.start()
        if self.lead_sec > 0:
            LOG.info("capture lead: waiting %.1fs for journalctl to attach", self.lead_sec)
            time.sleep(self.lead_sec)

    def _reader(self) -> None:
        raw = self.raw_path.open("a", encoding="utf-8")
        try:
            assert self._proc and self._proc.stdout
            for line in self._proc.stdout:
                if self._stop.is_set():
                    break
                raw.write(line)
                raw.flush()
        except Exception:  # noqa: BLE001
            pass
        finally:
            raw.close()

    def stop(self) -> None:
        if self.dry_run or self._proc is None:
            self._stop.set()
            return
        if self.drain_sec > 0:
            LOG.info("capture drain: keeping journalctl open %.1fs for tail logs", self.drain_sec)
            time.sleep(self.drain_sec)
        self._stop.set()
        try:
            os.killpg(os.getpgid(self._proc.pid), signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            self._proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(os.getpgid(self._proc.pid), signal.SIGKILL)
            except ProcessLookupError:
                pass
        if self._thread:
            self._thread.join(timeout=5)


# ---------------------------------------------------------------------------
# CloudcpLogCapture — tails cloudcp.log into a per-case file
# ---------------------------------------------------------------------------
class CloudcpLogCapture:
    """Tail cloudcp.log across the test window."""

    def __init__(self, log_path: str, out_path: Path, dry_run: bool):
        self.log_path = log_path
        self.out_path = out_path
        self.dry_run = dry_run
        self._proc: subprocess.Popen | None = None
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()

    def start(self) -> None:
        self.out_path.write_text("", encoding="utf-8")
        if self.dry_run:
            LOG.info("[dry-run] would tail %s -> %s", self.log_path, self.out_path.name)
            return
        cmd = ["sudo", "tail", "-F", "-n", "0", self.log_path]
        LOG.info("$ %s", " ".join(cmd))
        self._proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            text=True, bufsize=1, preexec_fn=os.setsid,
        )
        self._thread = threading.Thread(target=self._reader, daemon=True)
        self._thread.start()

    def _reader(self) -> None:
        out = self.out_path.open("a", encoding="utf-8")
        try:
            assert self._proc and self._proc.stdout
            for line in self._proc.stdout:
                if self._stop.is_set():
                    break
                out.write(line)
                out.flush()
        except Exception:  # noqa: BLE001
            pass
        finally:
            out.close()

    def stop(self) -> None:
        self._stop.set()
        if self.dry_run or self._proc is None:
            return
        try:
            os.killpg(os.getpgid(self._proc.pid), signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            self._proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(os.getpgid(self._proc.pid), signal.SIGKILL)
            except ProcessLookupError:
                pass
        if self._thread:
            self._thread.join(timeout=5)


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def _parse_ts(line: str, year: int) -> _dt.datetime | None:
    m = _TS_RE.match(line)
    if not m:
        return None
    try:
        return _dt.datetime.strptime(f"{year} {m.group(1)}", "%Y %b %d %H:%M:%S")
    except ValueError:
        return None


def _safe_dict(text: str) -> dict:
    try:
        d = ast.literal_eval(text)
        if isinstance(d, dict):
            return {str(k).lower(): int(v) for k, v in d.items()}
    except Exception:  # noqa: BLE001
        pass
    return {}


def _minmaxavg(vals: list[float]) -> dict:
    if not vals:
        return {"min": 0.0, "max": 0.0, "avg": 0.0, "median": 0.0}
    s = sorted(vals)
    n = len(s)
    mid = n // 2
    median = s[mid] if n % 2 else (s[mid - 1] + s[mid]) / 2
    return {"min": s[0], "max": s[-1], "avg": sum(s) / n, "median": median}


def _mode_rounded(vals: list[float], ndigits: int = 1) -> float:
    if not vals:
        return 0.0
    counts: dict[float, int] = {}
    for v in vals:
        r = round(v, ndigits)
        counts[r] = counts.get(r, 0) + 1
    best = max(counts.values())
    return min(k for k, c in counts.items() if c == best)


def _histogram(values: list[float], nbins: int = 40) -> dict:
    if not values:
        return {"bins": [], "counts": [], "width": 0}
    lo, hi = min(values), max(values)
    if hi <= lo:
        hi = lo + 1.0
    width = (hi - lo) / nbins
    counts = [0] * nbins
    for v in values:
        idx = min(int((v - lo) / width), nbins - 1)
        counts[idx] += 1
    bins = [lo + i * width for i in range(nbins)]
    return {"bins": bins, "counts": counts, "width": width, "lo": lo, "hi": hi}


def _tier_for_avg_size(avg_size: float, tier_sizes: list[tuple[str, int]]) -> str:
    if avg_size <= 0:
        for name, sz in tier_sizes:
            if sz == 0:
                return name
    best, best_ratio = None, None
    for name, sz in tier_sizes:
        if sz <= 0:
            continue
        ratio = (avg_size / sz) if avg_size >= sz else (sz / avg_size if avg_size else 1e18)
        if best_ratio is None or ratio < best_ratio:
            best, best_ratio = name, ratio
    return best or (tier_sizes[0][0] if tier_sizes else "unknown")


# ---------------------------------------------------------------------------
# Parse raw journal for a specific transfer_id
# ---------------------------------------------------------------------------
def parse_journal_for_transfer(raw_path: Path, transfer_id: str, year: int) -> dict:
    """Parse the raw journal log, filtering for a specific transfer_id.

    Returns merged, forward-filled timeline + raw event counts.
    """
    pending_events, running_events, free_events = [], [], []
    text = raw_path.read_text(encoding="utf-8", errors="replace") if raw_path.is_file() else ""

    for line in text.splitlines():
        ts = _parse_ts(line, year)
        if not ts:
            continue
        # Pending-<id>
        m = _PENDING_RE.search(line)
        if m and m.group(1) == str(transfer_id):
            pending_events.append((ts, _safe_dict(m.group(2))))
            continue
        # Running with workers
        m = _RUNNING_RE.search(line)
        if m:
            running_events.append((ts, _safe_dict(m.group(1))))
            continue
        # free workers
        m = _FREE_RE.search(line)
        if m:
            free_events.append((ts, int(m.group(1))))

    all_ts = sorted(
        {ts for ts, _ in pending_events}
        | {ts for ts, _ in running_events}
        | {ts for ts, _ in free_events}
    )
    timeline = []
    if all_ts:
        t0 = all_ts[0]
        pi = ri = fi = 0
        cur_pending, cur_running, cur_free = {}, {}, 0
        for ts in all_ts:
            while pi < len(pending_events) and pending_events[pi][0] <= ts:
                cur_pending = pending_events[pi][1]
                pi += 1
            while ri < len(running_events) and running_events[ri][0] <= ts:
                cur_running = running_events[ri][1]
                ri += 1
            while fi < len(free_events) and free_events[fi][0] <= ts:
                cur_free = free_events[fi][1]
                fi += 1
            timeline.append({
                "t": (ts - t0).total_seconds(),
                "iso": ts.isoformat(),
                "pending": dict(cur_pending),
                "running": dict(cur_running),
                "free": cur_free,
            })

    tiers_seen: set[str] = set()
    for _, d in pending_events:
        tiers_seen |= set(d)
    for _, d in running_events:
        tiers_seen |= set(d)

    return {
        "timeline": timeline,
        "tiers_seen": sorted(tiers_seen),
        "counts": {
            "pending_events": len(pending_events),
            "running_events": len(running_events),
            "free_events": len(free_events),
        },
    }


# ---------------------------------------------------------------------------
# Parse PERF lines from raw journal for a specific transfer_id
# ---------------------------------------------------------------------------
def parse_perf_from_journal(raw_path: Path, transfer_id: str,
                            tier_order: list[str]) -> dict:
    """Parse broker PERF lines from the raw journal for a given transfer_id."""
    empty: dict[str, Any] = {"batches": [], "per_tier": {}, "overall": {}, "tiers": []}
    text = raw_path.read_text(encoding="utf-8", errors="replace") if raw_path.is_file() else ""

    batches: list[dict] = []
    filter_str = f"transfer_{transfer_id}"
    for line in text.splitlines():
        if filter_str not in line:
            continue
        m = _PERF_RE.search(line)
        if not m:
            continue
        cm = _PERF_CAT_RE.search(m.group(7))
        tier = cm.group(1).lower() if cm else "unknown"
        batches.append({
            "batch": m.group(1),
            "tier": tier,
            "files": int(m.group(2)),
            "bytes": int(m.group(3)),
            "upload": round(float(m.group(4)), 2),
            "total": round(float(m.group(5)), 2),
            "rc": int(m.group(6)),
            "order": len(batches),
        })
    if not batches:
        return empty

    def _stats(rows: list[dict]) -> dict:
        totals = [b["total"] for b in rows]
        uploads = [b["upload"] for b in rows]
        return {
            "batches": len(rows),
            "files": sum(b["files"] for b in rows),
            "bytes": sum(b["bytes"] for b in rows),
            "total_min": round(min(totals), 2), "total_max": round(max(totals), 2),
            "total_avg": round(sum(totals) / len(totals), 2),
            "total_mode": round(_mode_rounded(totals), 1),
            "upload_min": round(min(uploads), 2), "upload_max": round(max(uploads), 2),
            "upload_avg": round(sum(uploads) / len(uploads), 2),
            "upload_mode": round(_mode_rounded(uploads), 1),
            "rc_fail": sum(1 for b in rows if b["rc"] != 0),
        }

    order = tier_order + [t for t in {b["tier"] for b in batches} if t not in tier_order]
    per_tier: dict[str, dict] = {}
    for tier in order:
        tb = [b for b in batches if b["tier"] == tier]
        if tb:
            per_tier[tier] = _stats(tb)

    return {
        "batches": batches,
        "per_tier": per_tier,
        "overall": _stats(batches),
        "tiers": [t for t in order if t in per_tier],
    }


# ---------------------------------------------------------------------------
# Parse cloudcp.log throughput
# ---------------------------------------------------------------------------
def parse_cloudcp_log(path: Path, tier_order: list[str],
                      tier_sizes: list[tuple[str, int]] | None = None) -> dict:
    """Parse per-batch throughput from cloudcp.log (or cloudcplogs.txt)."""
    if tier_sizes is None:
        tier_sizes = list(TIER_FILE_SIZES)
    empty: dict[str, Any] = {"batches": [], "per_tier": {}, "overall": {}, "tiers": []}
    if not path.is_file():
        return empty
    text = path.read_text(encoding="utf-8", errors="replace")

    batch_done: dict[str, _dt.datetime] = {}
    stats: list[dict] = []
    for line in text.splitlines():
        tsm = _CLOUDCP_TS_RE.match(line)
        ts = None
        if tsm:
            try:
                ts = _dt.datetime.strptime(tsm.group(1), "%Y-%m-%d %H:%M:%S.%f")
            except ValueError:
                ts = None
        bm = _CLOUDCP_BATCH_RE.search(line)
        if bm and ts:
            batch_done[bm.group(1)] = ts
            continue
        sm = _CLOUDCP_STATS_RE.search(line)
        if sm and ts:
            files = int(sm.group(3))
            nbytes = int(sm.group(7))
            avg_size = (nbytes / files) if files else 0.0
            stats.append({
                "pid": sm.group(1), "stats_ts": ts,
                "elapsed": float(sm.group(2)),
                "files": files, "bytes": nbytes,
                "files_sec": float(sm.group(8)),
                "throughput": float(sm.group(9)) * 1.048576,  # MiB/s -> MB/s
                "tier": _tier_for_avg_size(avg_size, tier_sizes),
            })
    if not stats:
        return empty

    for s in stats:
        s["done_ts"] = batch_done.get(s["pid"], s["stats_ts"])
        s["start_ts"] = s["done_ts"] - _dt.timedelta(seconds=s["elapsed"])
    t0 = min(s["start_ts"] for s in stats)

    batches = []
    for s in sorted(stats, key=lambda x: x["done_ts"]):
        batches.append({
            "tier": s["tier"],
            "t_start": round((s["start_ts"] - t0).total_seconds(), 2),
            "t_done": round((s["done_ts"] - t0).total_seconds(), 2),
            "elapsed": round(s["elapsed"], 2),
            "files": s["files"], "bytes": s["bytes"],
            "files_sec": round(s["files_sec"], 2),
            "throughput": round(s["throughput"], 3),
        })

    order = tier_order + [t for t in {b["tier"] for b in batches} if t not in tier_order]
    per_tier: dict[str, dict] = {}
    for tier in order:
        tb = [b for b in batches if b["tier"] == tier]
        if not tb:
            continue
        st = _minmaxavg([b["throughput"] for b in tb])
        el = _minmaxavg([b["elapsed"] for b in tb])
        tot_bytes = sum(b["bytes"] for b in tb)
        span = max(b["t_done"] for b in tb) - min(b["t_start"] for b in tb)
        per_tier[tier] = {
            "batches": len(tb),
            "files": sum(b["files"] for b in tb),
            "bytes": tot_bytes,
            "min": round(st["min"], 3), "max": round(st["max"], 3),
            "avg": round(st["avg"], 3), "median": round(st["median"], 3),
            "avg_files_sec": round(sum(b["files_sec"] for b in tb) / len(tb), 2),
            "aggregate_mb_s": round((tot_bytes / 1e6 / span) if span > 0 else 0.0, 3),
            "batches_per_sec": round((len(tb) / span) if span > 0 else 0.0, 3),
            "elapsed_min": round(el["min"], 2), "elapsed_max": round(el["max"], 2),
            "elapsed_avg": round(el["avg"], 2), "elapsed_median": round(el["median"], 2),
        }

    ov = _minmaxavg([b["throughput"] for b in batches])
    el = _minmaxavg([b["elapsed"] for b in batches])
    all_bytes = sum(b["bytes"] for b in batches)
    wall = max(b["t_done"] for b in batches) - min(b["t_start"] for b in batches)
    overall = {
        "batches": len(batches), "bytes": all_bytes,
        "min": round(ov["min"], 3), "max": round(ov["max"], 3),
        "avg": round(ov["avg"], 3), "median": round(ov["median"], 3),
        "aggregate_mb_s": round((all_bytes / 1e6 / wall) if wall > 0 else 0.0, 3),
        "batches_per_sec": round((len(batches) / wall) if wall > 0 else 0.0, 3),
        "elapsed_min": round(el["min"], 2), "elapsed_max": round(el["max"], 2),
        "elapsed_avg": round(el["avg"], 2), "elapsed_median": round(el["median"], 2),
        "wall_sec": round(wall, 2),
    }
    return {"batches": batches, "per_tier": per_tier, "overall": overall,
            "tiers": [t for t in order if t in per_tier]}


# ---------------------------------------------------------------------------
# Parse transfer results CSV
# ---------------------------------------------------------------------------
def parse_results_csv(csv_path: Path) -> dict:
    empty = {"total": 0, "status_counts": {}, "success": 0, "failed": 0,
             "total_bytes": 0, "completions_rel": [], "completion_span_sec": 0}
    if not csv_path.is_file():
        return empty
    rows: list[dict] = []
    completions: list[_dt.datetime] = []
    status_counts: dict[str, int] = {}
    total_bytes = 0
    with csv_path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            rows.append(row)
            status = (row.get("status") or "").strip().upper()
            status_counts[status] = status_counts.get(status, 0) + 1
            try:
                total_bytes += int(row.get("size") or 0)
            except ValueError:
                pass
            fin = (row.get("finished_at") or "").strip()
            if fin:
                try:
                    completions.append(_dt.datetime.fromisoformat(fin))
                except ValueError:
                    pass
    completions.sort()
    comp_rel: list[float] = []
    if completions:
        t0 = completions[0]
        comp_rel = [(c - t0).total_seconds() for c in completions]
    return {
        "total": len(rows),
        "status_counts": status_counts,
        "success": status_counts.get("SUCCESS", 0),
        "failed": sum(v for k, v in status_counts.items() if k not in ("SUCCESS",)),
        "total_bytes": total_bytes,
        "completions_rel": comp_rel,
        "completion_span_sec": (comp_rel[-1] - comp_rel[0]) if comp_rel else 0,
    }


# ---------------------------------------------------------------------------
# TransferPerfCollector — high-level wrapper
# ---------------------------------------------------------------------------
class TransferPerfCollector:
    """Manages performance capture for a single transfer case.

    Create before initiating the transfer, call ``start()``, then after the
    transfer reaches a terminal state call ``finish()`` with the transfer id.
    """

    def __init__(self, case_dir: Path, cfg: dict, dry_run: bool):
        """
        Parameters
        ----------
        case_dir : Path
            Per-case output directory (e.g. ``results/<RUN_ID>/<TEST_ID>``).
        cfg : dict
            The plan's ``config`` block, expected to contain:
            journal_tag, cloudcp_log, capture_lead, capture_drain,
            transfer_logs_dir, bryck_config_json.
        dry_run : bool
        """
        self.case_dir = case_dir
        self.cfg = cfg
        self.dry_run = dry_run

        self.perf_dir = case_dir / "perf"
        self.perf_dir.mkdir(parents=True, exist_ok=True)

        self._start_dt: _dt.datetime | None = None
        self._end_dt: _dt.datetime | None = None
        self._journal: JournalCapture | None = None
        self._cloudcp: CloudcpLogCapture | None = None

    def start(self) -> None:
        """Start journal + cloudcp.log capture. Call BEFORE initiating transfer."""
        self._start_dt = _dt.datetime.now()
        tags = self.cfg.get("journal_tag") or DEF_JOURNAL_TAGS
        lead = self.cfg.get("capture_lead", DEF_CAPTURE_LEAD)
        drain = self.cfg.get("capture_drain", DEF_CAPTURE_DRAIN)
        cloudcp_log = self.cfg.get("cloudcp_log", DEF_CLOUDCP_LOG)

        self._cloudcp = CloudcpLogCapture(
            cloudcp_log, self.perf_dir / "cloudcplogs.txt", self.dry_run)
        self._cloudcp.start()

        self._journal = JournalCapture(
            tags, self.perf_dir, self._start_dt, self.dry_run,
            lead_sec=lead, drain_sec=drain)
        self._journal.start()

    def finish(self, transfer_id: str, csv_path: Path | None = None,
               test_id: str = "", tier: str = "", mode: str = "",
               description: str = "", gen_summary: dict | None = None) -> dict:
        """Stop capture, parse logs, generate HTML report.

        Returns a dict with performance data and the path to the HTML report.
        """
        self._end_dt = _dt.datetime.now()

        if self._journal:
            self._journal.stop()
        if self._cloudcp:
            self._cloudcp.stop()

        year = self._start_dt.year if self._start_dt else _dt.datetime.now().year
        batch_cfg = load_batch_config(self.cfg.get("bryck_config_json", ""))
        tier_order = tier_order_from_config(batch_cfg)

        # Parse journal for timeline
        raw_path = self.perf_dir / "journal_raw.log"
        journal_data = parse_journal_for_transfer(raw_path, str(transfer_id), year)

        # Parse PERF lines from journal
        perf_data = parse_perf_from_journal(raw_path, str(transfer_id), tier_order)

        # Parse cloudcp.log throughput
        cloudcp_path = self.perf_dir / "cloudcplogs.txt"
        throughput = parse_cloudcp_log(cloudcp_path, tier_order)

        # Parse results CSV
        csv_summary = parse_results_csv(csv_path) if csv_path and csv_path.is_file() else {
            "total": 0, "status_counts": {}, "success": 0, "failed": 0,
            "total_bytes": 0, "completions_rel": [], "completion_span_sec": 0}

        # Build meta
        start_iso = self._start_dt.isoformat(timespec="seconds") if self._start_dt else ""
        end_iso = self._end_dt.isoformat(timespec="seconds") if self._end_dt else ""
        duration = round((self._end_dt - self._start_dt).total_seconds(), 1) if (
            self._start_dt and self._end_dt) else 0.0

        meta = {
            "test_id": test_id,
            "transfer_id": str(transfer_id),
            "tier": tier,
            "mode": mode,
            "description": description,
            "start": start_iso,
            "end": end_iso,
            "duration_sec": duration,
        }

        # Dataset info
        dataset_info = {
            "tier": tier,
            "mode": mode,
            "actual_files": (gen_summary or {}).get("actual_files", 0),
            "expected_files": (gen_summary or {}).get("expected_files", 0),
            "dataset_root": (gen_summary or {}).get("dataset_root", ""),
        }

        tiers = list(dict.fromkeys(
            tier_order + journal_data.get("tiers_seen", [])))

        payload = {
            "meta": meta,
            "dataset": dataset_info,
            "transfer_id": str(transfer_id),
            "tiers": tiers,
            "tier_colors": {t: TIER_COLORS.get(t, "#9b59b6") for t in tiers},
            "batch_config": {k.lower(): v for k, v in batch_cfg.items()},
            "timeline": journal_data["timeline"],
            "log_counts": journal_data["counts"],
            "csv_summary": csv_summary,
            "throughput": throughput,
            "perf": perf_data,
            "completion_hist": _histogram(csv_summary.get("completions_rel", [])),
        }

        # Write JSON data
        (self.perf_dir / "perf_data.json").write_text(
            json.dumps(payload, indent=2, default=str), encoding="utf-8")

        # Render HTML
        html_path = self.perf_dir / "perf_report.html"
        render_perf_html(payload, html_path)

        # Write text summary
        txt_path = self.perf_dir / "perf_summary.txt"
        write_perf_summary_txt(txt_path, meta, journal_data, csv_summary,
                               throughput, perf_data)

        # Zip perf artifacts
        zip_path = self.case_dir / f"perf_{transfer_id}.zip"
        _zip_dir(self.perf_dir, zip_path)

        result = {
            "html_report": str(html_path),
            "json_data": str(self.perf_dir / "perf_data.json"),
            "zip": str(zip_path),
            "duration_sec": duration,
            "timeline_frames": len(journal_data["timeline"]),
            "log_counts": journal_data["counts"],
            "csv_summary_brief": {
                "total": csv_summary["total"],
                "success": csv_summary["success"],
                "failed": csv_summary["failed"],
                "total_bytes": csv_summary["total_bytes"],
            },
            "throughput_overall": throughput.get("overall", {}),
            "perf_overall": perf_data.get("overall", {}),
        }
        return result


def _zip_dir(src_dir: Path, zip_path: Path) -> None:
    if zip_path.exists():
        zip_path.unlink()
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for p in sorted(src_dir.rglob("*")):
            if p.is_file():
                zf.write(p, p.relative_to(src_dir.parent))
    LOG.info("wrote %s (%.1f KB)", zip_path, zip_path.stat().st_size / 1024)


# ---------------------------------------------------------------------------
# Text summary
# ---------------------------------------------------------------------------
def write_perf_summary_txt(path: Path, meta: dict, journal_data: dict,
                           csv_summary: dict, throughput: dict,
                           perf: dict) -> None:
    def _bytes(n: int) -> str:
        x = float(n)
        for u in ("B", "KB", "MB", "GB", "TB"):
            if x < 1024 or u == "TB":
                return f"{x:.2f} {u}" if x < 10 else f"{x:.0f} {u}"
            x /= 1024
        return f"{n} B"

    lines = [
        "CloudCP CLI Transfer Performance Summary",
        "=" * 60,
        f"test id          : {meta.get('test_id', '')}",
        f"transfer id      : {meta.get('transfer_id', '')}",
        f"tier             : {meta.get('tier', '')}",
        f"mode             : {meta.get('mode', '')}",
        f"start            : {meta.get('start', '')}",
        f"end              : {meta.get('end', '')}",
        f"duration (s)     : {meta.get('duration_sec', 0)}",
        "",
        "Results (CSV):",
        f"  total files    : {csv_summary.get('total', 0)}",
        f"  success        : {csv_summary.get('success', 0)}",
        f"  failed         : {csv_summary.get('failed', 0)}",
        f"  total bytes    : {_bytes(csv_summary.get('total_bytes', 0))}",
        f"  status counts  : {csv_summary.get('status_counts', {})}",
        "",
        "Log capture:",
        f"  pending events : {journal_data['counts']['pending_events']}",
        f"  running events : {journal_data['counts']['running_events']}",
        f"  free events    : {journal_data['counts']['free_events']}",
        f"  timeline frames: {len(journal_data['timeline'])}",
    ]

    tp = throughput or {}
    if tp.get("per_tier"):
        lines += ["", "Throughput (MB/s per batch, grouped by tier):"]
        for tier in tp.get("tiers", []):
            s = tp["per_tier"][tier]
            lines.append(
                f"  {tier:<6} batches={s['batches']:>3}  "
                f"min={s['min']:>7.2f}  avg={s['avg']:>7.2f}  med={s['median']:>7.2f}  "
                f"max={s['max']:>7.2f}  files/s={s['avg_files_sec']:>7.1f}  "
                f"agg={s['aggregate_mb_s']:>7.2f}  batch/s={s.get('batches_per_sec', 0):>6.3f}")
        ov = tp.get("overall", {})
        if ov:
            lines.append(
                f"  {'ALL':<6} batches={ov['batches']:>3}  "
                f"min={ov['min']:>7.2f}  avg={ov['avg']:>7.2f}  med={ov['median']:>7.2f}  "
                f"max={ov['max']:>7.2f}  {'':>16}  agg={ov['aggregate_mb_s']:>7.2f}  "
                f"batch/s={ov.get('batches_per_sec', 0):>6.3f}")

    pf = perf or {}
    if pf.get("per_tier"):
        lines += ["", "Batch completion — broker PERF (journal):"]
        for tier in pf.get("tiers", []):
            s = pf["per_tier"][tier]
            rc = f"  rc!=0={s['rc_fail']}" if s["rc_fail"] else ""
            lines.append(
                f"  {tier:<6} batches={s['batches']:>3}  files={s['files']:>7}  "
                f"min={s['total_min']:>7.2f}s  avg={s['total_avg']:>7.2f}s  "
                f"max={s['total_max']:>7.2f}s  mode={s['total_mode']:>6.1f}s  "
                f"upl_avg={s['upload_avg']:>7.2f}s{rc}")

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


# ---------------------------------------------------------------------------
# HTML report template (self-contained, adapted from schedular_test.py)
# ---------------------------------------------------------------------------

_HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>CLI Transfer Perf — __TITLE__</title>
<style>
  :root{--bg:#0e1116;--panel:#161b22;--fg:#e6edf3;--mut:#8b949e;--line:#30363d;--acc:#58a6ff;}
  *{box-sizing:border-box}
  body{margin:0;font-family:-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;background:var(--bg);color:var(--fg);}
  header{padding:18px 24px;border-bottom:1px solid var(--line);background:var(--panel);}
  h1{margin:0;font-size:20px}
  h2{font-size:15px;color:var(--acc);margin:0 0 10px;text-transform:uppercase;letter-spacing:.05em}
  .sub{color:var(--mut);font-size:13px;margin-top:4px}
  .wrap{max-width:1180px;margin:0 auto;padding:24px}
  .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:14px}
  .card{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:16px}
  .kv{display:flex;justify-content:space-between;padding:4px 0;border-bottom:1px dashed var(--line);font-size:13px}
  .kv:last-child{border-bottom:none}
  .kv .k{color:var(--mut)} .kv .v{font-weight:600}
  .big{font-size:26px;font-weight:700}
  .ok{color:#3fb950}.bad{color:#f85149}
  .controls{display:flex;align-items:center;gap:12px;flex-wrap:wrap;margin:14px 0}
  button{background:var(--acc);color:#06131f;border:0;border-radius:6px;padding:8px 14px;font-weight:700;cursor:pointer}
  button.sec{background:#21262d;color:var(--fg);border:1px solid var(--line)}
  input[type=range]{flex:1;min-width:200px}
  select{background:#21262d;color:var(--fg);border:1px solid var(--line);border-radius:6px;padding:6px}
  canvas{width:100%;background:#0b0f14;border:1px solid var(--line);border-radius:8px}
  .row{display:grid;grid-template-columns:1fr 1fr;gap:14px}
  .clock{font-variant-numeric:tabular-nums;color:var(--acc);font-weight:700}
  .legend{display:flex;gap:14px;flex-wrap:wrap;font-size:12px;color:var(--mut);margin-top:8px}
  .legend span{display:inline-flex;align-items:center;gap:6px}
  .dot{width:11px;height:11px;border-radius:2px;display:inline-block}
  .muted{color:var(--mut);font-size:12px}
  section{margin-top:26px}
  table{width:100%;border-collapse:collapse;font-size:13px}
  th,td{text-align:left;padding:6px 8px;border-bottom:1px solid var(--line)}
  th{color:var(--mut);font-weight:600}
  .metricdefs{display:grid;grid-template-columns:repeat(auto-fit,minmax(240px,1fr));gap:10px;margin:2px 0 14px}
  .metricdefs div{background:#0b0f14;border:1px solid var(--line);border-radius:8px;padding:10px 12px;font-size:12px;color:var(--mut)}
  .metricdefs b{color:var(--fg);font-size:13px}
  .chartwrap{position:relative}
  .tip{position:fixed;pointer-events:none;z-index:50;background:#0b0f14;border:1px solid var(--acc);border-radius:6px;padding:8px 10px;font-size:12px;color:var(--fg);box-shadow:0 4px 14px rgba(0,0,0,.5);display:none;max-width:260px;line-height:1.5}
  .tip b{color:var(--acc)}
  .tip .r{display:flex;justify-content:space-between;gap:14px}
  .tip .r span:last-child{font-weight:700}
  details{background:#0b0f14;border:1px solid var(--line);border-radius:8px;margin:8px 0;padding:6px 12px}
  details[open]{padding-bottom:12px}
  summary{cursor:pointer;font-size:13px;padding:4px 0;user-select:none}
  summary::-webkit-details-marker{color:var(--acc)}
  details table{margin-top:8px}
  details th{cursor:pointer;user-select:none}
  tr.bad td{background:rgba(248,81,73,.10)}
  @media(max-width:760px){.row{grid-template-columns:1fr}}
</style>
</head>
<body>
<header>
  <h1>CLI Transfer Performance — <span id="ttl"></span></h1>
  <div class="sub" id="subttl"></div>
</header>
<div class="wrap">

  <section>
    <h2>Summary</h2>
    <div class="grid" id="summaryGrid"></div>
  </section>

  <section>
    <h2>Replay — pending / running / free workers over time</h2>
    <div class="controls">
      <button id="playBtn">&#9654; Play</button>
      <button id="resetBtn" class="sec">&#10226; Reset</button>
      <span class="clock" id="clock">t = 0.0s</span>
      <label class="muted">speed
        <select id="speed">
          <option value="1">1&times;</option>
          <option value="2" selected>2&times;</option>
          <option value="4">4&times;</option>
          <option value="8">8&times;</option>
          <option value="16">16&times;</option>
        </select>
      </label>
      <input type="range" id="scrub" min="0" max="0" value="0"/>
    </div>
    <div class="row">
      <div>
        <canvas id="pendingCv" height="260"></canvas>
        <div class="muted" style="text-align:center">Pending batches per tier</div>
      </div>
      <div>
        <canvas id="runningCv" height="260"></canvas>
        <div class="muted" style="text-align:center">Running workers per tier</div>
      </div>
    </div>
    <div class="row" style="margin-top:14px">
      <div>
        <canvas id="freeCv" height="120"></canvas>
        <div class="muted" style="text-align:center">Free workers</div>
      </div>
      <div>
        <canvas id="totalCv" height="120"></canvas>
        <div class="muted" style="text-align:center">Total running vs free (stacked)</div>
      </div>
    </div>
    <div class="legend" id="legend"></div>
  </section>

  <section>
    <h2>Time-series (full transfer)</h2>
    <canvas id="tsCv" height="300"></canvas>
    <div class="muted" style="text-align:center">Pending totals, running total &amp; free workers vs time (s)</div>
  </section>

  <section>
    <h2>Completion histogram (from results CSV)</h2>
    <canvas id="histCv" height="220"></canvas>
    <div class="muted" style="text-align:center">Files completed per time-bin (finished_at)</div>
  </section>

  <section>
    <h2>Throughput &amp; per-batch time — per batch &amp; per category</h2>
    <div class="sub" id="thrOverall"></div>
    <div class="metricdefs">
      <div><b>MB/s (throughput)</b><br>Data transfer rate of a batch = bytes uploaded &divide; processing time.</div>
      <div><b>s/batch (processing time)</b><br>Wall-clock seconds a single batch took to process (cloudcp.log elapsed).</div>
      <div><b>batches/s (batch rate)</b><br>How many batches completed per second in a tier = batch count &divide; tier wall-clock span.</div>
    </div>
    <div class="chartwrap"><canvas id="thrScatterCv" height="280"></canvas></div>
    <div class="muted" style="text-align:center">Per-batch throughput (MB/s, log scale) vs completion time &mdash; colored by tier</div>
    <div class="row" style="margin-top:14px">
      <div>
        <div class="chartwrap"><canvas id="thrBarsCv" height="240"></canvas></div>
        <div class="muted" style="text-align:center">Avg throughput per tier (MB/s, log)</div>
      </div>
      <div>
        <div class="chartwrap"><canvas id="elapBarsCv" height="240"></canvas></div>
        <div class="muted" style="text-align:center">Avg processing time per batch per tier (s)</div>
      </div>
    </div>
    <table id="thrTbl" style="margin-top:14px">
      <thead><tr><th>Tier</th><th>Batches</th><th>Files</th><th>Data</th>
      <th>Min MB/s</th><th>Avg MB/s</th><th>Med MB/s</th><th>Max MB/s</th><th>files/s</th><th>Agg MB/s</th><th>batches/s</th>
      <th>s/batch min</th><th>s/batch avg</th><th>s/batch max</th></tr></thead>
      <tbody></tbody>
    </table>
  </section>

  <section>
    <h2>Batch completion — broker PERF (journal)</h2>
    <div class="sub" id="perfOverall"></div>
    <div class="metricdefs">
      <div><b>total s</b><br>Full batch completion time from the broker PERF line.</div>
      <div><b>upload s</b><br>Upload-phase seconds from the same PERF line.</div>
      <div><b>mode</b><br>Most frequent completion time per tier (rounded to 0.1s).</div>
    </div>
    <div class="row">
      <div>
        <div class="chartwrap"><canvas id="perfBarsCv" height="240"></canvas></div>
        <div class="muted" style="text-align:center">Avg batch completion per tier (total s)</div>
      </div>
      <div>
        <div class="chartwrap"><canvas id="perfHistCv" height="240"></canvas></div>
        <div class="muted" style="text-align:center">Completion-time distribution (total s) &mdash; stacked by tier</div>
      </div>
    </div>
    <div class="chartwrap" style="margin-top:14px"><canvas id="perfScatterCv" height="280"></canvas></div>
    <div class="muted" style="text-align:center">Per-batch completion time (total s) vs order &mdash; red ring = rc&ne;0</div>
    <div id="perfGroups" style="margin-top:14px"></div>
  </section>

  <section>
    <h2>Per-status breakdown</h2>
    <table id="statusTbl"><thead><tr><th>Status</th><th>Count</th></tr></thead><tbody></tbody></table>
  </section>

</div>
<div id="tip" class="tip"></div>
<script id="payload" type="application/json">__DATA__</script>
<script>
const DATA = JSON.parse(document.getElementById('payload').textContent);
const TIERS = DATA.tiers, COL = DATA.tier_colors;
const TL = DATA.timeline;
const fmt = n => (n>=1e6?(n/1e6).toFixed(1)+'M':n>=1e3?(n/1e3).toFixed(1)+'k':(''+n));
const bytes = n => {const u=['B','KB','MB','GB','TB'];let i=0,x=n;while(x>=1024&&i<u.length-1){x/=1024;i++;}return x.toFixed(x<10?2:0)+' '+u[i];};
const dsize = n => {const u=['B','KB','MB','GB','TB'];let i=0,x=n;while(x>=1000&&i<u.length-1){x/=1000;i++;}return x.toFixed(x<10?2:0)+' '+u[i];};

document.getElementById('ttl').textContent = (DATA.meta.test_id||'') + '  \u00b7  transfer ' + DATA.transfer_id;
document.getElementById('subttl').textContent =
  'tier ' + (DATA.meta.tier||'-') + '  \u00b7  mode ' + (DATA.meta.mode||'-')
  + '  \u00b7  ' + (DATA.meta.start||'') + '  \u2192  ' + (DATA.meta.end||'');

// ---- summary cards ----
const cs = DATA.csv_summary || {};
const dur = DATA.meta.duration_sec || 0;
const ds = DATA.dataset || {};
const cards = [
  ['Transfer', [['test id',DATA.meta.test_id],['transfer id',DATA.transfer_id],['tier',DATA.meta.tier||'-'],
                ['mode',DATA.meta.mode||'-'],['duration', dur.toFixed(1)+' s']]],
  ['Dataset', [['actual files',fmt(ds.actual_files||0)],['expected files',fmt(ds.expected_files||0)],
               ['dataset root',ds.dataset_root||'-']]],
  ['Results (CSV)', [['files',fmt(cs.total||0)],['success',(cs.success||0)],['failed',(cs.failed||0)],
                     ['bytes',bytes(cs.total_bytes||0)],['completion span',(cs.completion_span_sec||0).toFixed(1)+' s']]],
  ['Log capture', [['pending events',DATA.log_counts.pending_events],['running events',DATA.log_counts.running_events],
                   ['free events',DATA.log_counts.free_events],['timeline frames',TL.length]]],
];
const sg = document.getElementById('summaryGrid');
for(const [title,kvs] of cards){
  const d=document.createElement('div'); d.className='card';
  d.innerHTML='<div class="big">'+title+'</div>'+kvs.map(([k,v])=>
    '<div class="kv"><span class="k">'+k+'</span><span class="v">'+(v==null?'-':v)+'</span></div>').join('');
  sg.appendChild(d);
}
const stb=document.querySelector('#statusTbl tbody');
Object.entries(cs.status_counts||{}).forEach(([s,c])=>{
  const cls = s==='SUCCESS'?'ok':(s?'bad':'');
  stb.innerHTML+='<tr><td class="'+cls+'">'+(s||'(blank)')+'</td><td>'+c+'</td></tr>';
});
const lg=document.getElementById('legend');
TIERS.forEach(t=>{lg.innerHTML+='<span><i class="dot" style="background:'+(COL[t]||'#888')+'"></i>'+t+'</span>';});

// ---- throughput ----
const THR=DATA.throughput||{batches:[],per_tier:{},overall:{},tiers:[]};
{
  const ov=THR.overall||{};
  document.getElementById('thrOverall').textContent = ov.batches
    ? (ov.batches+' batches \u00b7 avg '+(ov.avg||0).toFixed(2)+' MB/s \u00b7 peak '+(ov.max||0).toFixed(2)
       +' MB/s \u00b7 aggregate '+(ov.aggregate_mb_s||0).toFixed(2)+' MB/s \u00b7 '+(ov.batches_per_sec||0).toFixed(3)+' batches/s \u00b7 avg '+(ov.elapsed_avg||0).toFixed(2)
       +'s/batch over '+(ov.wall_sec||0).toFixed(1)+'s')
    : 'no cloudcp.log throughput data';
  const ttb=document.querySelector('#thrTbl tbody');
  (THR.tiers||[]).forEach(t=>{const s=THR.per_tier[t];
    ttb.innerHTML+='<tr><td><i class="dot" style="background:'+(COL[t]||'#888')+'"></i> '+t+'</td>'
      +'<td>'+s.batches+'</td><td>'+fmt(s.files)+'</td><td>'+dsize(s.bytes)+'</td>'
      +'<td>'+s.min.toFixed(2)+'</td><td>'+s.avg.toFixed(2)+'</td><td>'+s.median.toFixed(2)+'</td><td>'+s.max.toFixed(2)+'</td>'
      +'<td>'+s.avg_files_sec.toFixed(1)+'</td><td>'+s.aggregate_mb_s.toFixed(2)+'</td><td>'+(s.batches_per_sec||0).toFixed(3)+'</td>'
      +'<td>'+s.elapsed_min.toFixed(2)+'</td><td>'+s.elapsed_avg.toFixed(2)+'</td><td>'+s.elapsed_max.toFixed(2)+'</td></tr>';});
  if(ov.batches){ttb.innerHTML+='<tr style="font-weight:700"><td>ALL</td><td>'+ov.batches+'</td><td>-</td>'
      +'<td>'+dsize(ov.bytes||0)+'</td><td>'+(ov.min||0).toFixed(2)+'</td><td>'+(ov.avg||0).toFixed(2)+'</td>'
      +'<td>'+(ov.median||0).toFixed(2)+'</td><td>'+(ov.max||0).toFixed(2)+'</td><td>-</td>'
      +'<td>'+(ov.aggregate_mb_s||0).toFixed(2)+'</td><td>'+(ov.batches_per_sec||0).toFixed(3)+'</td>'
      +'<td>'+(ov.elapsed_min||0).toFixed(2)+'</td><td>'+(ov.elapsed_avg||0).toFixed(2)+'</td><td>'+(ov.elapsed_max||0).toFixed(2)+'</td></tr>';}
}
const HITS={};
function tipRows(rows){return rows.map(r=>'<div class="r"><span>'+r[0]+'</span><span>'+r[1]+'</span></div>').join('');}
function drawThrScatter(){
  const cv=document.getElementById('thrScatterCv'); const {c,w,h}=prep(cv); clear(c,w,h);
  HITS.thrScatterCv={w,h,regions:[]};
  const B=THR.batches||[]; const pad=46; axis(c,w,h,pad);
  if(!B.length){c.fillStyle='#8b949e';c.fillText('no throughput data',20,30);return;}
  const tMax=Math.max(1,...B.map(b=>b.t_done));
  const pos=B.map(b=>b.throughput).filter(v=>v>0);
  const vMin=pos.length?Math.max(0.01,Math.min(...pos)):0.01;
  const vMax=Math.max(1,...B.map(b=>b.throughput));
  const lgv=v=>Math.log10(Math.max(v,vMin));
  const lo=lgv(vMin), hi=lgv(vMax);
  const X=t=>pad+(w-pad-12)*(t/tMax);
  const Y=v=>{const y=(lgv(v)-lo)/((hi-lo)||1); return (h-pad-8)-(h-pad-20)*y;};
  c.font='10px sans-serif';c.textAlign='right';
  for(let p=Math.floor(lo);p<=Math.ceil(hi);p++){const yy=Y(Math.pow(10,p));
    c.strokeStyle='#20262d';c.beginPath();c.moveTo(pad,yy);c.lineTo(w-6,yy);c.stroke();
    c.fillStyle='#8b949e';c.fillText(Math.pow(10,p)+'',pad-4,yy+3);}
  B.forEach(b=>{const x=X(b.t_done),y=Y(b.throughput);
    c.fillStyle=COL[b.tier]||'#888';c.globalAlpha=0.8;
    c.beginPath();c.arc(x,y,3,0,6.283);c.fill();c.globalAlpha=1;
    HITS.thrScatterCv.regions.push({type:'circ',x,y,r:6,
      html:'<b>'+b.tier+' batch</b>'+tipRows([
        ['throughput',b.throughput.toFixed(2)+' MB/s'],['processing',b.elapsed.toFixed(2)+' s'],
        ['files',fmt(b.files)],['data',dsize(b.bytes)],['files/s',b.files_sec.toFixed(1)],
        ['done at',b.t_done.toFixed(1)+' s']])});});
  c.fillStyle='#8b949e';c.textAlign='center';c.fillText(tMax.toFixed(0)+'s',w-24,h-pad+14);
  c.textAlign='left';c.fillText('MB/s (log)',pad-40,12);
}
function drawThrBars(){
  const cv=document.getElementById('thrBarsCv'); const {c,w,h}=prep(cv); clear(c,w,h);
  HITS.thrBarsCv={w,h,regions:[]};
  const tiers=THR.tiers||[]; const pad=40; axis(c,w,h,pad);
  if(!tiers.length){c.fillStyle='#8b949e';c.fillText('no throughput data',20,30);return;}
  const maxV=Math.max(1,...tiers.map(t=>THR.per_tier[t].max));
  const lgv=v=>Math.log10(Math.max(v,0.01));
  const lo=lgv(0.01), hi=lgv(maxV);
  const Y=v=>{const y=(lgv(v)-lo)/((hi-lo)||1);return (h-pad-8)-(h-pad-20)*y;};
  const bw=(w-pad-10)/tiers.length; c.font='11px sans-serif';
  tiers.forEach((t,i)=>{const s=THR.per_tier[t];const x=pad+i*bw+bw*0.2;const bwidth=bw*0.6;
    const yTop=Y(Math.max(s.avg,0.01));
    c.fillStyle=COL[t]||'#888';c.fillRect(x,yTop,bwidth,(h-pad)-yTop);
    c.strokeStyle='#e6edf3';c.lineWidth=1.5;c.beginPath();
    c.moveTo(x+bwidth/2,Y(Math.max(s.min,0.01)));c.lineTo(x+bwidth/2,Y(Math.max(s.max,0.01)));c.stroke();
    c.fillStyle='#e6edf3';c.textAlign='center';c.fillText(s.avg.toFixed(1),x+bwidth/2,yTop-4);
    c.fillStyle='#8b949e';c.fillText(t,x+bwidth/2,h-pad+12);
    HITS.thrBarsCv.regions.push({type:'rect',x,y:8,w:bwidth,h:(h-pad)-8,
      html:'<b>'+t+' throughput</b>'+tipRows([
        ['min',s.min.toFixed(2)+' MB/s'],['avg',s.avg.toFixed(2)+' MB/s'],
        ['median',s.median.toFixed(2)+' MB/s'],['max',s.max.toFixed(2)+' MB/s'],
        ['aggregate',s.aggregate_mb_s.toFixed(2)+' MB/s'],['batches/s',(s.batches_per_sec||0).toFixed(3)],
        ['batches',s.batches],['files',fmt(s.files)]])});});
  c.textAlign='left';c.fillStyle='#8b949e';c.fillText('avg MB/s (log)',pad,12);
}
function drawElapBars(){
  const cv=document.getElementById('elapBarsCv'); const {c,w,h}=prep(cv); clear(c,w,h);
  HITS.elapBarsCv={w,h,regions:[]};
  const tiers=THR.tiers||[]; const pad=40; axis(c,w,h,pad);
  if(!tiers.length){c.fillStyle='#8b949e';c.fillText('no throughput data',20,30);return;}
  const maxV=Math.max(0.1,...tiers.map(t=>THR.per_tier[t].elapsed_max));
  const Y=v=>(h-pad-8)-(h-pad-20)*(v/maxV);
  const bw=(w-pad-10)/tiers.length; c.font='11px sans-serif';
  tiers.forEach((t,i)=>{const s=THR.per_tier[t];const x=pad+i*bw+bw*0.2;const bwidth=bw*0.6;
    const yTop=Y(s.elapsed_avg);
    c.fillStyle=COL[t]||'#888';c.fillRect(x,yTop,bwidth,(h-pad)-yTop);
    c.strokeStyle='#e6edf3';c.lineWidth=1.5;c.beginPath();
    c.moveTo(x+bwidth/2,Y(s.elapsed_min));c.lineTo(x+bwidth/2,Y(s.elapsed_max));c.stroke();
    c.fillStyle='#e6edf3';c.textAlign='center';c.fillText(s.elapsed_avg.toFixed(1)+'s',x+bwidth/2,yTop-4);
    c.fillStyle='#8b949e';c.fillText(t,x+bwidth/2,h-pad+12);
    HITS.elapBarsCv.regions.push({type:'rect',x,y:8,w:bwidth,h:(h-pad)-8,
      html:'<b>'+t+' processing time</b>'+tipRows([
        ['min',s.elapsed_min.toFixed(2)+' s'],['avg',s.elapsed_avg.toFixed(2)+' s'],
        ['median',s.elapsed_median.toFixed(2)+' s'],['max',s.elapsed_max.toFixed(2)+' s'],
        ['batches/s',(s.batches_per_sec||0).toFixed(3)],['batches',s.batches]])});});
  c.textAlign='left';c.fillStyle='#8b949e';c.fillText('avg seconds/batch',pad,12);
}
function attachHover(id){
  const cv=document.getElementById(id); const tip=document.getElementById('tip');
  cv.addEventListener('mousemove',e=>{
    const hit=HITS[id]; if(!hit){tip.style.display='none';return;}
    const rect=cv.getBoundingClientRect();
    const mx=(e.clientX-rect.left)*(hit.w/rect.width);
    const my=(e.clientY-rect.top)*(hit.h/rect.height);
    let found=null,best=1e9;
    for(const rg of hit.regions){
      if(rg.type==='rect'){ if(mx>=rg.x&&mx<=rg.x+rg.w&&my>=rg.y&&my<=rg.y+rg.h){found=rg;break;} }
      else { const d=Math.hypot(mx-rg.x,my-rg.y); if(d<=rg.r&&d<best){best=d;found=rg;} }
    }
    if(found){tip.innerHTML=found.html;tip.style.display='block';
      tip.style.left=(e.clientX+14)+'px';tip.style.top=(e.clientY+14)+'px';}
    else tip.style.display='none';
  });
  cv.addEventListener('mouseleave',()=>{tip.style.display='none';});
}
['thrScatterCv','thrBarsCv','elapBarsCv'].forEach(attachHover);

// ---- PERF ----
const PERF=DATA.perf||{batches:[],per_tier:{},overall:{},tiers:[]};
function makeSortable(tbl){
  const heads=tbl.querySelectorAll('th');
  heads.forEach((th,ci)=>{let dir=1;
    th.addEventListener('click',()=>{
      const tb=tbl.querySelector('tbody');
      const rows=[...tb.querySelectorAll('tr')];
      rows.sort((a,b)=>{
        const x=a.children[ci].textContent.trim(), y=b.children[ci].textContent.trim();
        const nx=parseFloat(x.replace(/[^0-9.\-]/g,'')), ny=parseFloat(y.replace(/[^0-9.\-]/g,''));
        const num=!isNaN(nx)&&!isNaN(ny);
        return dir*(num? nx-ny : (x<y?-1:x>y?1:0));
      });
      dir*=-1; rows.forEach(r=>tb.appendChild(r));
    });
  });
}
{
  const ov=PERF.overall||{};
  document.getElementById('perfOverall').textContent = ov.batches
    ? (ov.batches+' batches \u00b7 avg '+(ov.total_avg||0).toFixed(2)+'s total (mode '
       +(ov.total_mode||0).toFixed(1)+'s \u00b7 max '+(ov.total_max||0).toFixed(2)+'s) \u00b7 avg '
       +(ov.upload_avg||0).toFixed(2)+'s upload \u00b7 '+fmt(ov.files||0)+' files \u00b7 '+dsize(ov.bytes||0)
       +(ov.rc_fail?(' \u00b7 '+ov.rc_fail+' rc\u22600'):' \u00b7 all rc=0'))
    : 'no PERF batch data captured from the journal';
  const pg=document.getElementById('perfGroups');
  (PERF.tiers||[]).forEach(t=>{
    const s=PERF.per_tier[t];
    const rows=(PERF.batches||[]).filter(b=>b.tier===t).map(b=>
      '<tr class="'+(b.rc!==0?'bad':'')+'"><td>'+b.batch+'</td><td>'+fmt(b.files)+'</td><td>'
      +dsize(b.bytes)+'</td><td>'+b.upload.toFixed(2)+'</td><td>'+b.total.toFixed(2)+'</td>'
      +'<td class="'+(b.rc!==0?'bad':'ok')+'">'+b.rc+'</td></tr>').join('');
    const det=document.createElement('details');
    det.innerHTML='<summary><i class="dot" style="background:'+(COL[t]||'#888')+'"></i> <b>'+t+'</b>'
      +' \u2014 '+s.batches+' batches \u00b7 avg '+s.total_avg.toFixed(2)+'s \u00b7 min '+s.total_min.toFixed(2)
      +'s \u00b7 max '+s.total_max.toFixed(2)+'s \u00b7 mode '+s.total_mode.toFixed(1)+'s'
      +(s.rc_fail?(' \u00b7 <span class="bad">'+s.rc_fail+' rc\u22600</span>'):'')+'</summary>'
      +'<table><thead><tr><th>Batch</th><th>Files</th><th>Size</th><th>Upload s</th>'
      +'<th>Total s</th><th>rc</th></tr></thead><tbody>'+rows+'</tbody></table>';
    pg.appendChild(det);
  });
  pg.querySelectorAll('table').forEach(makeSortable);
}
function drawPerfBars(){
  const cv=document.getElementById('perfBarsCv'); const {c,w,h}=prep(cv); clear(c,w,h);
  HITS.perfBarsCv={w,h,regions:[]};
  const tiers=PERF.tiers||[]; const pad=40; axis(c,w,h,pad);
  if(!tiers.length){c.fillStyle='#8b949e';c.fillText('no PERF data',20,30);return;}
  const maxV=Math.max(0.1,...tiers.map(t=>PERF.per_tier[t].total_max));
  const Y=v=>(h-pad-8)-(h-pad-20)*(v/maxV);
  const bw=(w-pad-10)/tiers.length; c.font='11px sans-serif';
  tiers.forEach((t,i)=>{const s=PERF.per_tier[t];const x=pad+i*bw+bw*0.2;const bwidth=bw*0.6;
    const yTop=Y(s.total_avg);
    c.fillStyle=COL[t]||'#888';c.fillRect(x,yTop,bwidth,(h-pad)-yTop);
    c.strokeStyle='#e6edf3';c.lineWidth=1.5;c.beginPath();
    c.moveTo(x+bwidth/2,Y(s.total_min));c.lineTo(x+bwidth/2,Y(s.total_max));c.stroke();
    c.fillStyle='#e6edf3';c.textAlign='center';c.fillText(s.total_avg.toFixed(1)+'s',x+bwidth/2,yTop-4);
    c.fillStyle='#8b949e';c.fillText(t,x+bwidth/2,h-pad+12);
    HITS.perfBarsCv.regions.push({type:'rect',x,y:8,w:bwidth,h:(h-pad)-8,
      html:'<b>'+t+' completion</b>'+tipRows([
        ['avg total',s.total_avg.toFixed(2)+' s'],['min',s.total_min.toFixed(2)+' s'],
        ['max',s.total_max.toFixed(2)+' s'],['mode',s.total_mode.toFixed(1)+' s'],
        ['avg upload',s.upload_avg.toFixed(2)+' s'],['batches',s.batches],
        ['files',fmt(s.files)],['rc\u22600',s.rc_fail]])});});
  c.textAlign='left';c.fillStyle='#8b949e';c.fillText('avg total s/batch',pad,12);
}
function drawPerfHist(){
  const cv=document.getElementById('perfHistCv'); const {c,w,h}=prep(cv); clear(c,w,h);
  const B=PERF.batches||[]; const tiers=PERF.tiers||[]; const pad=40; axis(c,w,h,pad);
  if(!B.length){c.fillStyle='#8b949e';c.fillText('no PERF data',20,30);return;}
  const totals=B.map(b=>b.total); const lo=Math.min(...totals), hi=Math.max(...totals);
  const nb=Math.min(24,Math.max(6,B.length)); const width=((hi-lo)/nb)||1;
  const counts=Array.from({length:nb},()=>({}));
  B.forEach(b=>{let idx=Math.min(nb-1,Math.floor((b.total-lo)/width)); if(idx<0)idx=0;
    counts[idx][b.tier]=(counts[idx][b.tier]||0)+1;});
  const totMax=Math.max(1,...counts.map(cn=>Object.values(cn).reduce((a,v)=>a+v,0)));
  const bw=(w-pad-10)/nb;
  counts.forEach((cn,i)=>{let yBase=h-pad; const x=pad+i*bw;
    tiers.forEach(t=>{const v=cn[t]||0; if(!v)return; const bh=(h-pad-12)*(v/totMax);
      c.fillStyle=COL[t]||'#888'; c.fillRect(x+1,yBase-bh,bw-1,bh); yBase-=bh;});});
  c.fillStyle='#8b949e';c.font='11px sans-serif';c.textAlign='left';
  c.fillText('peak '+totMax+' /bin',pad+4,14); c.fillText(lo.toFixed(0)+'s',pad,h-pad+14);
  c.textAlign='right';c.fillText(hi.toFixed(0)+'s',w-8,h-pad+14);
}
function drawPerfScatter(){
  const cv=document.getElementById('perfScatterCv'); const {c,w,h}=prep(cv); clear(c,w,h);
  HITS.perfScatterCv={w,h,regions:[]};
  const B=PERF.batches||[]; const pad=46; axis(c,w,h,pad);
  if(!B.length){c.fillStyle='#8b949e';c.fillText('no PERF data',20,30);return;}
  const n=B.length; const vMax=Math.max(1,...B.map(b=>b.total));
  const X=i=>pad+(w-pad-12)*(n>1? i/(n-1):0);
  const Y=v=>(h-pad-8)-(h-pad-20)*(v/vMax);
  c.font='10px sans-serif';c.textAlign='right';
  for(let k=0;k<=4;k++){const vv=vMax*k/4;const yy=Y(vv);
    c.strokeStyle='#20262d';c.beginPath();c.moveTo(pad,yy);c.lineTo(w-6,yy);c.stroke();
    c.fillStyle='#8b949e';c.fillText(vv.toFixed(0)+'s',pad-4,yy+3);}
  B.forEach((b,i)=>{const x=X(i),y=Y(b.total);
    c.fillStyle=COL[b.tier]||'#888';c.globalAlpha=0.85;
    c.beginPath();c.arc(x,y,3.2,0,6.283);c.fill();c.globalAlpha=1;
    if(b.rc!==0){c.strokeStyle='#f85149';c.lineWidth=1.6;c.beginPath();c.arc(x,y,5.4,0,6.283);c.stroke();}
    HITS.perfScatterCv.regions.push({type:'circ',x,y,r:6,
      html:'<b>'+b.tier+' \u00b7 '+b.batch+'</b>'+tipRows([
        ['total',b.total.toFixed(2)+' s'],['upload',b.upload.toFixed(2)+' s'],
        ['files',fmt(b.files)],['size',dsize(b.bytes)],['rc',b.rc],['order','#'+(b.order+1)]])});});
  c.fillStyle='#8b949e';c.textAlign='center';c.fillText('completion order \u2192',w/2,h-pad+14);
  c.textAlign='left';c.fillText('total s',pad-40,12);
}
['perfBarsCv','perfScatterCv'].forEach(attachHover);

// ---- canvas helpers ----
function prep(cv){const r=window.devicePixelRatio||1;const w=cv.clientWidth;const h=cv.height;
  cv.width=w*r;cv.height=h*r;const c=cv.getContext('2d');c.setTransform(r,0,0,r,0,0);return {c,w,h};}
function clear(c,w,h){c.clearRect(0,0,w,h);}
function axis(c,w,h,pad){c.strokeStyle='#30363d';c.lineWidth=1;c.beginPath();
  c.moveTo(pad,h-pad);c.lineTo(w-6,h-pad);c.moveTo(pad,h-pad);c.lineTo(pad,8);c.stroke();}

function drawBars(cv, map, maxV){
  const {c,w,h}=prep(cv); clear(c,w,h); const pad=34; axis(c,w,h,pad);
  const n=TIERS.length; const bw=(w-pad-10)/n; maxV=Math.max(maxV,1);
  c.font='11px sans-serif';
  TIERS.forEach((t,i)=>{
    const v=map[t]||0; const bh=(h-pad-14)*(v/maxV);
    const x=pad+i*bw+bw*0.18; const bwidth=bw*0.64;
    c.fillStyle=COL[t]||'#888'; c.fillRect(x,h-pad-bh,bwidth,bh);
    c.fillStyle='#e6edf3'; c.textAlign='center';
    c.fillText(fmt(v), x+bwidth/2, h-pad-bh-4);
    c.fillStyle='#8b949e'; c.fillText(t, x+bwidth/2, h-pad+12);
  });
  c.fillStyle='#8b949e'; c.textAlign='left'; c.fillText('max '+fmt(maxV), pad+2, 12);
}
function drawFree(cv, val, maxV){
  const {c,w,h}=prep(cv); clear(c,w,h); const pad=30;
  maxV=Math.max(maxV,1); const bw=(w-pad-10)*(val/maxV);
  c.fillStyle='#238636'; c.fillRect(pad,h/2-16,bw,32);
  c.fillStyle='#e6edf3'; c.font='20px sans-serif'; c.textAlign='left';
  c.fillText(val+' free', pad+6, h/2+7);
}
function drawStack(cv, running, free, maxV){
  const {c,w,h}=prep(cv); clear(c,w,h); const pad=30; maxV=Math.max(maxV,1);
  let x=pad; const totalRun=TIERS.reduce((a,t)=>a+(running[t]||0),0);
  const scale=(w-pad-10)/maxV;
  TIERS.forEach(t=>{const seg=(running[t]||0)*scale; c.fillStyle=COL[t]||'#888';
    c.fillRect(x,h/2-16,seg,32); x+=seg;});
  c.fillStyle='#30363d'; c.fillRect(x,h/2-16,(free)*scale,32);
  c.fillStyle='#e6edf3'; c.font='13px sans-serif'; c.textAlign='left';
  c.fillText('running '+totalRun+' \u00b7 free '+free, pad+4, h/2-22);
}

let maxPend=1,maxRun=1,maxFree=1,maxSlots=1;
TL.forEach(f=>{
  TIERS.forEach(t=>{maxPend=Math.max(maxPend,f.pending[t]||0);maxRun=Math.max(maxRun,f.running[t]||0);});
  maxFree=Math.max(maxFree,f.free||0);
  const run=TIERS.reduce((a,t)=>a+(f.running[t]||0),0);
  maxSlots=Math.max(maxSlots,run+(f.free||0));
});

const pendCv=document.getElementById('pendingCv'), runCv=document.getElementById('runningCv'),
      freeCv=document.getElementById('freeCv'), totCv=document.getElementById('totalCv');
function renderFrame(i){
  if(!TL.length)return;
  const f=TL[Math.max(0,Math.min(i,TL.length-1))];
  drawBars(pendCv,f.pending,maxPend);
  drawBars(runCv,f.running,maxRun);
  drawFree(freeCv,f.free||0,Math.max(maxFree,maxSlots));
  drawStack(totCv,f.running,f.free||0,maxSlots);
  document.getElementById('clock').textContent='t = '+(f.t||0).toFixed(1)+'s  ('+f.iso.split('T')[1]+')';
  document.getElementById('scrub').value=i;
}

function drawTS(){
  const cv=document.getElementById('tsCv'); const {c,w,h}=prep(cv); clear(c,w,h);
  if(!TL.length){c.fillStyle='#8b949e';c.fillText('no timeline data',20,30);return;}
  const pad=40; axis(c,w,h,pad);
  const tMax=TL[TL.length-1].t||1;
  const pendTot=TL.map(f=>TIERS.reduce((a,t)=>a+(f.pending[t]||0),0));
  const runTot=TL.map(f=>TIERS.reduce((a,t)=>a+(f.running[t]||0),0));
  const freeArr=TL.map(f=>f.free||0);
  const yMaxL=Math.max(1,...pendTot);
  const yMaxR=Math.max(1,...runTot,...freeArr);
  const X=t=>pad+(w-pad-10)*(t/tMax);
  function line(arr,ymax,color){c.strokeStyle=color;c.lineWidth=2;c.beginPath();
    TL.forEach((f,i)=>{const x=X(f.t);const y=(h-pad-8)-(h-pad-16)*(arr[i]/ymax);
      i?c.lineTo(x,y):c.moveTo(x,y);});c.stroke();}
  line(pendTot,yMaxL,'#58a6ff'); line(runTot,yMaxR,'#f39c12'); line(freeArr,yMaxR,'#3fb950');
  c.font='11px sans-serif';c.textAlign='left';
  c.fillStyle='#58a6ff';c.fillText('pending total (L)',pad+4,14);
  c.fillStyle='#f39c12';c.fillText('running total (R)',pad+140,14);
  c.fillStyle='#3fb950';c.fillText('free (R)',pad+280,14);
  c.fillStyle='#8b949e';c.textAlign='center';c.fillText(tMax.toFixed(0)+'s',w-24,h-pad+14);
}
function drawHist(){
  const cv=document.getElementById('histCv'); const {c,w,h}=prep(cv); clear(c,w,h);
  const H=DATA.completion_hist||{}; const pad=40; axis(c,w,h,pad);
  const counts=H.counts||[]; if(!counts.length){c.fillStyle='#8b949e';c.fillText('no completion data',20,30);return;}
  const ymax=Math.max(1,...counts); const bw=(w-pad-10)/counts.length;
  counts.forEach((v,i)=>{const bh=(h-pad-12)*(v/ymax);const x=pad+i*bw;
    c.fillStyle='#58a6ff';c.fillRect(x+1,h-pad-bh,bw-1,bh);});
  c.fillStyle='#8b949e';c.font='11px sans-serif';c.textAlign='left';
  c.fillText('peak '+ymax+' files/bin',pad+4,14);
  c.textAlign='center';c.fillText(((H.hi||0)).toFixed(0)+'s',w-24,h-pad+14);
}

// ---- animation ----
let idx=0, playing=false, timer=null;
const playBtn=document.getElementById('playBtn'), scrub=document.getElementById('scrub');
scrub.max=Math.max(0,TL.length-1);
function tick(){
  if(!playing)return;
  idx++; if(idx>=TL.length){idx=TL.length-1;stop();renderFrame(idx);return;}
  renderFrame(idx);
}
function start(){if(!TL.length)return;playing=true;playBtn.textContent='\u23f8 Pause';
  const spd=parseInt(document.getElementById('speed').value,10);
  clearInterval(timer);timer=setInterval(tick,Math.max(40,400/spd));}
function stop(){playing=false;playBtn.textContent='\u25b6 Play';clearInterval(timer);}
playBtn.onclick=()=>{playing?stop():(idx>=TL.length-1?(idx=0):0,start());};
document.getElementById('resetBtn').onclick=()=>{stop();idx=0;renderFrame(0);};
document.getElementById('speed').onchange=()=>{if(playing)start();};
scrub.oninput=e=>{stop();idx=parseInt(e.target.value,10);renderFrame(idx);};

function redraw(){renderFrame(idx);drawTS();drawHist();drawThrScatter();drawThrBars();drawElapBars();drawPerfBars();drawPerfHist();drawPerfScatter();}
window.addEventListener('resize',redraw);
redraw();
</script>
</body>
</html>
"""


def render_perf_html(payload: dict, out_path: Path) -> None:
    meta = payload.get("meta", {})
    title = f"{meta.get('test_id', '')} \u00b7 transfer {payload.get('transfer_id', '')}"
    data_json = json.dumps(payload, separators=(",", ":"), default=str).replace("</", "<\\/")
    html = (_HTML_TEMPLATE
            .replace("__TITLE__", title)
            .replace("__DATA__", data_json))
    out_path.write_text(html, encoding="utf-8")
    LOG.info("perf HTML report -> %s", out_path)
