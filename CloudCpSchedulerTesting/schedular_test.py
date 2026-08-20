#!/usr/bin/env python3
"""
schedular_test.py — end-to-end scheduler/broker test harness (standalone).

Runs on the Linux bryck host. For each selected deterministic-enumeration dataset
(SCH-ORD-01..12 / SCH-DEEP-01..03) it:

  1. materialises the data by invoking `datagen --spec <L*.yaml>` on every
     per-level spec under spec_files/<ID>/ in BFS chain order (L0 -> L4);
  2. records the ground-truth enumeration order to enumeration_order.json;
  3. allocates the next transfer id (max existing transfer_* in the batchmeta dir + 1)
     and creates transfer_<id>;
  4. starts a journalctl follower and fans matching lines into three log files
     (Pending-<id>, "free workers", "Running with workers") plus a raw log,
     stamping the test start; concurrently tails cloudcp.log (--cloudcp-log)
     into cloudcplogs.txt for the same initiation -> completion window;
  5. runs batch_scheduler.py and waits for it to exit (exit 0 == transfer complete),
     stamping the test completion;
  6. fetches the per-file results CSV
     (/opt/bryck/bryckapi/downloads/cloud_transfer_logs/cloud_transfer_<id>/transfer_report_<id>.csv);
  7. parses logs + CSV and renders a fully self-contained HTML report with an animated
     "replay" of pending / running workers / free workers over time, plus histograms
     and per-batch / per-tier throughput (min/avg/max) parsed from cloudcp.log;
  8. zips everything into sch_test_<id>.zip.

For a range or --all it produces a per-dataset zip for EACH dataset and one COMBINED
report + zip.

Assumes the spec files already exist (this script does NOT call generate_dataset.py).

Usage
-----
    python3 schedular_test.py 7                 # single dataset (number or id)
    python3 schedular_test.py SCH-DEEP-02
    python3 schedular_test.py --from 1 --to 5   # inclusive range
    python3 schedular_test.py --all             # all datasets, each + combined

Negative (scheduler-level fault injection; see schedular_negative_test.py):
    python3 schedular_test.py --negative-list    # list negative cases
    python3 schedular_test.py --negative         # run the whole negative suite
    python3 schedular_test.py --negative-case NEG-ENUM-03

Common options (all have host-sensible defaults):
    --spec-dir PATH        default: <this dir>/spec_files
    --data-root PATH       default: /bryck/cloudcp_sched_data
    --s3-base URI          default: s3://aditya/sch_test
    --datagen PATH         default: /home/bryck/rperiyas/datagen
    --datagen-flag FLAG    default: --spec 
    --batchmeta-dir PATH   default: /opt/bryck/bryckapi/downloads/bcloud_batchmeta
    --transfer-logs-dir P  default: /opt/bryck/bryckapi/downloads/cloud_transfer_logs
    --cloudcp-log PATH     default: <transfer-logs-dir>/cloudcp.log (tailed to cloudcplogs.txt)
    --scheduler-python P   default: /opt/bryck/.venv/bryck/bin/python3
    --scheduler-script P   default: .../site-packages/bryckcloud/lib/cloud/batch_scheduler.py
    --dir-path PATH        default: .../site-packages/bryckcloud/lib/cloud
    --endpoint-url URL     default: https://10.10.10.103:9000
    --config PATH          default: /etc/bryck/bryckcloud/config.json (for BATCH tiers)
    --out-dir PATH         default: <this dir>/sch_test_runs
    --poll-interval SEC    forwarded to batch_scheduler.py if set
    --capture-lead SEC     settle the journalctl follower before the scheduler (default 3)
    --capture-drain SEC    keep capturing after the scheduler exits (default 6)
    --skip-datagen         reuse already-materialised data
    --delete               after the run, delete the materialised data dir (<data-root>/<id>)
    --clear-bucket         after the run, clear uploaded S3 objects under <s3-base>/<id>
    --cleanup              after the run, do both --delete and --clear-bucket
    --dry-run              print external commands without executing
"""

from __future__ import annotations

import argparse
import ast
import csv
import datetime as _dt
import json
import logging
import os
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
import zipfile
from pathlib import Path

LOG = logging.getLogger("schedular_test")

# ---- host defaults -------------------------------------------------------------------
DEF_DATAGEN = "/home/bryck/rperiyas/datagen"
DEF_DATAGEN_FLAG = "--spec"
DEF_DATA_ROOT = "/bryck/cloudcp_sched_data"
DEF_S3_BASE = "s3://aditya/sch_test"
DEF_BATCHMETA = "/opt/bryck/bryckapi/downloads/bcloud_batchmeta"
DEF_TRANSFER_LOGS = "/opt/bryck/bryckapi/downloads/cloud_transfer_logs"
DEF_CLOUDCP_LOG = "/opt/bryck/bryckapi/downloads/cloud_transfer_logs/cloudcp.log"
DEF_SCHED_PY = "/opt/bryck/.venv/bryck/bin/python3"
DEF_SCHED_SCRIPT = (
    "/opt/bryck/.venv/bryck/lib/python3.10/site-packages/"
    "bryckcloud/lib/cloud/batch_scheduler.py"
)
DEF_DIR_PATH = "/opt/bryck/.venv/bryck/lib/python3.10/site-packages/bryckcloud/lib/cloud"
DEF_ENDPOINT = "https://10.10.10.103:9000"
DEF_CONFIG = "/etc/bryck/bryckcloud/config.json"
DEF_JOURNAL_TAG = "bryckcloud"
# journal capture margins: start the follower before the scheduler and keep it
# running after the scheduler exits so no head/tail lines are missed.
DEF_CAPTURE_LEAD = 3   # seconds: settle the follower before launching the scheduler
DEF_CAPTURE_DRAIN = 6  # seconds: keep capturing after the scheduler exits

# Embedded fallback if /etc config is unreadable (mirrors the BATCH block).
FALLBACK_BATCH = {
    "ZERO": {"BATCH_SIZE": 2000, "TARGET_SIZE_MB": 0, "OPEN_BATCHES": 4},
    "TINY": {"BATCH_SIZE": 511, "TARGET_SIZE_MB": 256, "OPEN_BATCHES": 8},
    "SMALL": {"BATCH_SIZE": 317, "TARGET_SIZE_MB": 2048, "OPEN_BATCHES": 8},
    "MEDIUM": {"BATCH_SIZE": 50, "TARGET_SIZE_MB": 10240, "OPEN_BATCHES": 8},
    "LARGE": {"BATCH_SIZE": 5, "TARGET_SIZE_MB": 51200, "OPEN_BATCHES": 8},
}

TIER_COLORS = {
    "zero": "#7f8c8d",
    "tiny": "#3498db",
    "small": "#2ecc71",
    "medium": "#f39c12",
    "large": "#e74c3c",
}
DEFAULT_TIER_ORDER = ["zero", "tiny", "small", "medium", "large"]

# journalctl default timestamp prefix: "Jul 31 05:15:33"
_TS_RE = re.compile(r"^([A-Z][a-z]{2}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2})\b")
_PENDING_RE = re.compile(r"Pending-(\d+)\s*:\s*(\{.*\})")
_RUNNING_RE = re.compile(r"Running with workers\s*:\s*(\{.*\})")
_FREE_RE = re.compile(r"free workers\s+(\d+)")

# cloudcp.log lines: "2026-08-03 09:56:04.707 [Stats][2340562] SUMMARY elapsed=2.27s
# files=511 small=511 large=0 skipped=0 bytes=8372224 (7.98 MiB) files/sec=225.2 throughput=3.52 MiB/s"
_CLOUDCP_TS_RE = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+)")
_CLOUDCP_STATS_RE = re.compile(
    r"\[Stats\]\[(\d+)\]\s+SUMMARY\s+elapsed=([\d.]+)s\s+files=(\d+)\s+"
    r"small=(\d+)\s+large=(\d+)\s+skipped=(\d+)\s+bytes=(\d+)\s+\([^)]*\)\s+"
    r"files/sec=([\d.]+)\s+throughput=([\d.]+)\s+MiB/s"
)
_CLOUDCP_BATCH_RE = re.compile(r"\[Batch\]\[(\d+)\]\s+done\s+records=(\d+)")

# journal PERF lines (from the broker), e.g.:
# "Aug 03 12:29:52 host bryckcloud[2757105]: PERF batch=batch_000027.txt files_count:50
#  total_size:5242880000 upload=78.50s total=78.50s rc=0 batch_file:.../transfer_541/batches/completed/medium/batch_000027.txt"
_PERF_RE = re.compile(
    r"PERF\s+batch=(\S+)\s+files_count:(\d+)\s+total_size:(\d+)\s+"
    r"upload=([\d.]+)s\s+total=([\d.]+)s\s+rc=(-?\d+)\s+batch_file:(\S+)"
)
_PERF_CAT_RE = re.compile(r"/completed/([^/]+)/")

# --- expected config.json values (preflight validation) ------------------------------
EXPECTED_ENDPOINT = "https://10.10.10.103:9000"
EXPECTED_TRANSFER_FLAGS = {
    "PERF_STATS": "True",
    "TXR_BATCH_VERIFYSIZE": "True",
    "TRANSFER_DISPATCH": "broker",
}
EXPECTED_CLOUDCP = {
    "MULTIPART_THRESHOLD_MB": 64,
    "MULTIPART_CHUNKSIZE_MB": 64,
    "SKIP_EXISTING": True,
    "TRANSFER_STATS": True,
    "STATS_INTERVAL_SEC": 0,
}
EXPECTED_TEST = {
    "SIM_TRANSFER": False,
    "SIM_SLEEP_TINY_MS": 0,
    "SIM_SLEEP_SMALL_MS": 0,
    "SIM_SLEEP_MEDIUM_MS": 0,
    "SIM_SLEEP_LARGE_MS": 0,
    "SIM_SLEEP_DOWNLOAD_MS": 0,
    "FAULT_FAIL_PERCENT": 0,
    "FAULT_CRASH_PERCENT": 0,
    "FAULT_CRASH_MODE": "abort",
    "FAULT_SEED": 0,
}


# =====================================================================================
# Preflight — host + config.json validation
# =====================================================================================
def _diff_block(name: str, expected: dict, actual) -> list[str]:
    """Deep exact-match report for a config block (missing/wrong/extra keys)."""
    if not isinstance(actual, dict):
        return [f"{name} block is missing or not an object (found {type(actual).__name__})"]
    out = []
    for k, v in expected.items():
        if k not in actual:
            out.append(f"{name}.{k} missing, expected {v!r}")
        elif actual[k] != v:
            out.append(f"{name}.{k} is {actual[k]!r}, expected {v!r}")
    for k in actual:
        if k not in expected:
            out.append(f"{name}.{k} is unexpected ({actual[k]!r}); remove it")
    return out


def preflight_checks(args) -> None:
    """Validate datagen + config.json before any transfer runs.

    Collects every problem and prints them together. In a real run a failure
    aborts (SystemExit); under --dry-run or --skip-config-check it only warns.
    """
    problems: list[str] = []

    dg = Path(args.datagen)
    if not dg.is_file():
        problems.append(f"datagen binary not found at {args.datagen}")
    elif not os.access(dg, os.X_OK):
        problems.append(f"datagen binary is not executable: {args.datagen}")

    cfg = None
    try:
        with open(args.config, encoding="utf-8") as fh:
            cfg = json.load(fh)
    except FileNotFoundError:
        problems.append(f"config.json not found at {args.config}")
    except (OSError, json.JSONDecodeError) as exc:
        problems.append(f"config.json unreadable/invalid ({args.config}): {exc}")

    if cfg is not None:
        local_aws = (cfg.get("CLOUD", {}) or {}).get("LOCAL_AWS")
        if getattr(args, "endpoint_overridden", False):
            if local_aws != args.endpoint_url:
                problems.append(
                    f"CLOUD.LOCAL_AWS is {local_aws!r} but --endpoint-url was set to "
                    f"{args.endpoint_url!r}; they must match")
        elif local_aws != EXPECTED_ENDPOINT:
            problems.append(
                f"CLOUD.LOCAL_AWS is {local_aws!r}; for the aditya bucket the "
                f"configuration in {args.config} should be {EXPECTED_ENDPOINT!r}")

        transfer = cfg.get("TRANSFER", {}) or {}
        for k, v in EXPECTED_TRANSFER_FLAGS.items():
            if transfer.get(k) != v:
                problems.append(f"TRANSFER.{k} is {transfer.get(k)!r}, expected {v!r}")

        for name, expected in (("CLOUDCP", EXPECTED_CLOUDCP), ("TEST", EXPECTED_TEST)):
            block = cfg.get(name)
            if block != expected:
                problems.extend(_diff_block(name, expected, block))

    if not problems:
        LOG.info("preflight: datagen + config.json OK")
        return

    msg = "\n".join(
        ["preflight configuration check FAILED:"]
        + [f"  - {p}" for p in problems]
        + ["", "Fix the configuration(s) above first, then re-run."]
    )
    if args.dry_run or getattr(args, "skip_config_check", False):
        LOG.warning("%s", msg)
        LOG.warning("continuing despite failures (%s)",
                    "dry-run" if args.dry_run else "--skip-config-check")
        return
    raise SystemExit("error: " + msg)


# =====================================================================================
# Catalog / dataset resolution
# =====================================================================================
def load_manifest(spec_dir: Path) -> dict:
    mf = spec_dir / "manifest.json"
    if not mf.is_file():
        raise SystemExit(f"error: manifest.json not found at {mf}")
    with mf.open(encoding="utf-8") as fh:
        return json.load(fh)


def manifest_index(manifest: dict) -> dict[int, dict]:
    return {int(d["number"]): d for d in manifest.get("datasets", [])}


def _norm(x: str) -> set[str]:
    b = x.upper()
    return {b, b.replace("SCH-", ""), b.replace("SCH-", "").replace("-", ""), b.replace("-", "")}


def resolve_one(sel: str, by_num: dict[int, dict]) -> dict:
    s = sel.strip().upper()
    if s.isdigit():
        n = int(s)
        if n in by_num:
            return by_num[n]
        raise SystemExit(f"error: dataset number {n} out of range (have {sorted(by_num)})")
    want = _norm(s)
    for d in by_num.values():
        if want & _norm(d["id"]):
            return d
    raise SystemExit(f"error: could not resolve dataset '{sel}'")


def select_datasets(args, by_num: dict[int, dict]) -> list[dict]:
    if args.all:
        return [by_num[n] for n in sorted(by_num)]
    if args.from_ is not None or args.to is not None:
        lo = args.from_ if args.from_ is not None else min(by_num)
        hi = args.to if args.to is not None else max(by_num)
        if lo > hi:
            raise SystemExit("error: --from must be <= --to")
        picked = [by_num[n] for n in sorted(by_num) if lo <= n <= hi]
        if not picked:
            raise SystemExit(f"error: no datasets in range {lo}..{hi}")
        return picked
    if not args.dataset:
        raise SystemExit("error: provide a dataset selector, --from/--to, or --all")
    return [resolve_one(args.dataset, by_num)]


# =====================================================================================
# Config (BATCH tiers)
# =====================================================================================
def load_batch_config(path: str) -> dict:
    try:
        with open(path, encoding="utf-8") as fh:
            cfg = json.load(fh)
        batch = cfg.get("BATCH", {})
        tiers = {k: v for k, v in batch.items() if isinstance(v, dict) and "BATCH_SIZE" in v}
        if tiers:
            return tiers
    except Exception as exc:  # noqa: BLE001 - best effort, fall back
        LOG.debug("could not read BATCH config from %s: %s", path, exc)
    LOG.info("using embedded fallback BATCH config")
    return dict(FALLBACK_BATCH)


def tier_order_from_config(batch_cfg: dict) -> list[str]:
    order = [t.lower() for t in ("ZERO", "TINY", "SMALL", "MEDIUM", "LARGE") if t in batch_cfg]
    for k in batch_cfg:
        if k.lower() not in order:
            order.append(k.lower())
    return order or list(DEFAULT_TIER_ORDER)


# =====================================================================================
# External command helper
# =====================================================================================
def run_cmd(cmd: list[str], dry_run: bool, check: bool = True) -> int:
    LOG.info("$ %s", " ".join(cmd))
    if dry_run:
        return 0
    proc = subprocess.run(cmd, check=False)
    if check and proc.returncode != 0:
        raise SystemExit(f"error: command failed ({proc.returncode}): {' '.join(cmd)}")
    return proc.returncode


# =====================================================================================
# Step 1 — datagen
# =====================================================================================
def spec_files_in_order(spec_dir: Path, dataset_id: str) -> list[Path]:
    d = spec_dir / dataset_id
    if not d.is_dir():
        raise SystemExit(f"error: spec dir not found: {d}")
    specs = sorted(
        d.glob("L*.y*ml"),
        key=lambda p: int(re.match(r"L(\d+)_", p.name).group(1)) if re.match(r"L(\d+)_", p.name) else 999,
    )
    if not specs:
        raise SystemExit(f"error: no L*_*.yaml spec files under {d}")
    return specs


def run_datagen(specs: list[Path], datagen: str, flag: str, dry_run: bool) -> None:
    for sp in specs:
        run_cmd([datagen, flag, str(sp)], dry_run=dry_run, check=True)


# =====================================================================================
# Step 1.5 — enumeration order json
# =====================================================================================
def write_enumeration_order(dataset: dict, specs: list[Path], out_dir: Path) -> dict:
    levels = []
    for sp in specs:
        m = re.match(r"L(\d+)_([A-Za-z]+)\.", sp.name)
        if m:
            levels.append({"level": int(m.group(1)), "tier": m.group(2).lower(), "spec": sp.name})
    payload = {
        "dataset_id": dataset["id"],
        "number": dataset["number"],
        "profile": dataset.get("profile"),
        "enumeration_order": [t.lower() for t in dataset.get("enumeration_order", [])],
        "spec_levels": levels,
        "manifest_levels": dataset.get("levels", []),
        "totals": dataset.get("totals", {}),
        "note": "BFS chain order L0->L4; this is the deterministic enumeration oracle.",
    }
    out = out_dir / "enumeration_order.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    LOG.info("enumeration order -> %s : %s", out.name, " -> ".join(payload["enumeration_order"]))
    return payload


# =====================================================================================
# Step 3 — transfer id allocation
# =====================================================================================
def next_transfer_id(batchmeta_dir: str) -> int:
    d = Path(batchmeta_dir)
    ids = []
    if d.is_dir():
        for child in d.iterdir():
            m = re.fullmatch(r"transfer_(\d+)", child.name)
            if m:
                ids.append(int(m.group(1)))
    nxt = (max(ids) + 1) if ids else 501
    return nxt


def create_transfer_dir(batchmeta_dir: str, tr_id: int, dry_run: bool) -> Path:
    p = Path(batchmeta_dir) / f"transfer_{tr_id}"
    LOG.info("transfer-dir: %s", p)
    if not dry_run:
        p.mkdir(parents=True, exist_ok=True)
    return p


def cleanup_data_dir(src: str, data_root: str, dry_run: bool) -> None:
    """Remove the materialised data dir under the data-root (--delete).

    Guarded so it only ever deletes a path strictly under --data-root, never
    the data-root itself or something shorter/unexpected.
    """
    root = str(src).rstrip("/")
    base = str(data_root).rstrip("/")
    LOG.info("--delete: removing materialised data dir %s", root)
    if not (base and root.startswith(base + "/") and len(root) > len(base) + 1):
        LOG.error("refusing to delete unexpected path (not under %s): %s", base, root)
        return
    if dry_run:
        LOG.info("$ rm -rf %s", root)
        return
    p = Path(root)
    if p.is_dir():
        shutil.rmtree(p)
        LOG.info("removed %s", root)
    else:
        LOG.warning("data dir not found, nothing to delete: %s", root)


def clear_transfer_bucket(dst: str, endpoint_url: str, dry_run: bool) -> None:
    """Remove the uploaded objects under the dataset's S3 prefix (--clear-bucket).

    Clears only the dataset prefix (<s3-base>/<id>) via `aws s3 rm --recursive`;
    the bucket itself is preserved. A non-zero rc (e.g. empty prefix) is
    logged and ignored so it never aborts the run.
    """
    LOG.info("--clear-bucket: removing objects under %s (bucket preserved)", dst)
    cmd = ["aws", "s3", "rm", dst, "--recursive", "--endpoint-url", endpoint_url]
    rc = run_cmd(cmd, dry_run=dry_run, check=False)
    if not dry_run and rc != 0:
        LOG.warning("bucket clear returned rc=%s (continuing)", rc)


# =====================================================================================
# Step 4 — journalctl capture
# =====================================================================================
class JournalCapture:
    """Single `sudo journalctl -f` follower fanned into 3 filtered files + a raw log."""

    def __init__(self, tag: str, tr_id: int, log_dir: Path, since: _dt.datetime, dry_run: bool,
                 lead_sec: float = DEF_CAPTURE_LEAD, drain_sec: float = DEF_CAPTURE_DRAIN):
        self.tag = tag
        self.tr_id = tr_id
        self.dry_run = dry_run
        self.lead_sec = lead_sec
        self.drain_sec = drain_sec
        self.pending_path = log_dir / f"pending_{tr_id}.log"
        self.free_path = log_dir / f"free_workers_{tr_id}.log"
        self.running_path = log_dir / f"running_workers_{tr_id}.log"
        self.perf_path = log_dir / f"perf_{tr_id}.log"
        self.raw_path = log_dir / f"raw_{tr_id}.log"
        self._since = since
        self._proc: subprocess.Popen | None = None
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()

    def start(self) -> None:
        """Spawn the follower and block until it is attached (lead settle)."""
        for p in (self.pending_path, self.free_path, self.running_path,
                  self.perf_path, self.raw_path):
            p.write_text("", encoding="utf-8")
        if self.dry_run:
            LOG.info("[dry-run] would start journalctl follower for tag %s", self.tag)
            return
        # Backdate --since by the lead margin so the very first lines are never missed.
        since_dt = self._since - _dt.timedelta(seconds=max(self.lead_sec, 1))
        since = since_dt.strftime("%Y-%m-%d %H:%M:%S")
        cmd = ["sudo", "journalctl", "-f", "-t", self.tag, "--since", since, "-o", "short"]
        LOG.info("$ %s", " ".join(cmd))
        self._proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            text=True, bufsize=1, preexec_fn=os.setsid,
        )
        self._thread = threading.Thread(target=self._reader, daemon=True)
        self._thread.start()
        # Give journalctl time to attach and start following before the scheduler runs.
        if self.lead_sec > 0:
            LOG.info("capture lead: waiting %.1fs for journalctl to attach", self.lead_sec)
            time.sleep(self.lead_sec)

    def _reader(self) -> None:
        pend = self.pending_path.open("a", encoding="utf-8")
        free = self.free_path.open("a", encoding="utf-8")
        run = self.running_path.open("a", encoding="utf-8")
        perf = self.perf_path.open("a", encoding="utf-8")
        raw = self.raw_path.open("a", encoding="utf-8")
        try:
            assert self._proc and self._proc.stdout
            for line in self._proc.stdout:
                if self._stop.is_set():
                    break
                raw.write(line)
                raw.flush()
                if f"Pending-{self.tr_id}" in line:
                    pend.write(line)
                    pend.flush()
                elif "free workers" in line:
                    free.write(line)
                    free.flush()
                elif "Running with workers" in line:
                    run.write(line)
                    run.flush()
                elif "PERF batch=" in line and f"transfer_{self.tr_id}" in line:
                    perf.write(line)
                    perf.flush()
        except Exception as exc:  # noqa: BLE001
            LOG.debug("journal reader stopped: %s", exc)
        finally:
            for fh in (pend, free, run, perf, raw):
                fh.close()

    def stop(self) -> None:
        if self.dry_run or self._proc is None:
            self._stop.set()
            return
        # Drain: keep capturing after the scheduler exits so late/tail lines land.
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


class CloudcpLogCapture:
    """Tail the shared cloudcp.log into cloudcplogs.txt across the test window.

    Spawned just before JournalCapture.start() (whose lead settle covers this tail
    too) and stopped right after JournalCapture.stop() (whose drain already elapsed),
    so it records exactly the initiation -> completion window.
    """

    def __init__(self, log_path: str, out_path: Path, dry_run: bool):
        self.log_path = log_path
        self.out_path = out_path
        self.dry_run = dry_run
        self._proc: subprocess.Popen | None = None
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()

    def start(self) -> None:
        """Spawn `tail -F -n 0` on cloudcp.log (only lines from now onward)."""
        self.out_path.write_text("", encoding="utf-8")
        if self.dry_run:
            LOG.info("[dry-run] would tail %s -> %s", self.log_path, self.out_path.name)
            return
        # -F retries if the file is rotated/absent; -n 0 starts from the current end.
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
        except Exception as exc:  # noqa: BLE001
            LOG.debug("cloudcp.log tail stopped: %s", exc)
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
        try:
            n = sum(1 for _ in self.out_path.open(encoding="utf-8", errors="replace"))
            LOG.info("cloudcp.log capture: %d lines -> %s", n, self.out_path.name)
        except OSError:
            pass


# =====================================================================================
# Step 5 — scheduler
# =====================================================================================
def build_scheduler_cmd(args, tr_id: int, src: str, dst: str, base_src: str, transfer_dir: Path) -> list[str]:
    cmd = [
        args.scheduler_python, args.scheduler_script,
        str(tr_id), "upload", src, dst, base_src,
        "--transfer-dir", str(transfer_dir),
        "--dir-path", args.dir_path,
        "--endpoint-url", args.endpoint_url,
    ]
    if args.poll_interval is not None:
        cmd += ["--poll-interval", str(args.poll_interval)]
    return cmd


def run_scheduler(cmd: list[str], dry_run: bool) -> int:
    LOG.info("$ %s", " ".join(cmd))
    if dry_run:
        return 0
    proc = subprocess.run(cmd, check=False)
    LOG.info("scheduler exited with code %s", proc.returncode)
    return proc.returncode


# =====================================================================================
# Step 5b — pause / resume (kill & restart) — plan Group E.1
# =====================================================================================
DEF_PR_KILL_AFTER = 5.0        # default seconds before the broker is killed mid-run
PR_LATE_FRACTION = 0.80        # "kill late" fires once this fraction of files is done
PR_LATE_MAX_WAIT = 900         # safety cap (s) while waiting for the late-kill target

# Each case kills the broker (or a worker) mid-run then resumes on the SAME
# transfer-id/transfer-dir. mode:
#   "broker"      SIGKILL the whole broker process group, then resume
#   "broker_late" like "broker" but fire once PR_LATE_FRACTION of files are done
#   "sigint"      SIGINT the broker (graceful stop), then resume
#   "worker"      SIGKILL a single aws_transfer.py child; the broker SURVIVES and
#                 re-dispatches — a single run, no resume
PAUSE_RESUME_CASES = [
    {"id": "SCH-PR-01", "dataset": "SCH-DEEP-01", "kill_after": 5.0, "cycles": 1,
     "mode": "broker", "title": "Broker SIGKILL, single resume",
     "summary": "Kill the broker mid-run; resume on the same id+dir."},
    {"id": "SCH-PR-02", "dataset": "SCH-DEEP-02", "kill_after": 8.0, "cycles": 1,
     "mode": "broker", "title": "Kill mid-multipart (LARGE/MEDIUM)",
     "summary": "Kill while a bandwidth-tier multipart batch is inflight."},
    {"id": "SCH-PR-03", "dataset": "SCH-DEEP-01", "kill_after": 2.0, "cycles": 1,
     "mode": "broker", "title": "Kill very early (baseline ~0)",
     "summary": "Kill before any batch completes; resume uploads the full backlog."},
    {"id": "SCH-PR-04", "dataset": "SCH-ORD-01", "kill_after": None, "cycles": 1,
     "mode": "broker_late", "title": "Kill late (tail only)",
     "summary": "Kill after ~80% of files are done; resume dispatches only the tail."},
    {"id": "SCH-PR-05", "dataset": "SCH-DEEP-01", "kill_after": 5.0, "cycles": 2,
     "mode": "broker", "title": "Double kill / two resume cycles",
     "summary": "Kill, resume, kill again, resume to completion."},
    {"id": "SCH-PR-06", "dataset": "SCH-DEEP-01", "kill_after": 5.0, "cycles": 1,
     "mode": "worker", "title": "Worker crash, broker survives",
     "summary": "SIGKILL one aws_transfer.py worker; the broker re-dispatches it."},
    {"id": "SCH-PR-07", "dataset": "SCH-ORD-01", "kill_after": 5.0, "cycles": 1,
     "mode": "broker", "reconcile": "kept",
     "title": ".lst-backed inprogress kept for fallback",
     "summary": "Resume leaves a .lst-backed inprogress batch for the fallback."},
    {"id": "SCH-PR-08", "dataset": "SCH-ORD-01", "kill_after": 3.0, "cycles": 1,
     "mode": "broker", "reconcile": "reset",
     "title": "Stale inprogress (no .lst) requeued",
     "summary": "Resume requeues a stale inprogress batch back to pending."},
    {"id": "SCH-PR-09", "dataset": "SCH-DEEP-01", "kill_after": 5.0, "cycles": 1,
     "mode": "sigint", "title": "Graceful SIGINT pause + resume",
     "summary": "SIGINT the broker (clean shutdown); resume completes."},
]
PR_CASE_BY_ID = {c["id"]: c for c in PAUSE_RESUME_CASES}

# resume-reconcile lines the broker logs in _reset_stale_inprogress()
_PR_RECONCILE_RE = re.compile(
    r"resume reconcile.*?reset\s+(\d+)\s+stale.*?left\s+(\d+)\s+for the fallback",
    re.IGNORECASE)


def count_ok_in_cloudcp_log(log_path: str, tr_id: int) -> int:
    """Sum ``ok=N`` from ``[Batch]…done`` lines for transfer *tr_id* in cloudcp.log.

    This is the "how much is already durably uploaded" signal the resume relies
    on; the csv path on each line ties the batch to its transfer id.
    """
    p = Path(log_path)
    if not p.is_file():
        return 0
    marker = f"cloud_transfer_{tr_id}/"
    total = 0
    try:
        with p.open(encoding="utf-8", errors="replace") as fh:
            for line in fh:
                if marker not in line:
                    continue
                m = re.search(r"\[Batch\].*\bdone\b.*\bok=(\d+)", line)
                if m:
                    total += int(m.group(1))
    except OSError:
        pass
    return total


def csv_duplicate_stats(csv_path: Path) -> dict:
    """Return {distinct, rows, duplicates, failed} keyed on the S3 object path."""
    seen: dict[str, int] = {}
    rows = failed = 0
    try:
        with csv_path.open(newline="", encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                key = (row.get("s3path") or row.get("local_path") or "").strip()
                if not key:
                    continue
                rows += 1
                seen[key] = seen.get(key, 0) + 1
                if (row.get("status") or "").strip().upper() != "SUCCESS":
                    failed += 1
    except OSError:
        pass
    return {
        "distinct": len(seen),
        "rows": rows,
        "duplicates": sum(c - 1 for c in seen.values() if c > 1),
        "failed": failed,
    }


def _pgrep_children(pid: int) -> list[int]:
    """Direct child pids of *pid* (POSIX; via pgrep -P)."""
    try:
        out = subprocess.run(["pgrep", "-P", str(pid)], capture_output=True, text=True)
        return [int(x) for x in out.stdout.split()]
    except (OSError, ValueError):
        return []


def _proc_cmdline(pid: int) -> str:
    try:
        with open(f"/proc/{pid}/cmdline", "rb") as fh:
            return fh.read().replace(b"\x00", b" ").decode("utf-8", "replace")
    except OSError:
        return ""


def _find_worker_pid(broker_pid: int) -> int | None:
    """Pick one aws_transfer.py worker child of the broker (not the enumerator)."""
    for child in _pgrep_children(broker_pid):
        if "aws_transfer.py" in _proc_cmdline(child):
            return child
    return None


def _launch_broker(cmd: list[str]) -> subprocess.Popen:
    """Popen the broker in its own process group so we can signal the whole tree."""
    LOG.info("$ %s", " ".join(cmd))
    return subprocess.Popen(cmd, preexec_fn=os.setsid if os.name == "posix" else None)


def _signal_group(proc: subprocess.Popen, sig: int) -> None:
    try:
        if os.name == "posix":
            os.killpg(os.getpgid(proc.pid), sig)
        else:
            proc.send_signal(sig)
    except (ProcessLookupError, OSError):
        pass


def _run_broker_with_kill(cmd: list[str], mode: str, kill_after: float,
                          tr_id: int, cloudcp_log: str, expected_total: int) -> dict:
    """Launch the broker, kill it (or a worker) mid-run, and return kill info."""
    proc = _launch_broker(cmd)
    info = {"mode": mode, "killed": False, "worker_pid": None, "rc": None}

    if mode == "worker":
        # let a worker come up, kill it, then let the broker finish on its own
        deadline = time.time() + kill_after
        wpid = None
        while time.time() < deadline and proc.poll() is None:
            wpid = _find_worker_pid(proc.pid)
            if wpid:
                break
            time.sleep(0.2)
        if wpid and proc.poll() is None:
            LOG.info("SIGKILL worker pid=%s (broker %s survives)", wpid, proc.pid)
            try:
                os.kill(wpid, signal.SIGKILL)
                info["killed"] = True
                info["worker_pid"] = wpid
            except (ProcessLookupError, OSError):
                pass
        else:
            LOG.warning("no aws_transfer.py worker seen within %.1fs", kill_after)
        info["rc"] = proc.wait()
        LOG.info("broker exited rc=%s after worker crash", info["rc"])
        return info

    # broker / broker_late / sigint: wait for the kill trigger, then stop the broker
    if mode == "broker_late":
        target = max(1, int(expected_total * PR_LATE_FRACTION))
        LOG.info("kill-late: waiting until ok>=%d (%.0f%% of %d)",
                 target, PR_LATE_FRACTION * 100, expected_total)
        deadline = time.time() + PR_LATE_MAX_WAIT
        while time.time() < deadline and proc.poll() is None:
            if count_ok_in_cloudcp_log(cloudcp_log, tr_id) >= target:
                break
            time.sleep(0.5)
    else:
        end = time.time() + kill_after
        while time.time() < end and proc.poll() is None:
            time.sleep(0.1)

    if proc.poll() is None:
        sig = signal.SIGINT if mode == "sigint" else signal.SIGKILL
        LOG.info("stopping broker pid=%s with %s (mode=%s)",
                 proc.pid, getattr(sig, "name", sig), mode)
        _signal_group(proc, sig)
        info["killed"] = True
        try:
            info["rc"] = proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            LOG.warning("broker did not exit after signal; SIGKILL")
            _signal_group(proc, signal.SIGKILL)
            info["rc"] = proc.wait()
    else:
        info["rc"] = proc.returncode
        LOG.warning("broker already exited (rc=%s) before kill — shorten --pr-kill-after",
                    info["rc"])
    return info


def run_pause_resume_case(case: dict, args, spec_dir: Path, out_root: Path) -> dict:
    """Run one pause/resume case: kill mid-run, resume, validate no duplicates."""
    cid = case["id"]
    ds_sel = case["dataset"]
    mode = case["mode"]
    kill_after = args.pr_kill_after if args.pr_kill_after is not None else \
        (case.get("kill_after") or DEF_PR_KILL_AFTER)
    cycles = case.get("cycles", 1)

    LOG.info("=" * 70)
    LOG.info("PAUSE/RESUME %s — %s  (dataset=%s mode=%s kill_after=%s cycles=%s)",
             cid, case["title"], ds_sel, mode, kill_after, cycles)

    manifest = load_manifest(spec_dir)
    by_num = manifest_index(manifest)
    dataset = resolve_one(ds_sel, by_num)
    ds_id = dataset["id"]
    expected_total = build_structure_payload(dataset)["total_files"]

    result = {
        "id": cid, "title": case["title"], "summary": case["summary"],
        "dataset_id": ds_id, "mode": mode, "kill_after": kill_after, "cycles": cycles,
        "expected_total": expected_total, "status": "PENDING", "errors": [],
        "pr_baseline_ok": 0, "pr_final_ok": 0, "reconcile": {},
        "csv": {}, "transfer_id": None,
    }

    specs = spec_files_in_order(spec_dir, ds_id)
    if args.skip_datagen:
        LOG.info("--skip-datagen: reusing existing data for %s", ds_id)
    else:
        run_datagen(specs, args.datagen, args.datagen_flag, args.dry_run)

    tr_id = next_transfer_id(args.batchmeta_dir)
    transfer_dir = create_transfer_dir(args.batchmeta_dir, tr_id, args.dry_run)
    result["transfer_id"] = tr_id

    report_dir = out_root / f"pause_resume_{cid}_{tr_id}"
    log_dir = report_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    src = f"{args.data_root.rstrip('/')}/{ds_id}"
    dst = f"{args.s3_base.rstrip('/')}/{ds_id}"
    cmd = build_scheduler_cmd(args, tr_id, src, dst, src, transfer_dir)

    if args.dry_run:
        LOG.info("[dry-run] would launch broker, %s after %ss (×%s cycle(s)), then resume",
                 "kill worker" if mode == "worker" else "kill broker", kill_after, cycles)
        result["status"] = "DRY_RUN"
        _pr_cleanup(args, src, dst)
        return result

    if os.name != "posix":
        result["status"] = "SKIP"
        result["errors"].append("pause/resume kills need a POSIX host")
        return result

    try:
        # --- kill cycle(s) -------------------------------------------------
        for cyc in range(cycles):
            before = count_ok_in_cloudcp_log(args.cloudcp_log, tr_id)
            LOG.info("--- kill cycle %d/%d (ok so far=%d) ---", cyc + 1, cycles, before)
            kinfo = _run_broker_with_kill(cmd, mode, kill_after, tr_id,
                                          args.cloudcp_log, expected_total)
            after = count_ok_in_cloudcp_log(args.cloudcp_log, tr_id)
            LOG.info("cycle %d: ok before=%d after=%d (delta=%d) killed=%s",
                     cyc + 1, before, after, after - before, kinfo["killed"])

        result["pr_baseline_ok"] = count_ok_in_cloudcp_log(args.cloudcp_log, tr_id)
        # snapshot inprogress/.lst state before the resume reconcile
        result["reconcile"]["inprogress_before"] = _inprogress_snapshot(transfer_dir)

        # worker mode is a single run (broker survived + finished); no resume run
        if mode == "worker":
            final_rc = kinfo["rc"]
        else:
            # --- final resume run to completion (same id + dir) ------------
            LOG.info("--- final resume run (transfer_id=%s) ---", tr_id)
            start_dt = _dt.datetime.now()
            cap = JournalCapture(args.journal_tag, tr_id, log_dir, start_dt, args.dry_run,
                                 lead_sec=args.capture_lead, drain_sec=args.capture_drain)
            cloud_cap = CloudcpLogCapture(args.cloudcp_log, report_dir / "cloudcplogs.txt",
                                          args.dry_run)
            cloud_cap.start()
            cap.start()
            final_rc = run_scheduler(cmd, args.dry_run)
            cap.stop()
            cloud_cap.stop()
            result["reconcile"].update(_scan_reconcile(cap.raw_path))

        result["pr_final_ok"] = count_ok_in_cloudcp_log(args.cloudcp_log, tr_id)

        # --- validate ------------------------------------------------------
        csv_src = find_results_csv(args.transfer_logs_dir, tr_id)
        if csv_src:
            shutil.copy2(csv_src, report_dir / f"transfer_report_{tr_id}.csv")
            result["csv"] = csv_duplicate_stats(csv_src)
        else:
            result["errors"].append("results CSV not found")

        _pr_evaluate(result, case, final_rc)
        result["scheduler_exit"] = final_rc
        (report_dir / "run_meta.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
        _pr_write_summary(report_dir / "summary.txt", result)
    except Exception as exc:  # noqa: BLE001
        result["status"] = "FAIL"
        result["errors"].append(f"unexpected exception: {exc}")
        LOG.exception("pause/resume %s crashed", cid)
    finally:
        _pr_cleanup(args, src, dst)

    LOG.info("%s -> %s", cid, result["status"])
    return result


def _inprogress_snapshot(transfer_dir: Path) -> dict:
    """Count inprogress batches and how many have a sibling retry .lst on disk."""
    base = transfer_dir / "batches" / "inprogress"
    total = with_lst = 0
    if base.is_dir():
        for txt in base.rglob("*.txt"):
            total += 1
            if list(txt.parent.glob(txt.stem + "*.lst")):
                with_lst += 1
    return {"inprogress": total, "with_lst": with_lst}


def _scan_reconcile(raw_log: Path) -> dict:
    """Extract reset/kept counts from the broker's resume-reconcile journal line."""
    out = {"reset": None, "kept": None}
    try:
        text = raw_log.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return out
    m = _PR_RECONCILE_RE.search(text)
    if m:
        out["reset"] = int(m.group(1))
        out["kept"] = int(m.group(2))
    return out


def _pr_evaluate(result: dict, case: dict, final_rc: int | None) -> None:
    """Apply the resume oracle (§ plan E.1) and set result['status']/errors."""
    errs = result["errors"]
    csv = result.get("csv", {})
    exp = result["expected_total"]

    if final_rc != 0:
        errs.append(f"scheduler exit rc={final_rc} (want 0)")
    if csv:
        if csv["duplicates"] != 0:
            errs.append(f"{csv['duplicates']} duplicate object(s) uploaded")
        if csv["failed"] != 0:
            errs.append(f"{csv['failed']} non-SUCCESS row(s) in CSV")
        if csv["distinct"] != exp:
            errs.append(f"distinct objects {csv['distinct']} != expected {exp}")
    # reconcile-branch assertion for the two dedicated cases (soft: warn only)
    want = case.get("reconcile")
    rec = result.get("reconcile", {})
    if want == "reset" and not (rec.get("reset") or 0) >= 1:
        LOG.warning("%s: expected a stale-inprogress requeue but reconcile reset=%s",
                    case["id"], rec.get("reset"))
    if want == "kept" and not (rec.get("kept") or 0) >= 1:
        LOG.warning("%s: expected a .lst-backed batch kept for the fallback but kept=%s",
                    case["id"], rec.get("kept"))

    result["status"] = "PASS" if not errs else "FAIL"


def _pr_write_summary(path: Path, r: dict) -> None:
    csv = r.get("csv", {})
    lines = [
        "Pause / Resume Case Summary",
        "=" * 60,
        f"case            : {r['id']} — {r['title']}",
        f"dataset         : {r['dataset_id']}",
        f"transfer id     : {r['transfer_id']}",
        f"mode            : {r['mode']}  kill_after={r['kill_after']}s  cycles={r['cycles']}",
        f"expected files  : {r['expected_total']}",
        f"cloudcp.log ok  : baseline={r['pr_baseline_ok']}  final={r['pr_final_ok']}",
        f"reconcile       : {r.get('reconcile', {})}",
        f"csv             : distinct={csv.get('distinct')} rows={csv.get('rows')} "
        f"dups={csv.get('duplicates')} failed={csv.get('failed')}",
        f"scheduler exit  : {r.get('scheduler_exit')}",
        f"result          : {r['status']}",
    ]
    if r["errors"]:
        lines += ["", "Errors:"] + [f"  - {e}" for e in r["errors"]]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _pr_cleanup(args, src: str, dst: str) -> None:
    if args.delete or args.cleanup:
        cleanup_data_dir(src, args.data_root, args.dry_run)
    if args.clear_bucket or args.cleanup:
        clear_transfer_bucket(dst, args.endpoint_url, args.dry_run)


def run_pause_resume_suite(args, spec_dir: Path, out_root: Path) -> int:
    cases = _select_pr_cases(args)
    if os.name != "posix" and not args.dry_run:
        LOG.warning("pause/resume targets the Linux bryck host; use --dry-run to preview")
    results = [run_pause_resume_case(c, args, spec_dir, out_root) for c in cases]

    LOG.info("=" * 70)
    npass = nfail = 0
    for r in results:
        LOG.info("  %-10s %-8s %-13s dups=%s exit=%s",
                 r["id"], r["status"], r["dataset_id"],
                 r.get("csv", {}).get("duplicates"), r.get("scheduler_exit"))
        npass += r["status"] in ("PASS", "DRY_RUN", "SKIP")
        nfail += r["status"] == "FAIL"
    (out_root / "pause_resume_results.json").write_text(
        json.dumps(results, indent=2), encoding="utf-8")
    LOG.info("pause/resume: %d ok, %d failed", npass, nfail)
    return 1 if nfail else 0


def _select_pr_cases(args) -> list[dict]:
    if args.pause_resume_case:
        picked = []
        for tok in args.pause_resume_case.split(","):
            tok = tok.strip().upper()
            if tok in PR_CASE_BY_ID:
                picked.append(PR_CASE_BY_ID[tok])
            else:
                raise SystemExit(f"error: unknown pause/resume case '{tok}' "
                                 f"(have {', '.join(PR_CASE_BY_ID)})")
        return picked
    return list(PAUSE_RESUME_CASES)


# =====================================================================================
# Step 6/8 — parse logs & CSV
# =====================================================================================
def parse_ts(line: str, year: int) -> _dt.datetime | None:
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


def parse_capture(cap: JournalCapture, year: int) -> dict:
    """Return merged, forward-filled timeline + raw series."""
    pending_events, running_events, free_events = [], [], []

    for line in cap.pending_path.read_text(encoding="utf-8").splitlines():
        ts = parse_ts(line, year)
        m = _PENDING_RE.search(line)
        if ts and m:
            pending_events.append((ts, _safe_dict(m.group(2))))
    for line in cap.running_path.read_text(encoding="utf-8").splitlines():
        ts = parse_ts(line, year)
        m = _RUNNING_RE.search(line)
        if ts and m:
            running_events.append((ts, _safe_dict(m.group(1))))
    for line in cap.free_path.read_text(encoding="utf-8").splitlines():
        ts = parse_ts(line, year)
        m = _FREE_RE.search(line)
        if ts and m:
            free_events.append((ts, int(m.group(1))))

    all_ts = sorted({ts for ts, _ in pending_events} | {ts for ts, _ in running_events} | {ts for ts, _ in free_events})
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
    tiers_seen = set()
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


def find_results_csv(transfer_logs_dir: str, tr_id: int) -> Path | None:
    p = Path(transfer_logs_dir) / f"cloud_transfer_{tr_id}" / f"transfer_report_{tr_id}.csv"
    return p if p.is_file() else None


def parse_results_csv(csv_path: Path, year: int) -> dict:
    rows, completions = [], []
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
    comp_rel = []
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


# =====================================================================================
# Step 6b — throughput from cloudcp.log (per batch, per size-tier)
# =====================================================================================
def _tier_for_avg_size(avg_size: float, tier_sizes: list[tuple[str, int]]) -> str:
    """Map a batch's average file size to the nearest manifest size-tier."""
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


def _minmaxavg(vals: list[float]) -> dict:
    if not vals:
        return {"min": 0.0, "max": 0.0, "avg": 0.0, "median": 0.0}
    s = sorted(vals)
    n = len(s)
    mid = n // 2
    median = s[mid] if n % 2 else (s[mid - 1] + s[mid]) / 2
    return {"min": s[0], "max": s[-1], "avg": sum(s) / n, "median": median}


def parse_cloudcp_log(path: Path, tier_order: list[str],
                      tier_sizes: list[tuple[str, int]]) -> dict:
    """Parse per-batch throughput from cloudcp.log (cloudcplogs.txt).

    Each batch emits a `[Stats] SUMMARY ... throughput=<T> MiB/s` line plus a
    matching `[Batch][<pid>] done` line. Every batch is attributed to a size-tier
    by its average file size (bytes/files), then min/max/avg/median throughput is
    aggregated per tier and overall.
    """
    empty = {"batches": [], "per_tier": {}, "overall": {}, "tiers": []}
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
    per_tier = {}
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


# =====================================================================================
# Step 6c — batch completion from journal PERF lines (per batch, per category)
# =====================================================================================
def _mode_rounded(vals: list[float], ndigits: int = 1) -> float:
    """Most frequent value after rounding to `ndigits` decimals (ties -> smallest)."""
    if not vals:
        return 0.0
    counts: dict[float, int] = {}
    for v in vals:
        r = round(v, ndigits)
        counts[r] = counts.get(r, 0) + 1
    best = max(counts.values())
    return min(k for k, c in counts.items() if c == best)


def parse_perf_log(path: Path, tier_order: list[str]) -> dict:
    """Parse per-batch completion from the broker's journal PERF lines.

    Each completed batch emits a `PERF batch=<name> files_count:<n> total_size:<b>
    upload=<u>s total=<t>s rc=<rc> batch_file:.../completed/<tier>/<name>` line.
    Batches are grouped by the path's <tier>; per tier we report min/max/mode +
    avg for both upload and total time, plus the count of rc!=0 batches.
    """
    empty = {"batches": [], "per_tier": {}, "overall": {}, "tiers": []}
    if not path.is_file():
        return empty
    text = path.read_text(encoding="utf-8", errors="replace")

    batches: list[dict] = []
    for line in text.splitlines():
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
    per_tier = {}
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


# =====================================================================================
# Step 7 — HTML report (self-contained)
# =====================================================================================
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


def build_structure_payload(dataset: dict) -> dict:
    """Directory structure, per-tier file sizes and the enumeration expectation.

    Built from the manifest's per-level metadata (BFS chain order L0->L4), so it
    documents exactly what datagen materialises and the order the single-process
    BFS walker will enumerate each size-tier.
    """
    levels = []
    for lv in dataset.get("levels", []):
        num = int(lv.get("num_files", 0) or 0)
        sz = int(lv.get("size_bytes", 0) or 0)
        levels.append({
            "level": lv.get("level"),
            "tier": (lv.get("tier") or "").lower(),
            "root": lv.get("root"),
            "num_files": num,
            "file_size_bytes": sz,
            "total_bytes": num * sz,
            "batches": lv.get("batches"),
            "content": lv.get("content"),
            "seed": lv.get("seed"),
        })
    return {
        "levels": levels,
        "total_files": sum(l["num_files"] for l in levels),
        "total_bytes": sum(l["total_bytes"] for l in levels),
        "enumeration_order": [t.lower() for t in dataset.get("enumeration_order", [])],
    }


def build_report_payload(dataset, tr_id, enum_payload, cap_data, csv_summary,
                         meta, batch_cfg, tier_order, structure, throughput, perf) -> dict:
    tiers = list(dict.fromkeys(tier_order + cap_data.get("tiers_seen", [])))
    return {
        "meta": meta,
        "dataset": {
            "id": dataset["id"], "number": dataset["number"],
            "profile": dataset.get("profile"), "totals": dataset.get("totals", {}),
        },
        "transfer_id": tr_id,
        "enumeration_order": enum_payload["enumeration_order"],
        "structure": structure,
        "tiers": tiers,
        "tier_colors": {t: TIER_COLORS.get(t, "#9b59b6") for t in tiers},
        "batch_config": {k.lower(): v for k, v in batch_cfg.items()},
        "timeline": cap_data["timeline"],
        "log_counts": cap_data["counts"],
        "csv_summary": csv_summary,
        "throughput": throughput,
        "perf": perf,
        "completion_hist": _histogram(csv_summary.get("completions_rel", [])),
    }


HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Scheduler Test Report — __TITLE__</title>
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
  .tree{background:#0b0f14;border:1px solid var(--line);border-radius:8px;padding:14px;font-family:ui-monospace,SFMono-Regular,Consolas,monospace;font-size:12px;line-height:1.6;overflow:auto;white-space:pre}
  .enumexp{display:flex;flex-direction:column;gap:8px;margin-top:6px}
  .enumexp .step{display:flex;align-items:center;gap:10px;background:#0b0f14;border:1px solid var(--line);border-radius:8px;padding:8px 12px;font-size:13px}
  .enumexp .ord{width:22px;height:22px;border-radius:50%;background:#21262d;color:var(--fg);display:inline-flex;align-items:center;justify-content:center;font-weight:700;font-size:12px}
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
  <h1>Scheduler Test Report — <span id="ttl"></span></h1>
  <div class="sub" id="subttl"></div>
</header>
<div class="wrap">

  <section>
    <h2>Summary</h2>
    <div class="grid" id="summaryGrid"></div>
  </section>

  <section>
    <h2>Dataset structure &amp; enumeration expectation</h2>
    <div class="sub" id="structTotals"></div>
    <div class="row">
      <div>
        <div class="muted">Directory chain (BFS L0 → L4)</div>
        <pre id="dirTree" class="tree"></pre>
      </div>
      <div>
        <div class="muted">Enumeration expectation (order the BFS walker drains size-tiers)</div>
        <div id="enumExp" class="enumexp"></div>
      </div>
    </div>
    <table id="structTbl" style="margin-top:14px">
      <thead><tr>
        <th>Ord</th><th>Level</th><th>Tier</th><th>Directory</th>
        <th>Files</th><th>File size</th><th>Total size</th><th>Batches</th><th>Content</th>
      </tr></thead>
      <tbody></tbody>
    </table>
  </section>

  <section>
    <h2>Replay — pending / running / free workers over time</h2>
    <div class="controls">
      <button id="playBtn">▶ Play</button>
      <button id="resetBtn" class="sec">⟲ Reset</button>
      <span class="clock" id="clock">t = 0.0s</span>
      <label class="muted">speed
        <select id="speed">
          <option value="1">1×</option>
          <option value="2" selected>2×</option>
          <option value="4">4×</option>
          <option value="8">8×</option>
          <option value="16">16×</option>
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
      <div><b>MB/s (throughput)</b><br>Data transfer rate of a batch = bytes uploaded ÷ processing time. Higher is faster. Reported in megabytes/second (base-1000, converted from the log's MiB/s).</div>
      <div><b>s/batch (processing time)</b><br>Wall-clock seconds a single batch took to process, taken from the cloudcp.log <code>elapsed=</code> field. Lower is faster; rising values across a tier signal queueing/contention.</div>
      <div><b>batches/s (batch rate)</b><br>How many batches completed per second in a tier = batch count ÷ the tier's wall-clock span. Reflects scheduling/enumeration pace, not raw byte speed.</div>
    </div>
    <div class="chartwrap"><canvas id="thrScatterCv" height="280"></canvas></div>
    <div class="muted" style="text-align:center">Per-batch throughput (MB/s, log scale) vs completion time — colored by tier · hover a point for batch detail</div>
    <div class="row" style="margin-top:14px">
      <div>
        <div class="chartwrap"><canvas id="thrBarsCv" height="240"></canvas></div>
        <div class="muted" style="text-align:center">Avg throughput per tier (MB/s, log) · whisker = min–max · hover a bar</div>
      </div>
      <div>
        <div class="chartwrap"><canvas id="elapBarsCv" height="240"></canvas></div>
        <div class="muted" style="text-align:center">Avg processing time per batch per tier (s) · whisker = min–max · hover a bar</div>
      </div>
    </div>
    <table id="thrTbl" style="margin-top:14px">
      <thead><tr><th>Tier</th><th>Batches</th><th>Files</th><th>Data</th>
      <th>Min MB/s</th><th>Avg MB/s</th><th>Med MB/s</th><th>Max MB/s</th><th>files/s</th><th>Agg MB/s</th><th>batches/s</th>
      <th>s/batch min</th><th>s/batch avg</th><th>s/batch max</th></tr></thead>
      <tbody></tbody>
    </table>
    <div class="muted">MB/s = per-batch rate (min/avg/med/max) · Agg MB/s = tier bytes ÷ tier wall-clock span · batches/s = batches ÷ tier span · s/batch = per-batch processing time from cloudcp.log elapsed</div>
  </section>

  <section>
    <h2>Batch completion — broker PERF (journal)</h2>
    <div class="sub" id="perfOverall"></div>
    <div class="metricdefs">
      <div><b>total s</b><br>Full batch completion time from the broker <code>PERF … total=</code> field. Primary metric; lower is faster.</div>
      <div><b>upload s</b><br>Upload-phase seconds from the same PERF line (<code>upload=</code>).</div>
      <div><b>mode</b><br>Most frequent completion time per tier (total seconds rounded to 0.1s).</div>
    </div>
    <div class="row">
      <div>
        <div class="chartwrap"><canvas id="perfBarsCv" height="240"></canvas></div>
        <div class="muted" style="text-align:center">Avg batch completion per tier (total s) · whisker = min–max · hover a bar</div>
      </div>
      <div>
        <div class="chartwrap"><canvas id="perfHistCv" height="240"></canvas></div>
        <div class="muted" style="text-align:center">Completion-time distribution (total s) · stacked by tier</div>
      </div>
    </div>
    <div class="chartwrap" style="margin-top:14px"><canvas id="perfScatterCv" height="280"></canvas></div>
    <div class="muted" style="text-align:center">Per-batch completion time (total s) vs completion order · colored by tier · red ring = rc≠0 · hover for detail</div>
    <div id="perfGroups" style="margin-top:14px"></div>
    <div class="muted">Click a tier to expand its batches · click a column header to sort · total = full batch time, upload = upload phase, both from the journal PERF line</div>
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

document.getElementById('ttl').textContent = DATA.dataset.id + '  ·  transfer ' + DATA.transfer_id;
document.getElementById('subttl').textContent =
  'profile ' + (DATA.dataset.profile||'-') + '  ·  enumeration: ' + DATA.enumeration_order.join(' → ')
  + '  ·  ' + (DATA.meta.start||'') + '  →  ' + (DATA.meta.end||'');

// ---- summary cards ----
const cs = DATA.csv_summary || {};
const dur = DATA.meta.duration_sec || 0;
const cards = [
  ['Transfer', [['dataset',DATA.dataset.id],['transfer id',DATA.transfer_id],['profile',DATA.dataset.profile||'-'],
                ['scheduler exit',DATA.meta.scheduler_exit],['duration', dur.toFixed(1)+' s']]],
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
// status table
const stb=document.querySelector('#statusTbl tbody');
Object.entries(cs.status_counts||{}).forEach(([s,c])=>{
  const cls = s==='SUCCESS'?'ok':(s?'bad':'');
  stb.innerHTML+='<tr><td class="'+cls+'">'+(s||'(blank)')+'</td><td>'+c+'</td></tr>';
});
// legend
const lg=document.getElementById('legend');
TIERS.forEach(t=>{lg.innerHTML+='<span><i class="dot" style="background:'+(COL[t]||'#888')+'"></i>'+t+'</span>';});

// ---- throughput (per batch / per tier from cloudcp.log) ----
const THR=DATA.throughput||{batches:[],per_tier:{},overall:{},tiers:[]};
{
  const ov=THR.overall||{};
  document.getElementById('thrOverall').textContent = ov.batches
    ? (ov.batches+' batches · avg '+(ov.avg||0).toFixed(2)+' MB/s · peak '+(ov.max||0).toFixed(2)
       +' MB/s · aggregate '+(ov.aggregate_mb_s||0).toFixed(2)+' MB/s · '+(ov.batches_per_sec||0).toFixed(3)+' batches/s · avg '+(ov.elapsed_avg||0).toFixed(2)
       +'s/batch (max '+(ov.elapsed_max||0).toFixed(2)+'s) over '+(ov.wall_sec||0).toFixed(1)+'s')
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

// ---- batch completion (broker PERF from journal) ----
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
    ? (ov.batches+' batches · avg '+(ov.total_avg||0).toFixed(2)+'s total (mode '
       +(ov.total_mode||0).toFixed(1)+'s · max '+(ov.total_max||0).toFixed(2)+'s) · avg '
       +(ov.upload_avg||0).toFixed(2)+'s upload · '+fmt(ov.files||0)+' files · '+dsize(ov.bytes||0)
       +(ov.rc_fail?(' · '+ov.rc_fail+' rc≠0'):' · all rc=0'))
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
      +' — '+s.batches+' batches · avg '+s.total_avg.toFixed(2)+'s · min '+s.total_min.toFixed(2)
      +'s · max '+s.total_max.toFixed(2)+'s · mode '+s.total_mode.toFixed(1)+'s'
      +(s.rc_fail?(' · <span class="bad">'+s.rc_fail+' rc≠0</span>'):'')+'</summary>'
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
        ['files',fmt(s.files)],['rc≠0',s.rc_fail]])});});
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
      html:'<b>'+b.tier+' · '+b.batch+'</b>'+tipRows([
        ['total',b.total.toFixed(2)+' s'],['upload',b.upload.toFixed(2)+' s'],
        ['files',fmt(b.files)],['size',dsize(b.bytes)],['rc',b.rc],['order','#'+(b.order+1)]])});});
  c.fillStyle='#8b949e';c.textAlign='center';c.fillText('completion order →',w/2,h-pad+14);
  c.textAlign='left';c.fillText('total s',pad-40,12);
}
['perfBarsCv','perfScatterCv'].forEach(attachHover);

// ---- dataset structure & enumeration expectation ----
const ST=DATA.structure||{levels:[],enumeration_order:[]};
document.getElementById('structTotals').textContent =
  (ST.levels||[]).length + ' levels · ' + fmt(ST.total_files||0) + ' files · ' + bytes(ST.total_bytes||0) + ' logical';
const dirTree=document.getElementById('dirTree');
let treeStr='';
(ST.levels||[]).forEach(l=>{
  const name=(l.root||'').split('/').pop()||l.root||'';
  const branch = l.level===0 ? '' : '  '.repeat(l.level-1)+'└─ ';
  treeStr += branch + name + '/'
    + '   ['+l.tier+' × '+fmt(l.num_files)+' @ '+bytes(l.file_size_bytes)
    + ' = '+bytes(l.total_bytes)+', '+l.batches+' batches, '+(l.content||'-')+']\n';
});
dirTree.textContent = treeStr || '(no structure metadata)';
const enumExp=document.getElementById('enumExp');
(ST.enumeration_order||[]).forEach((t,i)=>{
  const lv=(ST.levels||[]).find(x=>x.tier===t)||{};
  enumExp.innerHTML += '<div class="step"><span class="ord">'+(i+1)+'</span>'
    + '<i class="dot" style="background:'+(COL[t]||'#888')+'"></i>'
    + '<b>'+t+'</b><span class="muted">'+fmt(lv.num_files||0)+' files · '
    + bytes(lv.file_size_bytes||0)+' each · '+(lv.batches||0)+' batches</span></div>';
});
const stbody=document.querySelector('#structTbl tbody');
(ST.levels||[]).forEach((l,i)=>{
  stbody.innerHTML += '<tr>'
    + '<td>'+(i+1)+'</td><td>L'+l.level+'</td>'
    + '<td><i class="dot" style="background:'+(COL[l.tier]||'#888')+'"></i> '+l.tier+'</td>'
    + '<td class="muted">'+(l.root||'')+'</td>'
    + '<td>'+fmt(l.num_files)+'</td><td>'+bytes(l.file_size_bytes)+'</td>'
    + '<td>'+bytes(l.total_bytes)+'</td><td>'+(l.batches!=null?l.batches:'-')+'</td>'
    + '<td>'+(l.content||'-')+'</td></tr>';
});

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
  c.fillText('running '+totalRun+' · free '+free, pad+4, h/2-22);
}

// precompute maxima
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

// ---- static time-series ----
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
function start(){if(!TL.length)return;playing=true;playBtn.textContent='⏸ Pause';
  const spd=parseInt(document.getElementById('speed').value,10);
  clearInterval(timer);timer=setInterval(tick,Math.max(40,400/spd));}
function stop(){playing=false;playBtn.textContent='▶ Play';clearInterval(timer);}
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


def render_html(payload: dict, out_path: Path) -> None:
    data_json = json.dumps(payload, separators=(",", ":")).replace("</", "<\\/")
    html = (HTML_TEMPLATE
            .replace("__TITLE__", f"{payload['dataset']['id']} · transfer {payload['transfer_id']}")
            .replace("__DATA__", data_json))
    out_path.write_text(html, encoding="utf-8")


# =====================================================================================
# Step 9 — zip
# =====================================================================================
def zip_dir(src_dir: Path, zip_path: Path) -> None:
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for p in sorted(src_dir.rglob("*")):
            if p.is_file():
                zf.write(p, p.relative_to(src_dir.parent))
    LOG.info("wrote %s (%.1f KB)", zip_path, zip_path.stat().st_size / 1024)


# =====================================================================================
# Per-dataset orchestration
# =====================================================================================
def run_one(dataset: dict, args, spec_dir: Path, batch_cfg: dict, tier_order: list[str],
            out_root: Path) -> dict:
    ds_id = dataset["id"]
    LOG.info("=" * 70)
    LOG.info("DATASET %s (#%s, profile %s)", ds_id, dataset["number"], dataset.get("profile"))

    specs = spec_files_in_order(spec_dir, ds_id)

    # 1. datagen
    if args.skip_datagen:
        LOG.info("--skip-datagen: reusing existing data for %s", ds_id)
    else:
        run_datagen(specs, args.datagen, args.datagen_flag, args.dry_run)

    # 3. transfer id + dir
    tr_id = next_transfer_id(args.batchmeta_dir)
    transfer_dir = create_transfer_dir(args.batchmeta_dir, tr_id, args.dry_run)

    report_dir = out_root / f"report_{tr_id}"
    log_dir = report_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    # 1.5 enumeration order json
    enum_payload = write_enumeration_order(dataset, specs, report_dir)

    src = f"{args.data_root.rstrip('/')}/{ds_id}"
    dst = f"{args.s3_base.rstrip('/')}/{ds_id}"
    base_src = src

    # 4. journal capture (starts BEFORE the scheduler; lead settle + post-exit drain)
    start_dt = _dt.datetime.now()
    cap = JournalCapture(args.journal_tag, tr_id, log_dir, start_dt, args.dry_run,
                         lead_sec=args.capture_lead, drain_sec=args.capture_drain)
    # cloudcp.log tail spanning the same window, written alongside the report
    cloud_cap = CloudcpLogCapture(args.cloudcp_log, report_dir / "cloudcplogs.txt", args.dry_run)
    cloud_cap.start()
    cap.start()

    # 5. scheduler (blocks until transfer complete)
    cmd = build_scheduler_cmd(args, tr_id, src, dst, base_src, transfer_dir)
    sched_rc = run_scheduler(cmd, args.dry_run)
    end_dt = _dt.datetime.now()

    # stop capture (drains tail logs first)
    cap.stop()
    cloud_cap.stop()

    year = start_dt.year
    cap_data = parse_capture(cap, year)

    # 6b. throughput from cloudcp.log (per batch, per size-tier)
    tier_sizes = [((lv.get("tier") or "").lower(), int(lv.get("size_bytes", 0) or 0))
                  for lv in dataset.get("levels", [])]
    throughput = parse_cloudcp_log(report_dir / "cloudcplogs.txt", tier_order, tier_sizes)
    # 6c. batch completion from the broker's journal PERF lines
    perf = parse_perf_log(cap.perf_path, tier_order)
    # 6. results csv
    csv_dst = report_dir / f"transfer_report_{tr_id}.csv"
    csv_summary: dict = {"total": 0, "status_counts": {}, "success": 0, "failed": 0,
                         "total_bytes": 0, "completions_rel": [], "completion_span_sec": 0}
    if not args.dry_run:
        csv_src = find_results_csv(args.transfer_logs_dir, tr_id)
        if csv_src:
            shutil.copy2(csv_src, csv_dst)
            csv_summary = parse_results_csv(csv_dst, year)
            LOG.info("results CSV: %s (%d rows, %d success)",
                     csv_src, csv_summary["total"], csv_summary["success"])
        else:
            LOG.warning("results CSV not found for transfer %s under %s",
                        tr_id, args.transfer_logs_dir)

    meta = {
        "dataset_id": ds_id,
        "transfer_id": tr_id,
        "src": src, "dst": dst, "base_src": base_src,
        "transfer_dir": str(transfer_dir),
        "start": start_dt.isoformat(timespec="seconds"),
        "end": end_dt.isoformat(timespec="seconds"),
        "duration_sec": round((end_dt - start_dt).total_seconds(), 1),
        "scheduler_exit": sched_rc,
        "scheduler_cmd": " ".join(cmd),
        "endpoint_url": args.endpoint_url,
    }
    (report_dir / "run_meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")

    # 7. html + summary
    structure = build_structure_payload(dataset)
    payload = build_report_payload(dataset, tr_id, enum_payload, cap_data, csv_summary,
                                   meta, batch_cfg, tier_order, structure, throughput, perf)
    render_html(payload, report_dir / "report.html")
    write_summary_txt(report_dir / "summary.txt", meta, enum_payload, cap_data, csv_summary,
                      structure, throughput, perf)

    # 9. zip
    zip_path = out_root / f"sch_test_{tr_id}.zip"
    if zip_path.exists():
        zip_path.unlink()
    zip_dir(report_dir, zip_path)

    # 10. optional cleanup of the materialised data dir + S3 dst prefix
    if args.delete or args.cleanup:
        cleanup_data_dir(src, args.data_root, args.dry_run)
    if args.clear_bucket or args.cleanup:
        clear_transfer_bucket(dst, args.endpoint_url, args.dry_run)

    return {
        "dataset_id": ds_id, "transfer_id": tr_id, "meta": meta,
        "csv_summary": csv_summary, "report_dir": str(report_dir),
        "zip": str(zip_path), "log_counts": cap_data["counts"],
        "throughput_overall": throughput.get("overall", {}),
    }


def write_summary_txt(path: Path, meta, enum_payload, cap_data, csv_summary, structure,
                      throughput=None, perf=None) -> None:
    def _bytes(n: int) -> str:
        x = float(n)
        for u in ("B", "KB", "MB", "GB", "TB"):
            if x < 1024 or u == "TB":
                return f"{x:.2f} {u}" if x < 10 else f"{x:.0f} {u}"
            x /= 1024
        return f"{n} B"

    lines = [
        "Scheduler Test Summary",
        "=" * 60,
        f"dataset          : {meta['dataset_id']}",
        f"transfer id      : {meta['transfer_id']}",
        f"enumeration order: {' -> '.join(enum_payload['enumeration_order'])}",
        f"src              : {meta['src']}",
        f"dst              : {meta['dst']}",
        f"transfer-dir     : {meta['transfer_dir']}",
        f"start            : {meta['start']}",
        f"end              : {meta['end']}",
        f"duration (s)     : {meta['duration_sec']}",
        f"scheduler exit   : {meta['scheduler_exit']}",
        "",
        "Dataset structure (BFS chain L0->L4):",
        f"  total files    : {structure['total_files']}",
        f"  total bytes    : {_bytes(structure['total_bytes'])}",
    ]
    for i, lv in enumerate(structure["levels"], 1):
        lines.append(
            f"  {i}. L{lv['level']} {lv['tier']:<6} "
            f"{lv['num_files']:>8} files x {_bytes(lv['file_size_bytes']):>10} "
            f"= {_bytes(lv['total_bytes']):>10}  ({lv['batches']} batches)  {lv['root']}"
        )
    lines += [
        "",
        "Results (CSV):",
        f"  total files    : {csv_summary['total']}",
        f"  success        : {csv_summary['success']}",
        f"  failed         : {csv_summary['failed']}",
        f"  total bytes    : {csv_summary['total_bytes']}",
        f"  status counts  : {csv_summary['status_counts']}",
        "",
        "Log capture:",
        f"  pending events : {cap_data['counts']['pending_events']}",
        f"  running events : {cap_data['counts']['running_events']}",
        f"  free events    : {cap_data['counts']['free_events']}",
        f"  timeline frames: {len(cap_data['timeline'])}",
    ]
    tp = throughput or {}
    if tp.get("per_tier"):
        lines += [
            "",
            "Metric definitions:",
            "  MB/s      = per-batch transfer rate (bytes uploaded / processing time)",
            "  agg MB/s  = tier bytes / tier wall-clock span",
            "  batch/s   = batches completed per second (batches / tier span)",
            "  s/batch   = per-batch processing time (cloudcp.log elapsed=)",
            "",
            "Throughput (MB/s per batch, grouped by tier):",
        ]
        for tier in tp.get("tiers", []):
            s = tp["per_tier"][tier]
            lines.append(
                f"  {tier:<6} batches={s['batches']:>3}  "
                f"min={s['min']:>7.2f}  avg={s['avg']:>7.2f}  med={s['median']:>7.2f}  "
                f"max={s['max']:>7.2f}  files/s={s['avg_files_sec']:>7.1f}  "
                f"agg={s['aggregate_mb_s']:>7.2f}  batch/s={s.get('batches_per_sec', 0):>6.3f}"
            )
        ov = tp.get("overall", {})
        if ov:
            lines.append(
                f"  {'ALL':<6} batches={ov['batches']:>3}  "
                f"min={ov['min']:>7.2f}  avg={ov['avg']:>7.2f}  med={ov['median']:>7.2f}  "
                f"max={ov['max']:>7.2f}  {'':>16}  agg={ov['aggregate_mb_s']:>7.2f}  "
                f"batch/s={ov.get('batches_per_sec', 0):>6.3f}"
            )
        lines += ["", "Batch processing time (seconds per batch, grouped by tier):"]
        for tier in tp.get("tiers", []):
            s = tp["per_tier"][tier]
            lines.append(
                f"  {tier:<6} batches={s['batches']:>3}  "
                f"min={s['elapsed_min']:>7.2f}s  avg={s['elapsed_avg']:>7.2f}s  "
                f"med={s['elapsed_median']:>7.2f}s  max={s['elapsed_max']:>7.2f}s"
            )
        if ov:
            lines.append(
                f"  {'ALL':<6} batches={ov['batches']:>3}  "
                f"min={ov['elapsed_min']:>7.2f}s  avg={ov['elapsed_avg']:>7.2f}s  "
                f"med={ov['elapsed_median']:>7.2f}s  max={ov['elapsed_max']:>7.2f}s"
            )
    pf = perf or {}
    if pf.get("per_tier"):
        lines += [
            "",
            "Batch completion — broker PERF (journal), total time per batch:",
            "  (total = full batch time; upload = upload phase; mode on 0.1s-rounded totals)",
        ]
        for tier in pf.get("tiers", []):
            s = pf["per_tier"][tier]
            rc = f"  rc!=0={s['rc_fail']}" if s["rc_fail"] else ""
            lines.append(
                f"  {tier:<6} batches={s['batches']:>3}  files={s['files']:>7}  "
                f"min={s['total_min']:>7.2f}s  avg={s['total_avg']:>7.2f}s  "
                f"max={s['total_max']:>7.2f}s  mode={s['total_mode']:>6.1f}s  "
                f"upl_avg={s['upload_avg']:>7.2f}s{rc}"
            )
        ov = pf.get("overall", {})
        if ov:
            rc = f"  rc!=0={ov['rc_fail']}" if ov["rc_fail"] else ""
            lines.append(
                f"  {'ALL':<6} batches={ov['batches']:>3}  files={ov['files']:>7}  "
                f"min={ov['total_min']:>7.2f}s  avg={ov['total_avg']:>7.2f}s  "
                f"max={ov['total_max']:>7.2f}s  mode={ov['total_mode']:>6.1f}s  "
                f"upl_avg={ov['upload_avg']:>7.2f}s{rc}"
            )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


# =====================================================================================
# Combined report
# =====================================================================================
def build_combined(results: list[dict], out_root: Path, dry_run: bool) -> None:
    ts = _dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    comb_dir = out_root / f"combined_{ts}"
    comb_dir.mkdir(parents=True, exist_ok=True)

    rows = ""
    for r in results:
        cs = r["csv_summary"]
        ov = r.get("throughput_overall", {})
        rows += (
            "<tr>"
            f"<td>{r['dataset_id']}</td>"
            f"<td>{r['transfer_id']}</td>"
            f"<td>{r['meta']['duration_sec']}</td>"
            f"<td>{cs['total']}</td>"
            f"<td class='ok'>{cs['success']}</td>"
            f"<td class='bad'>{cs['failed']}</td>"
            f"<td>{ov.get('avg', 0):.1f}</td>"
            f"<td>{ov.get('max', 0):.1f}</td>"
            f"<td>{r['meta']['scheduler_exit']}</td>"
            f"<td><a href='report_{r['transfer_id']}/report.html'>open</a></td>"
            "</tr>"
        )
    agg = {
        "datasets": len(results),
        "total_files": sum(r["csv_summary"]["total"] for r in results),
        "total_success": sum(r["csv_summary"]["success"] for r in results),
        "total_failed": sum(r["csv_summary"]["failed"] for r in results),
        "results": [{"dataset_id": r["dataset_id"], "transfer_id": r["transfer_id"],
                     "duration_sec": r["meta"]["duration_sec"],
                     "csv_summary": r["csv_summary"]} for r in results],
    }
    (comb_dir / "aggregate.json").write_text(json.dumps(agg, indent=2), encoding="utf-8")

    html = COMBINED_TEMPLATE.replace("__ROWS__", rows).replace("__TS__", ts).replace(
        "__AGG__", (f"{agg['datasets']} datasets · {agg['total_files']} files · "
                    f"{agg['total_success']} ok · {agg['total_failed']} failed"))
    (comb_dir / "index.html").write_text(html, encoding="utf-8")

    # copy per-dataset report dirs into combined for a self-contained bundle
    for r in results:
        src = Path(r["report_dir"])
        if src.is_dir():
            shutil.copytree(src, comb_dir / src.name, dirs_exist_ok=True)

    zip_path = out_root / f"sch_test_combined_{ts}.zip"
    zip_dir(comb_dir, zip_path)
    LOG.info("combined report: %s", zip_path)


COMBINED_TEMPLATE = r"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Scheduler Test — Combined __TS__</title>
<style>
body{margin:0;font-family:-apple-system,Segoe UI,Roboto,Arial,sans-serif;background:#0e1116;color:#e6edf3}
header{padding:18px 24px;border-bottom:1px solid #30363d;background:#161b22}
h1{margin:0;font-size:20px}.wrap{max-width:1000px;margin:0 auto;padding:24px}
.sub{color:#8b949e;margin-top:6px}
table{width:100%;border-collapse:collapse;font-size:14px;background:#161b22;border:1px solid #30363d;border-radius:8px;overflow:hidden}
th,td{padding:9px 12px;border-bottom:1px solid #30363d;text-align:left}
th{color:#8b949e}a{color:#58a6ff}.ok{color:#3fb950}.bad{color:#f85149}
</style></head><body>
<header><h1>Scheduler Test — Combined Report</h1><div class="sub">__AGG__ · __TS__</div></header>
<div class="wrap">
<table><thead><tr><th>Dataset</th><th>Transfer</th><th>Duration (s)</th><th>Files</th>
<th>Success</th><th>Failed</th><th>Avg MB/s</th><th>Peak MB/s</th><th>Exit</th><th>Report</th></tr></thead>
<tbody>__ROWS__</tbody></table>
</div></body></html>
"""


# =====================================================================================
# CLI
# =====================================================================================
def parse_args(argv: list[str]) -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        prog="schedular_test.py",
        description="End-to-end scheduler/broker test harness for the SCH-* datasets.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("dataset", nargs="?", help="dataset number (1..15) or id (SCH-ORD-07 / DEEP-02)")
    ap.add_argument("--from", dest="from_", type=int, help="range start (inclusive)")
    ap.add_argument("--to", dest="to", type=int, help="range end (inclusive)")
    ap.add_argument("--all", action="store_true", help="run every dataset (each + combined)")

    ap.add_argument("--spec-dir", default=None, help="spec_files dir (default: <script>/spec_files)")
    ap.add_argument("--data-root", default=DEF_DATA_ROOT)
    ap.add_argument("--s3-base", default=DEF_S3_BASE)
    ap.add_argument("--datagen", default=DEF_DATAGEN)
    ap.add_argument("--datagen-flag", default=DEF_DATAGEN_FLAG)
    ap.add_argument("--batchmeta-dir", default=DEF_BATCHMETA)
    ap.add_argument("--transfer-logs-dir", default=DEF_TRANSFER_LOGS)
    ap.add_argument("--cloudcp-log", dest="cloudcp_log", default=DEF_CLOUDCP_LOG,
                    help="cloudcp.log to tail into cloudcplogs.txt for the run window")
    ap.add_argument("--scheduler-python", default=DEF_SCHED_PY)
    ap.add_argument("--scheduler-script", default=DEF_SCHED_SCRIPT)
    ap.add_argument("--dir-path", default=DEF_DIR_PATH)
    ap.add_argument("--endpoint-url", dest="endpoint_url", default=None,
                    help=f"S3/MinIO endpoint (default: {DEF_ENDPOINT})")
    ap.add_argument("--config", default=DEF_CONFIG, help="config.json for BATCH tiers")
    ap.add_argument("--skip-config-check", dest="skip_config_check", action="store_true",
                    help="skip the preflight datagen/config.json validation")
    ap.add_argument("--journal-tag", default=DEF_JOURNAL_TAG)
    ap.add_argument("--capture-lead", type=float, default=DEF_CAPTURE_LEAD,
                    help="seconds to settle the journalctl follower before the scheduler starts")
    ap.add_argument("--capture-drain", type=float, default=DEF_CAPTURE_DRAIN,
                    help="seconds to keep capturing after the scheduler exits")
    ap.add_argument("--out-dir", default=None, help="output root (default: <script>/sch_test_runs)")
    ap.add_argument("--poll-interval", type=int, default=None)
    ap.add_argument("--skip-datagen", action="store_true")
    ap.add_argument("--delete", action="store_true",
                    help="after the run, delete the materialised data dir (<data-root>/<id>)")
    ap.add_argument("--clear-bucket", dest="clear_bucket", action="store_true",
                    help="after the run, clear the uploaded S3 objects under <s3-base>/<id>")
    ap.add_argument("--cleanup", action="store_true",
                    help="after the run, do both --delete and --clear-bucket")
    ap.add_argument("--negative", action="store_true",
                    help="run the scheduler-level negative test suite (all cases)")
    ap.add_argument("--negative-case", default=None,
                    help="run specific negative case id(s), comma-separated (e.g. NEG-ENUM-03)")
    ap.add_argument("--negative-list", action="store_true",
                    help="list the negative test cases and exit")
    ap.add_argument("--neg-timeout", type=int, default=None,
                    help="per-negative-case wall-clock bound in seconds")
    ap.add_argument("--pause-resume", dest="pause_resume", action="store_true",
                    help="run the pause/resume (kill & restart) suite — plan Group E.1")
    ap.add_argument("--pause-resume-case", dest="pause_resume_case", default=None,
                    help="run specific pause/resume case id(s), comma-separated (e.g. SCH-PR-04)")
    ap.add_argument("--pause-resume-list", dest="pause_resume_list", action="store_true",
                    help="list the pause/resume cases and exit")
    ap.add_argument("--pr-kill-after", dest="pr_kill_after", type=float, default=None,
                    help="override the mid-run kill delay (seconds) for ALL pause/resume cases")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("-v", "--verbose", action="store_true")
    return ap.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    # resolve the endpoint sentinel and remember whether the user overrode it
    args.endpoint_overridden = args.endpoint_url is not None
    if args.endpoint_url is None:
        args.endpoint_url = DEF_ENDPOINT
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )

    # Negative test suite is a separate harness; delegate before the positive flow.
    if args.negative_list or args.negative or args.negative_case:
        import schedular_negative_test as neg
        if args.negative_list:
            for c in neg.CASES:
                tag = " (POSIX-only)" if c.posix_only else ""
                print(f"{c.id:<13} {c.group:<12} {c.title}{tag}")
            return 0
        return neg.run_from_args(args)

    here = Path(__file__).resolve().parent
    spec_dir = Path(args.spec_dir) if args.spec_dir else here / "spec_files"
    out_root = Path(args.out_dir) if args.out_dir else here / "sch_test_runs"

    # Pause/resume (kill & restart) suite — plan Group E.1.
    if args.pause_resume_list:
        for c in PAUSE_RESUME_CASES:
            print(f"{c['id']:<10} {c['dataset']:<13} mode={c['mode']:<12} "
                  f"kill_after={c.get('kill_after')} cycles={c.get('cycles', 1)}  {c['title']}")
        return 0
    if args.pause_resume or args.pause_resume_case:
        preflight_checks(args)
        out_root.mkdir(parents=True, exist_ok=True)
        return run_pause_resume_suite(args, spec_dir, out_root)

    # host/config preflight before any transfer runs
    preflight_checks(args)

    out_root.mkdir(parents=True, exist_ok=True)
    manifest = load_manifest(spec_dir)
    by_num = manifest_index(manifest)
    datasets = select_datasets(args, by_num)

    batch_cfg = load_batch_config(args.config)
    tier_order = tier_order_from_config(batch_cfg)

    LOG.info("selected %d dataset(s): %s", len(datasets), ", ".join(d["id"] for d in datasets))
    if os.name != "posix" and not args.dry_run:
        LOG.warning("this harness targets the Linux bryck host; on this OS use --dry-run to preview")

    results = []
    for ds in datasets:
        try:
            results.append(run_one(ds, args, spec_dir, batch_cfg, tier_order, out_root))
        except SystemExit:
            raise
        except Exception as exc:  # noqa: BLE001
            LOG.exception("dataset %s failed: %s", ds["id"], exc)

    if len(results) > 1:
        build_combined(results, out_root, args.dry_run)

    LOG.info("=" * 70)
    LOG.info("DONE. %d dataset(s) processed. Output under: %s", len(results), out_root)
    for r in results:
        cs = r["csv_summary"]
        LOG.info("  %-13s transfer %-4s  exit=%s  files=%s ok=%s fail=%s  -> %s",
                 r["dataset_id"], r["transfer_id"], r["meta"]["scheduler_exit"],
                 cs["total"], cs["success"], cs["failed"], Path(r["zip"]).name)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
