"""Shared verification engine + report I/O used by every case plugin under cases/.

Reference merge-join algorithm per ../docs/bcloud_redesign_proposal.md §5 and
../docs/bcloud_final_design.md §16. Case plugins import from this module; it
has no knowledge of any specific case, so it never needs to change when a new
case is added.
"""
import csv
import json
import re
import zipfile
from pathlib import Path

REPORT_FIELDS = ["AbsoluteFilePath", "S3Path", "FileSize", "ETag", "Status"]


class VerificationRefused(Exception):
    """Raised when the engine's ordering/lifecycle guard rejects the run."""


def verify(source_entries, report_rows, scan_state="complete", pause_requested=False):
    if scan_state != "complete":
        raise VerificationRefused(f"scan_state={scan_state}, cannot verify")
    if pause_requested:
        raise VerificationRefused("pause_requested=True, cannot verify")

    # last-status-wins: later rows in the list overwrite earlier ones
    report_map = {}
    for row in report_rows:
        report_map[row["relpath"]] = row

    results = []
    for e in source_entries:
        rp, size, tier = e["relpath"], e["size"], e.get("tier", "unknown")
        row = report_map.get(rp)
        if row is None:
            status = "MISSING"
        elif row["status"] == "FAILED":
            status = "FAILED"
        elif int(row["size"]) != int(size):
            status = "MISMATCH"
        elif row["status"] in ("SUCCESS", "SKIPPED", "FALLBACK_OK"):
            status = "OK"
        else:
            status = "FAILED"
        results.append({
            "AbsoluteFilePath": rp, "S3Path": row.get("s3path", "") if row else "",
            "FileSize": size, "ETag": row.get("etag", "") if row else "",
            "Status": status, "tier": tier,
            "last_error": row.get("last_error", "") if row else "",
            "retry_count": row.get("retry_count", "") if row else "",
        })

    source_paths = {e["relpath"] for e in source_entries}
    for rp, row in report_map.items():
        if rp not in source_paths:
            results.append({
                "AbsoluteFilePath": rp, "S3Path": row.get("s3path", ""),
                "FileSize": row.get("size", 0), "ETag": row.get("etag", ""),
                "Status": "EXTRA", "tier": "n/a", "last_error": "", "retry_count": "",
            })
    return results


def write_final_report(results, out_dir):
    """Write final_report.csv grouped by status (easiest severity to scan first)
    and sorted by path within each group, with minimal quoting so plain rows
    stay readable and only fields that need quoting (commas/newlines) get it."""
    status_order = {"MISMATCH": 0, "FAILED": 1, "MISSING": 2, "EXTRA": 3, "OK": 4}
    ordered = sorted(results, key=lambda r: (status_order.get(r["Status"], 9),
                                              r["AbsoluteFilePath"]))
    path = Path(out_dir) / "final_report.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=REPORT_FIELDS, extrasaction="ignore",
                            quoting=csv.QUOTE_MINIMAL)
        w.writeheader()
        for r in ordered:
            w.writerow(r)

    summary_path = Path(out_dir) / "final_report_summary.txt"
    counts = {}
    for r in ordered:
        counts[r["Status"]] = counts.get(r["Status"], 0) + 1
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write(f"final_report.csv summary ({len(ordered)} rows)\n")
        f.write("-" * 40 + "\n")
        for status in ("OK", "MISMATCH", "FAILED", "MISSING", "EXTRA"):
            f.write(f"  {status:<10} {counts.get(status, 0)}\n")
    return path


def read_final_report(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def src(relpath, size, tier="tiny"):
    """Build one source.index entry."""
    return {"relpath": relpath, "size": size, "tier": tier}


def row(relpath, status, size, etag="etag0", **extra):
    """Build one upload-report/txhistory row."""
    d = {"relpath": relpath, "status": status, "size": size, "etag": etag,
         "s3path": f"s3://bucket/{relpath}"}
    d.update(extra)
    return d


# ---------------------------------------------------------------------------
# Live-mode input adapters — read-only, never write into a transfer's own dirs.
# ---------------------------------------------------------------------------
def load_source_index(path):
    """Read a real source.index CSV into verify()'s source_entries format.

    Accepts either the phase's documented header (AbsoluteFilePath,FileSize[,Tier])
    or a plain (relpath,size[,tier]) header - whichever the real tool emits.
    Read-only: opens for reading only, never modifies the file.
    """
    entries = []
    with open(path, newline="", encoding="utf-8-sig", errors="surrogateescape") as f:
        reader = csv.DictReader(f)
        for r in reader:
            relpath = r.get("AbsoluteFilePath") or r.get("relpath") or r.get("path")
            size = r.get("FileSize") or r.get("size")
            tier = r.get("Tier") or r.get("tier") or "unknown"
            if relpath is None or size is None:
                raise ValueError(f"{path}: row missing path/size columns: {r}")
            entries.append(src(relpath, int(size), tier=tier))
    return entries


def load_upload_report_rows(paths):
    """Read one or more real upload-report/txhistory CSV shards (cloudcp +
    fallback) into verify()'s report_rows format. Rows are read in the order
    the files/paths are given, so callers should pass shards oldest-first -
    verify() applies last-status-wins on top of that ordering.
    Read-only: opens for reading only, never modifies any shard file.
    """
    rows = []
    for shard in paths:
        with open(shard, newline="", encoding="utf-8-sig", errors="surrogateescape") as f:
            reader = csv.DictReader(f)
            for r in reader:
                relpath = r.get("local_path") or r.get("relpath") or r.get("AbsoluteFilePath")
                status = r.get("status") or r.get("Status")
                size = r.get("size") or r.get("FileSize") or 0
                etag = r.get("etag") or r.get("ETag") or ""
                if relpath is None or status is None:
                    raise ValueError(f"{shard}: row missing path/status columns: {r}")
                rows.append(row(relpath, status, int(size), etag=etag,
                                 last_error=r.get("last_error", ""),
                                 retry_count=r.get("retry_count", "")))
    return rows


def read_transfer_state(manifest_path):
    """Read scan_state / pause_requested from a real transfer's manifest.json,
    for the same P4-02/P4-03 ordering guards used against synthetic fixtures.
    Read-only. Missing keys default to values that allow verification to proceed.
    """
    with open(manifest_path, encoding="utf-8") as f:
        data = json.load(f)
    return data.get("scan_state", "complete"), bool(data.get("pause_requested", False))


# ---------------------------------------------------------------------------
# Live-mode report ZIP parsing (RT-* live cases, see cloud_cp_report_test_case_plan.md)
# ---------------------------------------------------------------------------
def unzip_report(zip_path, out_dir):
    """Extract a downloaded cloud_transfer_<id>.zip into out_dir. Read-only on
    the zip itself; returns the extraction directory."""
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(out_dir)
    return out_dir


def find_report_files(extracted_dir):
    """Locate the known report artifacts inside an extracted report dir
    (searched recursively since the zip may nest one level under
    cloud_transfer_<id>/). Missing files are simply absent from the result."""
    extracted_dir = Path(extracted_dir)
    files = {}
    for p in sorted(extracted_dir.rglob("*")):
        if not p.is_file():
            continue
        name = p.name
        if name.startswith("transfer_report_") and name.endswith(".csv"):
            files.setdefault("report_csv", p)
        elif name == "transfer_summary.txt":
            files.setdefault("summary_txt", p)
        elif name == "final_report.json":
            files.setdefault("final_report_json", p)
        elif name.startswith("cloud_transfer_txhistory_"):
            files.setdefault("txhistory_log", p)
        elif name.startswith("cloud_transfer_") and name.endswith(".log"):
            files.setdefault("debug_log", p)
    return files


def parse_transfer_report_csv(path):
    """Parse a real transfer_report_<id>.csv into a list of row dicts
    (local_path, s3path, size, etag, status, attempt, finished_at)."""
    with open(path, newline="", encoding="utf-8-sig", errors="surrogateescape") as f:
        return list(csv.DictReader(f))


_SUMMARY_LINE_RE = re.compile(r"^-?\s*([A-Za-z][A-Za-z0-9 /()\.]*?)\s*:\s*(.+?)\s*$")


def parse_transfer_summary_txt(path):
    """Parse transfer_summary.txt into a flat dict of label -> raw string value
    (e.g. {'Transfer status': 'Completed', 'Number of files': '120', ...})."""
    result = {}
    with open(path, encoding="utf-8", errors="surrogateescape") as f:
        for line in f:
            m = _SUMMARY_LINE_RE.match(line.rstrip("\n"))
            if m:
                result[m.group(1).strip()] = m.group(2).strip()
    return result


def summary_int(summary, *labels):
    """Pull the first integer found among candidate summary labels - handles
    label-naming variance across builds (e.g. 'Number of files' vs 'Total files')."""
    for label in labels:
        raw = summary.get(label)
        if raw is None:
            continue
        m = re.search(r"[\d,]+", raw)
        if m:
            return int(m.group(0).replace(",", ""))
    return None


def parse_final_report_json(path):
    """Parse the optional final_report.json S3 snapshot
    (AbsoluteFilePath, S3Path, FileSize, ETag rows)."""
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def cross_cutting_checks(report_rows, summary):
    """The checks in plan §8, applied identically to every live RT-* case
    regardless of its own case-specific assertions."""
    local_paths = [r.get("local_path", "") for r in report_rows]
    s3_paths = [r.get("s3path", "") for r in report_rows]
    sizes = [r.get("size", "") for r in report_rows]

    def _is_valid_size(v):
        try:
            return int(v) >= 0
        except (TypeError, ValueError):
            return False

    no_dup_paths = len(set(local_paths)) == len(local_paths) if local_paths else True
    no_null_local = all(bool(p) for p in local_paths)
    no_null_s3 = all(bool(p) for p in s3_paths)
    no_null_size = all(_is_valid_size(s) for s in sizes)
    transfer_completed = summary.get("Transfer status", "").lower() == "completed"
    number_of_files = summary_int(summary, "Number of files", "Total files")
    summary_count_matches = (number_of_files is not None
                              and number_of_files == len(report_rows))
    missing = summary_int(summary, "Number of missing files/objects")
    summary_missing_zero = missing == 0

    return {
        "no_duplicate_paths": no_dup_paths,
        "no_null_local_path": no_null_local,
        "no_null_s3path": no_null_s3,
        "no_null_size": no_null_size,
        "transfer_status_completed": transfer_completed,
        "summary_count_matches_report": summary_count_matches,
        "summary_missing_zero": summary_missing_zero,
    }

