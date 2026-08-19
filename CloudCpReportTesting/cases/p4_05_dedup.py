"""P4-05: De-dup - last status wins."""
from report_engine import verify, write_final_report, read_final_report, src, row

CASE_ID = "P4-05"
DESCRIPTION = "De-dup - last status wins"
STEPS = [
    "Build 50 source files",
    "For each, append two report rows in order: SKIPPED first, then FALLBACK_OK second "
    "(same size both times)",
    "Run verify() - last-status-wins means the second row overwrites the first",
    "Write and re-read final_report.csv",
    "Confirm exactly 50 rows exist (not 100 - no duplicate row per file)",
    "Confirm every one of the 50 is classified OK (resolved via the later FALLBACK_OK row)",
]


def run(out_dir):
    srcs, rows = [], []
    for i in range(50):
        rp = f"dedup/file_{i}.bin"
        srcs.append(src(rp, 2048))
        rows.append(row(rp, "SKIPPED", 2048))       # stale row, written first
        rows.append(row(rp, "FALLBACK_OK", 2048))    # authoritative row, written second
    results = verify(srcs, rows)
    path = write_final_report(results, out_dir)
    parsed = read_final_report(path)
    exactly_once = len(parsed) == 50
    all_ok = all(r["Status"] == "OK" for r in parsed)
    passed = exactly_once and all_ok
    return passed, {"files": 50, "appears_exactly_once": exactly_once,
                     "resolved_to_ok": all_ok, "final_report": str(path)}


def check_live(source_entries, report_rows, results, out_dir):
    """Generic version: confirm de-dup (last-status-wins) held for the real
    report - every source path resolves to exactly one row, regardless of how
    many report shard rows referenced it."""
    path = write_final_report(results, out_dir)
    parsed = read_final_report(path)
    source_paths = [e["relpath"] for e in source_entries]
    counts = {}
    for r in parsed:
        counts[r["AbsoluteFilePath"]] = counts.get(r["AbsoluteFilePath"], 0) + 1
    exactly_once = all(counts.get(p, 0) == 1 for p in source_paths)
    passed = exactly_once
    return passed, {"source_files": len(source_paths),
                     "each_appears_exactly_once": exactly_once, "final_report": str(path)}
