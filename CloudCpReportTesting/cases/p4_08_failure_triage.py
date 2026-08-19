"""P4-08: Failure-summary triage rows."""
from report_engine import verify, write_final_report, src, row

CASE_ID = "P4-08"
DESCRIPTION = "Failure-summary triage rows"
STEPS = [
    "Build 15 source files, each given a FAILED report row with a realistic error message "
    "('ENOSPC: no space left on device') and retry_count=3",
    "Run verify(), write final_report.csv",
    "Filter results down to Status == FAILED rows",
    "Confirm exactly 15 such rows exist (one per injected failure, no drops/duplicates)",
    "Confirm every one of those rows carries a non-empty last_error and populated retry_count",
]


def run(out_dir):
    srcs, rows = [], []
    for i in range(15):
        rp = f"perm_failed/file_{i}.bin"
        srcs.append(src(rp, 512, tier="tiny"))
        rows.append(row(rp, "FAILED", 512, last_error="ENOSPC: no space left on device",
                         retry_count=3))
    results = verify(srcs, rows)
    path = write_final_report(results, out_dir)

    failed = [r for r in results if r["Status"] == "FAILED"]
    one_per_file = len(failed) == 15
    has_context = all(r["last_error"] and r["retry_count"] != "" for r in failed)
    passed = one_per_file and has_context
    return passed, {"permanently_failed": 15, "triage_rows": len(failed),
                     "one_row_per_file": one_per_file,
                     "all_rows_have_context": has_context, "final_report": str(path)}


def check_live(source_entries, report_rows, results, out_dir):
    """Generic version: confirm every FAILED row in the real results keeps its
    triage context (last_error) whenever the underlying report row had one."""
    path = write_final_report(results, out_dir)
    failed = [r for r in results if r["Status"] == "FAILED"]
    report_map = {r["relpath"]: r for r in report_rows}
    context_ok = True
    for r in failed:
        src_row = report_map.get(r["AbsoluteFilePath"])
        if src_row and src_row.get("last_error") and not r["last_error"]:
            context_ok = False
    passed = context_ok
    return passed, {"failed_rows": len(failed), "context_preserved": context_ok,
                     "final_report": str(path)}
