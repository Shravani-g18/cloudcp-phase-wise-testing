"""P4-01: Exactly one correct status per file."""
from report_engine import verify, write_final_report, read_final_report, src, row

CASE_ID = "P4-01"
DESCRIPTION = "Exactly one correct status per file"
STEPS = [
    "Build 100 source files with matching SUCCESS report rows (same size) -> expect OK",
    "Build 20 source files with no report row at all -> expect MISSING",
    "Build 20 source files with a FAILED report row -> expect FAILED",
    "Build 20 source files with a SUCCESS row but a different size -> expect MISMATCH",
    "Add 10 report rows with no matching source file -> expect EXTRA",
    "Add one OK file whose name embeds a comma, a quote, and a newline (CSV-quoting stress)",
    "Run verify() on the combined 171-file set, write final_report.csv, re-read it back",
    "Count rows per status in the re-parsed file and compare to expected counts",
    "Confirm the tricky-named file re-parses to exactly one OK row",
    "Confirm no file path appears more than once in the final parsed report",
]


def run(out_dir):
    srcs, rows, expected = [], [], {"OK": 0, "MISSING": 0, "FAILED": 0, "MISMATCH": 0, "EXTRA": 0}
    for i in range(100):
        rp = f"ok/file_{i}.bin"
        srcs.append(src(rp, 1024)); rows.append(row(rp, "SUCCESS", 1024)); expected["OK"] += 1
    for i in range(20):
        rp = f"missing/file_{i}.bin"
        srcs.append(src(rp, 1024)); expected["MISSING"] += 1  # no report row
    for i in range(20):
        rp = f"failed/file_{i}.bin"
        srcs.append(src(rp, 1024)); rows.append(row(rp, "FAILED", 1024)); expected["FAILED"] += 1
    for i in range(20):
        rp = f"mismatch/file_{i}.bin"
        srcs.append(src(rp, 1024)); rows.append(row(rp, "SUCCESS", 2048)); expected["MISMATCH"] += 1
    for i in range(10):
        rp = f"extra/file_{i}.bin"
        rows.append(row(rp, "SUCCESS", 512)); expected["EXTRA"] += 1
    # CSV-quoting stress: embedded comma + newline + trailing space in the name
    tricky = 'weird/name, with "quote", and\nnewline .txt'
    srcs.append(src(tricky, 1024)); rows.append(row(tricky, "SUCCESS", 1024)); expected["OK"] += 1

    results = verify(srcs, rows)
    path = write_final_report(results, out_dir)
    parsed = read_final_report(path)

    counts = {s: 0 for s in expected}
    for r in parsed:
        counts[r["Status"]] += 1
    tricky_ok = any(r["AbsoluteFilePath"] == tricky and r["Status"] == "OK" for r in parsed)
    no_dupes = len({r["AbsoluteFilePath"] for r in parsed}) == len(parsed)

    passed = counts == expected and tricky_ok and no_dupes
    return passed, {"expected": expected, "actual": counts,
                     "csv_quoting_preserved": tricky_ok, "no_duplicates": no_dupes,
                     "final_report": str(path)}


def check_live(source_entries, report_rows, results, out_dir):
    """Generic (data-agnostic) version of the same status-correctness check,
    run against a real transfer's reconciliation results instead of a fixture."""
    path = write_final_report(results, out_dir)
    parsed = read_final_report(path)
    valid_statuses = {"OK", "MISSING", "FAILED", "MISMATCH", "EXTRA"}
    all_valid = all(r["Status"] in valid_statuses for r in parsed)
    no_dupes = len({r["AbsoluteFilePath"] for r in parsed}) == len(parsed)
    source_paths = {e["relpath"] for e in source_entries}
    all_source_covered = source_paths <= {r["AbsoluteFilePath"] for r in parsed}
    passed = all_valid and no_dupes and all_source_covered
    return passed, {"rows": len(parsed), "all_statuses_valid": all_valid,
                     "no_duplicate_paths": no_dupes,
                     "every_source_file_covered": all_source_covered,
                     "final_report": str(path)}
