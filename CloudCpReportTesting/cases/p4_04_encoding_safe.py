"""P4-04: Encoding-safe merge-join."""
from report_engine import verify, write_final_report, read_final_report, src, row

CASE_ID = "P4-04"
DESCRIPTION = "Encoding-safe merge-join"
STEPS = [
    "Build 6 source files with tricky names: plain ASCII, mixed unicode/emoji, multiple "
    "internal spaces, many dots (.tar.gz-style), bracket/paren characters, one long segment",
    "Give each an exactly-matching SUCCESS report row (same path, same size)",
    "Run verify(), write and re-read final_report.csv",
    "Confirm all 6 rows are present and every one classified OK",
]


def run(out_dir):
    variants = [
        "plain/ascii_name.txt",
        "unicode/файл_名前_🎯.bin",
        "spaces/name with   spaces.dat",
        "dots/name.with.many.dots.tar.gz",
        "special/name(1)[2]{3}.log",
        "long/" + ("segment_" * 20) + ".bin",
    ]
    srcs = [src(v, 4096, tier="small") for v in variants]
    rows = [row(v, "SUCCESS", 4096) for v in variants]
    results = verify(srcs, rows)
    path = write_final_report(results, out_dir)
    parsed = read_final_report(path)
    all_ok = all(r["Status"] == "OK" for r in parsed)
    count_ok = len(parsed) == len(variants)
    passed = all_ok and count_ok
    return passed, {"variants_tested": len(variants), "all_classified_ok": all_ok,
                     "final_report": str(path)}


def check_live(source_entries, report_rows, results, out_dir):
    """Generic version: confirm the real transfer's file paths (whatever
    characters they contain) survive the CSV write/read round-trip intact."""
    path = write_final_report(results, out_dir)
    parsed = read_final_report(path)
    written_paths = {r["AbsoluteFilePath"] for r in results}
    parsed_paths = {r["AbsoluteFilePath"] for r in parsed}
    round_trip_intact = written_paths == parsed_paths
    passed = round_trip_intact
    return passed, {"paths_checked": len(written_paths),
                     "round_trip_intact": round_trip_intact, "final_report": str(path)}
