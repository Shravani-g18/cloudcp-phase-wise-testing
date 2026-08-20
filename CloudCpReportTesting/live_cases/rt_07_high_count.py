"""RT-07: High file count (1,000 tiny files)."""
import live_common as lc

CASE_ID = "RT-07"
DESCRIPTION = "High file count, 1000 files (upload)"
STEPS = [
    "Cleanup RT-07 source dir and S3 prefix",
    "datagen 1000 files, 512KB each, flat",
    "Initiate upload transfer, poll until COMPLETED",
    "Download + parse report (timed)",
    "Assert 1000 rows, no duplicates, summary count == 1000",
    "Cleanup source dir + S3 prefix",
]

EXPECTED_COUNT = 1000


def _extra(entries, report_rows, summary, parsed):
    import time
    start = time.monotonic()
    paths = [r.get("local_path", "") for r in report_rows]
    no_dupes = len(set(paths)) == len(paths)
    parse_time_sec = time.monotonic() - start
    return no_dupes, {"no_duplicate_local_paths": no_dupes,
                       "parse_time_sec": round(parse_time_sec, 4)}


def run(ctx, out_dir):
    return lc.run_upload_case(ctx, CASE_ID, "RT-07_high_count.yaml", EXPECTED_COUNT,
                               out_dir, extra_assert=_extra)
