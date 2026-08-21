"""RT-07: High file count, reusing the real CloudCpFallbackTesting datagen
spec (02_tiny_files.yaml, root: /bryck/cloudcp_fallback/tiny_files, 5,400 tiny
files) instead of a duplicate.
"""
import live_common as lc

CASE_ID = "RT-07"
DESCRIPTION = "High file count, 5400 files (upload)"
SPEC_REF = "../CloudCpFallbackTesting/spec_files/02_tiny_files.yaml"
STEPS = [
    "Cleanup RT-07 source dir and S3 prefix",
    "datagen (02_tiny_files.yaml): 5400 tiny files, tree layout",
    "Initiate upload transfer, poll until COMPLETED",
    "Download + parse report (timed)",
    "Assert 5400 rows, no duplicates, summary count == 5400",
    "Cleanup source dir + S3 prefix",
]

EXPECTED_COUNT = 5400


def _extra(entries, report_rows, summary, parsed):
    import time
    start = time.monotonic()
    paths = [r.get("local_path", "") for r in report_rows]
    no_dupes = len(set(paths)) == len(paths)
    parse_time_sec = time.monotonic() - start
    return no_dupes, {"no_duplicate_local_paths": no_dupes,
                       "parse_time_sec": round(parse_time_sec, 4)}


def run(ctx, out_dir):
    return lc.run_upload_case(ctx, CASE_ID, SPEC_REF, EXPECTED_COUNT,
                               out_dir, extra_assert=_extra)
