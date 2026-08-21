"""RT-03: Filename encoding variants, reusing the real CloudCpFallbackTesting
datagen spec (09_unicode_names.yaml, root: /bryck/cloudcp_fallback/unicode_names)
instead of a duplicate - already covers spaces/parentheses/brackets plus
Latin/Cyrillic/CJK/emoji unicode.
"""
import live_common as lc

CASE_ID = "RT-03"
DESCRIPTION = "Filename encoding variants (upload)"
SPEC_REF = "../CloudCpFallbackTesting/spec_files/09_unicode_names.yaml"
STEPS = [
    "Cleanup RT-03 source dir and S3 prefix",
    "datagen (09_unicode_names.yaml): 150 files, unicode/emoji/CJK/spaces names",
    "Initiate upload transfer, poll until COMPLETED",
    "Download + parse report",
    "Assert paths round-trip through the CSV without truncation or corruption",
    "Cleanup source dir + S3 prefix",
]

EXPECTED_COUNT = 150


def _extra(entries, report_rows, summary, parsed):
    source_names = {e[0].rsplit("/", 1)[-1] for e in entries}
    report_names = {r.get("local_path", "").rsplit("/", 1)[-1] for r in report_rows}
    round_trip_ok = source_names == report_names
    encoding_failures = [r for r in report_rows if r.get("status", "").upper() != "SUCCESS"]
    return (round_trip_ok and not encoding_failures), {
        "round_trip_ok": round_trip_ok,
        "missing_from_report": sorted(source_names - report_names),
        "unexpected_in_report": sorted(report_names - source_names),
        "non_success_rows": len(encoding_failures),
    }


def run(ctx, out_dir):
    return lc.run_upload_case(ctx, CASE_ID, SPEC_REF, EXPECTED_COUNT,
                               out_dir, extra_assert=_extra)
