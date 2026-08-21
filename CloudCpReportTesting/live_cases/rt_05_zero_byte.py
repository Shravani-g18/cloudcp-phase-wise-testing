"""RT-05: Zero-byte files, reusing the real CloudCpFallbackTesting datagen
spec (01_zero_byte.yaml, root: /bryck/cloudcp_fallback/zero_byte) instead of
a duplicate.
"""
import live_common as lc

CASE_ID = "RT-05"
DESCRIPTION = "Zero-byte files (upload)"
SPEC_REF = "../CloudCpFallbackTesting/spec_files/01_zero_byte.yaml"
STEPS = [
    "Cleanup RT-05 source dir and S3 prefix",
    "datagen (01_zero_byte.yaml): 200 zero-byte files",
    "Initiate upload transfer, poll until COMPLETED",
    "Download + parse report",
    "Assert all 200 appear with size==0, SUCCESS, non-empty ETag",
    "Cleanup source dir + S3 prefix",
]

EXPECTED_COUNT = 200

_EMPTY_OBJECT_ETAG = "d41d8cd98f00b204e9800998ecf8427e"


def _extra(entries, report_rows, summary, parsed):
    all_zero = all(int(r.get("size", -1)) == 0 for r in report_rows)
    etags_present = all(bool(r.get("etag")) for r in report_rows)
    # tool silently dropping zero-byte files is the known defect this case catches
    not_silently_dropped = len(report_rows) == EXPECTED_COUNT
    return (all_zero and etags_present and not_silently_dropped), {
        "all_sizes_zero": all_zero,
        "all_etags_present": etags_present,
        "row_count": len(report_rows),
        "not_silently_dropped": not_silently_dropped,
    }


def run(ctx, out_dir):
    return lc.run_upload_case(ctx, CASE_ID, SPEC_REF, EXPECTED_COUNT,
                               out_dir, extra_assert=_extra)
