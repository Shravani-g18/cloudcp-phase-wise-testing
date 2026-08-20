"""RT-06: Single large file (600MB-1GB), multipart upload."""
import live_common as lc

CASE_ID = "RT-06"
DESCRIPTION = "Single large file (upload, multipart)"
STEPS = [
    "Cleanup RT-06 source dir and S3 prefix",
    "datagen 1 file, 600MB-1GB",
    "Initiate upload transfer, poll until COMPLETED",
    "Download + parse report",
    "Assert exactly 1 SUCCESS row, size matches source, ETag present",
    "Cleanup source dir + S3 prefix",
]

EXPECTED_COUNT = 1


def _extra(entries, report_rows, summary, parsed):
    if not entries or not report_rows:
        return False, {"reason": "no source entries or no report rows"}
    source_size = entries[0][1]
    reported_size = int(report_rows[0].get("size", -1))
    size_match = source_size == reported_size
    return size_match, {"source_size": source_size, "reported_size": reported_size,
                         "size_match": size_match}


def run(ctx, out_dir):
    return lc.run_upload_case(ctx, CASE_ID, "RT-06_large_single.yaml", EXPECTED_COUNT,
                               out_dir, extra_assert=_extra)
