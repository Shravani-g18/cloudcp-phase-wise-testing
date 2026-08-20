"""RT-01: Happy path - small flat files, upload."""
import live_common as lc

CASE_ID = "RT-01"
DESCRIPTION = "Happy path: small flat files (upload)"
STEPS = [
    "Cleanup /bryck/report_testing/RT-01 and s3://.../RT-01 (idempotent reset)",
    "datagen 120 files, 1-20MB each, flat dir",
    "Initiate upload transfer, poll until COMPLETED",
    "Download + unzip report, parse transfer_report_89.csv + transfer_summary.txt",
    "Assert: 120 rows, all SUCCESS, sizes match source, missing count == 0",
    "Cleanup source dir + S3 prefix, verify both empty",
]

EXPECTED_COUNT = 120


def _extra(entries, report_rows, summary, parsed):
    size_by_name = {e[0]: e[1] for e in entries}
    mismatches = []
    for row in report_rows:
        name = row.get("local_path", "").rsplit("/", 1)[-1]
        expected = None
        for relpath, size in size_by_name.items():
            if relpath.rsplit("/", 1)[-1] == name:
                expected = size
                break
        if expected is not None and int(row.get("size", -1)) != expected:
            mismatches.append(name)
    etags_present = all(bool(r.get("etag")) for r in report_rows)
    return (not mismatches and etags_present), {
        "size_mismatches": mismatches,
        "all_etags_present": etags_present,
    }


def run(ctx, out_dir):
    return lc.run_upload_case(ctx, CASE_ID, "RT-01_small_flat.yaml", EXPECTED_COUNT,
                               out_dir, extra_assert=_extra)
