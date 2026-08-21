"""RT-08: Re-upload to same destination (idempotency) - two uploads, no S3 cleanup between.

Reuses the real CloudCpFallbackTesting datagen spec (03_small_files.yaml,
same one RT-01 uses) instead of a duplicate.
"""
import live_common as lc
import bryck_client as bc

CASE_ID = "RT-08"
DESCRIPTION = "Re-upload to same destination (idempotency)"
SPEC_REF = "../CloudCpFallbackTesting/spec_files/03_small_files.yaml"
STEPS = [
    "Cleanup RT-08 source dir and S3 prefix",
    "datagen (03_small_files.yaml) fresh data set",
    "First upload transfer -> confirm COMPLETED",
    "Second upload transfer to the SAME S3 dest, without cleaning S3 first -> confirm COMPLETED",
    "Assert second report: every row SUCCESS or SKIPPED, zero FAILED-by-already-exists rows",
    "Cleanup source dir + S3 prefix",
]

EXPECTED_COUNT = 120
ACCEPTABLE_STATUSES = {"SUCCESS", "SKIPPED"}


def run(ctx, out_dir):
    remote_dir = bc.remote_case_dir(ctx.cfg, CASE_ID)
    s3_uri = bc.s3_case_uri(ctx.cfg, CASE_ID)
    details = {"remote_dir": remote_dir, "s3_uri": s3_uri}

    try:
        lc.setup_case(ctx, [remote_dir], [s3_uri])
        entries = lc.generate_data(ctx, CASE_ID, SPEC_REF, remote_dir)
    except bc.LiveClientError as exc:
        details["error"] = str(exc)
        return "SETUP_ERROR", details
    details["source_file_count"] = len(entries)

    try:
        first_id, first_state, _h = lc.initiate_and_wait(ctx, remote_dir, s3_uri)
    except bc.LiveClientError as exc:
        details["error"] = str(exc)
        return "SETUP_ERROR", details
    details["first_transfer_id"] = first_id
    details["first_state"] = first_state
    if first_state != "COMPLETED":
        details["cleanup"] = lc.cleanup_case(ctx, [remote_dir], [s3_uri])
        return "TRANSFER_FAILED", details

    # Deliberately skip S3 cleanup: second upload targets the same, already-populated dest.
    try:
        second_id, second_state, _h = lc.initiate_and_wait(ctx, remote_dir, s3_uri)
    except bc.LiveClientError as exc:
        details["error"] = str(exc)
        details["cleanup"] = lc.cleanup_case(ctx, [remote_dir], [s3_uri])
        return "SETUP_ERROR", details
    details["second_transfer_id"] = second_id
    details["second_state"] = second_state

    if second_state == "TIMEOUT":
        details["cleanup"] = lc.cleanup_case(ctx, [remote_dir], [s3_uri])
        return "TIMEOUT", details
    if second_state != "COMPLETED":
        details["cleanup"] = lc.cleanup_case(ctx, [remote_dir], [s3_uri])
        return "TRANSFER_FAILED", details

    try:
        parsed = lc.download_and_parse(ctx, second_id, out_dir)
    except bc.LiveClientError as exc:
        details["error"] = str(exc)
        return "REPORT_DOWNLOAD_ERROR", details
    except (KeyError, ValueError, OSError) as exc:
        details["error"] = str(exc)
        return "REPORT_PARSE_ERROR", details

    report_rows = parsed["report_rows"]
    summary = parsed["summary"]
    details["report_row_count"] = len(report_rows)
    details["summary"] = summary

    statuses_ok = all(r.get("status", "").upper() in ACCEPTABLE_STATUSES for r in report_rows)
    failed_rows = [r for r in report_rows if r.get("status", "").upper() == "FAILED"]
    count_ok = len(report_rows) == EXPECTED_COUNT
    details["statuses_ok"] = statuses_ok
    details["failed_row_count"] = len(failed_rows)
    details["row_count_matches_expected"] = count_ok

    passed = statuses_ok and not failed_rows and count_ok
    status = "PASS" if passed else "FAIL"
    cleanup_result = lc.cleanup_case(ctx, [remote_dir], [s3_uri])
    details["cleanup"] = cleanup_result
    status = lc.downgrade_status_for_cleanup(status, cleanup_result)
    return status, details
