"""RT-09: Download transfer (S3 -> /bryck).

Self-contained: seeds its own upload first (RT-09-src -> RT-09 S3 prefix)
rather than depending on RT-01 having already run and left data in S3, so
this case can be run standalone (--one RT-09) or after --all with any
per-case cleanup policy.
"""
import live_common as lc
import bryck_client as bc

CASE_ID = "RT-09"
DESCRIPTION = "Download transfer (S3 -> /bryck)"
STEPS = [
    "Cleanup RT-09 source/download dirs and S3 prefix",
    "datagen 120 files (reuses RT-01 spec) into RT-09-src, upload to seed S3",
    "Initiate DOWNLOAD transfer S3 -> RT-09-download, poll until COMPLETED",
    "Download + parse report",
    "Assert 120 rows, all SUCCESS, sizes match the seeded upload, files present on disk",
    "Cleanup source/download dirs + S3 prefix",
]

EXPECTED_COUNT = 120


def run(ctx, out_dir):
    src_dir = bc.remote_case_dir(ctx.cfg, CASE_ID, "src")
    dl_dir = bc.remote_case_dir(ctx.cfg, CASE_ID, "download")
    s3_uri = bc.s3_case_uri(ctx.cfg, CASE_ID)
    details = {"src_dir": src_dir, "download_dir": dl_dir, "s3_uri": s3_uri}

    try:
        lc.setup_case(ctx, [src_dir, dl_dir], [s3_uri])
        entries = lc.generate_data(ctx, CASE_ID, "RT-01_small_flat.yaml", src_dir,
                                    spec_case_dir="RT-01")
    except bc.LiveClientError as exc:
        details["error"] = str(exc)
        return "SETUP_ERROR", details
    details["source_file_count"] = len(entries)

    try:
        seed_id, seed_state, _h = lc.initiate_and_wait(ctx, src_dir, s3_uri)
    except bc.LiveClientError as exc:
        details["error"] = str(exc)
        return "SETUP_ERROR", details
    details["seed_upload_transfer_id"] = seed_id
    details["seed_upload_state"] = seed_state
    if seed_state != "COMPLETED":
        details["cleanup"] = lc.cleanup_case(ctx, [src_dir, dl_dir], [s3_uri])
        return "TRANSFER_FAILED", details

    try:
        dl_id, dl_state, _h = lc.initiate_and_wait(ctx, s3_uri, dl_dir)
    except bc.LiveClientError as exc:
        details["error"] = str(exc)
        details["cleanup"] = lc.cleanup_case(ctx, [src_dir, dl_dir], [s3_uri])
        return "SETUP_ERROR", details
    details["download_transfer_id"] = dl_id
    details["download_state"] = dl_state

    if dl_state == "TIMEOUT":
        details["cleanup"] = lc.cleanup_case(ctx, [src_dir, dl_dir], [s3_uri])
        return "TIMEOUT", details
    if dl_state != "COMPLETED":
        details["cleanup"] = lc.cleanup_case(ctx, [src_dir, dl_dir], [s3_uri])
        return "TRANSFER_FAILED", details

    try:
        parsed = lc.download_and_parse(ctx, dl_id, out_dir)
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
    checks = lc.cross_cutting(report_rows, summary)
    details["cross_cutting_checks"] = checks

    all_success = bool(report_rows) and all(
        r.get("status", "").upper() == "SUCCESS" for r in report_rows
    )
    count_ok = len(report_rows) == EXPECTED_COUNT

    downloaded_files = bc.enumerate_remote_files(ctx.ssh(), dl_dir)
    downloaded_by_name = {p.rsplit("/", 1)[-1]: sz for p, sz in downloaded_files}
    seeded_by_name = {p.rsplit("/", 1)[-1]: sz for p, sz in entries}
    files_on_disk_ok = all(
        name in downloaded_by_name and downloaded_by_name[name] == size
        for name, size in seeded_by_name.items()
    )
    details["files_on_disk_ok"] = files_on_disk_ok
    details["downloaded_file_count"] = len(downloaded_files)

    passed = all_success and count_ok and all(checks.values()) and files_on_disk_ok
    status = "PASS" if passed else "FAIL"
    cleanup_result = lc.cleanup_case(ctx, [src_dir, dl_dir], [s3_uri])
    details["cleanup"] = cleanup_result
    status = lc.downgrade_status_for_cleanup(status, cleanup_result)
    return status, details
