"""RT-10: Round-trip - upload then download, cross-check sizes/ETags."""
import live_common as lc
import bryck_client as bc

CASE_ID = "RT-10"
DESCRIPTION = "Round-trip: upload + download"
STEPS = [
    "Cleanup RT-10-src/RT-10-dst dirs and S3 prefix",
    "datagen 120 files (reuses RT-01 spec) into RT-10-src",
    "Upload transfer RT-10-src -> S3, poll until COMPLETED",
    "Download transfer S3 -> RT-10-dst, poll until COMPLETED",
    "Cross-check: size (and ETag where possible) match between upload and download reports",
    "Assert files at RT-10-dst match RT-10-src by name and size",
    "Cleanup both dirs + S3 prefix",
]

EXPECTED_COUNT = 120


def run(ctx, out_dir):
    src_dir = bc.remote_case_dir(ctx.cfg, CASE_ID, "src")
    dst_dir = bc.remote_case_dir(ctx.cfg, CASE_ID, "dst")
    s3_uri = bc.s3_case_uri(ctx.cfg, CASE_ID)
    details = {"src_dir": src_dir, "dst_dir": dst_dir, "s3_uri": s3_uri}

    try:
        lc.setup_case(ctx, [src_dir, dst_dir], [s3_uri])
        entries = lc.generate_data(ctx, CASE_ID, "RT-01_small_flat.yaml", src_dir,
                                    spec_case_dir="RT-01")
    except bc.LiveClientError as exc:
        details["error"] = str(exc)
        return "SETUP_ERROR", details
    details["source_file_count"] = len(entries)

    try:
        up_id, up_state, _h = lc.initiate_and_wait(ctx, src_dir, s3_uri)
    except bc.LiveClientError as exc:
        details["error"] = str(exc)
        return "SETUP_ERROR", details
    details["upload_transfer_id"] = up_id
    details["upload_state"] = up_state
    if up_state != "COMPLETED":
        details["cleanup"] = lc.cleanup_case(ctx, [src_dir, dst_dir], [s3_uri])
        return "TRANSFER_FAILED", details

    try:
        up_parsed = lc.download_and_parse(ctx, up_id, out_dir)
    except (bc.LiveClientError, KeyError, ValueError, OSError) as exc:
        details["error"] = f"upload report parse failed: {exc}"
        return "REPORT_PARSE_ERROR", details

    try:
        dl_id, dl_state, _h = lc.initiate_and_wait(ctx, s3_uri, dst_dir)
    except bc.LiveClientError as exc:
        details["error"] = str(exc)
        details["cleanup"] = lc.cleanup_case(ctx, [src_dir, dst_dir], [s3_uri])
        return "SETUP_ERROR", details
    details["download_transfer_id"] = dl_id
    details["download_state"] = dl_state

    if dl_state == "TIMEOUT":
        details["cleanup"] = lc.cleanup_case(ctx, [src_dir, dst_dir], [s3_uri])
        return "TIMEOUT", details
    if dl_state != "COMPLETED":
        details["cleanup"] = lc.cleanup_case(ctx, [src_dir, dst_dir], [s3_uri])
        return "TRANSFER_FAILED", details

    try:
        dl_parsed = lc.download_and_parse(ctx, dl_id, out_dir)
    except bc.LiveClientError as exc:
        details["error"] = str(exc)
        return "REPORT_DOWNLOAD_ERROR", details
    except (KeyError, ValueError, OSError) as exc:
        details["error"] = str(exc)
        return "REPORT_PARSE_ERROR", details

    up_rows = up_parsed["report_rows"]
    dl_rows = dl_parsed["report_rows"]
    details["upload_row_count"] = len(up_rows)
    details["download_row_count"] = len(dl_rows)

    up_all_success = all(r.get("status", "").upper() == "SUCCESS" for r in up_rows)
    dl_all_success = all(r.get("status", "").upper() == "SUCCESS" for r in dl_rows)
    counts_match = len(up_rows) == len(dl_rows) == EXPECTED_COUNT

    up_by_name = {r.get("local_path", "").rsplit("/", 1)[-1]: r for r in up_rows}
    dl_by_name = {r.get("local_path", "").rsplit("/", 1)[-1]: r for r in dl_rows}
    size_mismatches = [
        name for name, up_row in up_by_name.items()
        if name in dl_by_name and int(up_row.get("size", -1)) != int(dl_by_name[name].get("size", -2))
    ]
    etag_mismatches = [
        name for name, up_row in up_by_name.items()
        if name in dl_by_name and up_row.get("etag") and dl_by_name[name].get("etag")
        and up_row.get("etag") != dl_by_name[name].get("etag")
    ]

    downloaded_files = bc.enumerate_remote_files(ctx.ssh(), dst_dir)
    downloaded_by_name = {p.rsplit("/", 1)[-1]: sz for p, sz in downloaded_files}
    src_by_name = {p.rsplit("/", 1)[-1]: sz for p, sz in entries}
    files_match_on_disk = all(
        name in downloaded_by_name and downloaded_by_name[name] == size
        for name, size in src_by_name.items()
    )

    details["size_mismatches"] = size_mismatches
    details["etag_mismatches"] = etag_mismatches
    details["files_match_on_disk"] = files_match_on_disk

    passed = (up_all_success and dl_all_success and counts_match
              and not size_mismatches and not etag_mismatches and files_match_on_disk)
    status = "PASS" if passed else "FAIL"
    cleanup_result = lc.cleanup_case(ctx, [src_dir, dst_dir], [s3_uri])
    details["cleanup"] = cleanup_result
    status = lc.downgrade_status_for_cleanup(status, cleanup_result)
    return status, details
