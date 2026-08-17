#!/usr/bin/env python3
"""
Standalone Bryck cloud-transfer report downloader.

Flow:
    1. Log in to the Bryck REST API.
    2. GET /api/download?name=cloud_log&type=<transfer_id> as a
       streamed response.
    3. Write the response body to <report_path> in 8 KiB chunks.

The downloaded file is the raw ZIP produced by the Bryck (containing
``transfer_summary.txt``, ``transfer_report.json``, etc.). Extraction
and parsing are out of scope for this runner.

Usage:
    python3 bryck_cloud_transfer_report.py --cloud-transfer-id ID \
        [--report-path PATH] [--login PATH]
"""
from __future__ import annotations

import argparse
import logging
import os
import sys

from bryck_api import BryckApi
from session import ApiSession

logger = logging.getLogger(__name__)

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_LOGIN_JSON = os.path.join(_SCRIPT_DIR, "login.json")

_CHUNK_SIZE = 8192


# =============================================================================
# Helpers
# =============================================================================

def _resolve_report_path(user_value: str | None, transfer_id: str) -> str:
    """Return the on-disk path to write the report zip to.

    - No value          -> ./cloud_transfer_report_<id>.zip in cwd.
    - Existing dir      -> <dir>/cloud_transfer_report_<id>.zip.
    - Anything else     -> treated as a full file path (verbatim).
    """
    default_name = f"cloud_transfer_report_{transfer_id}.zip"
    if not user_value:
        return os.path.abspath(default_name)
    expanded = os.path.expanduser(user_value)
    if os.path.isdir(expanded):
        return os.path.join(os.path.abspath(expanded), default_name)
    return os.path.abspath(expanded)


# =============================================================================
# Entry point
# =============================================================================

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Download the cloud-transfer report ZIP for a given "
            "transfer ID from the Bryck."
        )
    )
    parser.add_argument(
        "--cloud-transfer-id",
        required=True,
        help="Cloud transfer ID whose report should be downloaded.",
    )
    parser.add_argument(
        "--report-path",
        default=None,
        help=(
            "Destination path or directory. Defaults to "
            "./cloud_transfer_report_<id>.zip in the current directory."
        ),
    )
    parser.add_argument("--login", default=DEFAULT_LOGIN_JSON)
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    transfer_id = str(args.cloud_transfer_id).strip()
    if not transfer_id:
        logger.error("--cloud-transfer-id must be non-empty")
        return 2

    save_path = _resolve_report_path(args.report_path, transfer_id)
    parent = os.path.dirname(save_path)
    if parent and not os.path.isdir(parent):
        logger.error("Destination directory does not exist: %s", parent)
        return 2

    session = ApiSession.from_login_json(args.login)
    try:
        session.login()
        api = BryckApi(session)

        logger.info(
            "Downloading cloud-transfer report: transfer_id=%s -> %s",
            transfer_id, save_path,
        )
        resp = api.download_cloud_transfer_log(transfer_id)
        if resp is None:
            logger.error(
                "download_cloud_transfer_log request failed for %s; "
                "aborting (see previous error).",
                transfer_id,
            )
            return 1

        # Check for HTTP errors before downloading
        if not resp.ok:
            logger.error(
                "Failed to download report for transfer %s: HTTP %d %s",
                transfer_id, resp.status_code, resp.reason
            )
            try:
                error_data = resp.json()
                if isinstance(error_data, dict) and "error" in error_data:
                    error_msg = error_data["error"]
                    if isinstance(error_msg, dict):
                        logger.error("Server error: %s", error_msg.get("message", error_msg))
                    elif isinstance(error_msg, str):
                        logger.error("Server error: %s", error_msg)
            except Exception:
                pass
            resp.close()
            return 1

        bytes_written = 0
        try:
            with open(save_path, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=_CHUNK_SIZE):
                    if not chunk:
                        continue
                    fh.write(chunk)
                    bytes_written += len(chunk)
        except OSError as exc:
            logger.error("Failed to write report to %s: %s", save_path, exc)
            return 3
        finally:
            resp.close()

        if bytes_written == 0:
            logger.error(
                "Report for transfer %s downloaded 0 bytes; "
                "the Bryck may not have a report for this ID.",
                transfer_id,
            )
            return 3

        logger.info(
            "Cloud-transfer report saved: %s (%d bytes)",
            save_path, bytes_written,
        )
        return 0
    finally:
        session.close()


if __name__ == "__main__":
    sys.exit(main())
