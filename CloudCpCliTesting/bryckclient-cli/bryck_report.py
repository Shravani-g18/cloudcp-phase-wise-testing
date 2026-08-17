#!/usr/bin/env python3
"""
Standalone Bryck diagnostic report runner.

Replicates the flow of ``Configuration.get_bryck_report`` from the
functional test suite:

    1. POST /api/tasks/capture_bryck_state          (start generation)
    2. Poll GET /api/tasks/list?task_type=CAPTURE_BRYCK_STATE
       until the newest entry has ``state == "COMPLETED"``.
    3. GET  /api/download?name=bryck_report         (download tgz)
    4. Save the tgz to ``<output_dir>/bryck_report.tgz``.

Usage:
    python3 bryck_report.py --output-dir /some/local/path
    python3 bryck_report.py --output-dir /tmp/reports \
        --login /etc/bryck/login.json
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time as _time

from bryck_api import BryckApi, ticker
from session import ApiSession

logger = logging.getLogger(__name__)

DEFAULT_LOGIN_JSON = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "login.json"
)

# Match the functional-test defaults.
REPORT_TIMEOUT = 900           # ticker budget for CAPTURE_BRYCK_STATE
REPORT_INITIAL_DELAY = 5       # matches sleep(5) in configuration.py
REPORT_FILENAME = "bryck_report.tgz"


# =============================================================================
# Validator
# =============================================================================

def _validate_report_generated(api: BryckApi) -> bool:
    """Poll callback: True once capture task reports state == COMPLETED."""
    resp = api.check_bryck_report_generate()
    if resp is None:
        return False
    try:
        payload = resp.json()
    except ValueError:
        logger.debug("check_bryck_report_generate returned non-JSON")
        return False

    if not payload.get("success"):
        logger.debug("check_bryck_report_generate success=False: %s", payload)
        return False

    result = payload.get("result") or []
    if not result:
        return False
    state = result[0].get("state")
    logger.debug("CAPTURE_BRYCK_STATE state=%s", state)
    return state == "COMPLETED"


# =============================================================================
# Helpers
# =============================================================================

def _write_report(response, dest_path: str) -> None:
    """Stream the report body to dest_path on disk."""
    with open(dest_path, "wb") as fh:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                fh.write(chunk)


# =============================================================================
# Entry point
# =============================================================================

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Generate and download a Bryck diagnostic report.",
    )
    parser.add_argument("--login", default=DEFAULT_LOGIN_JSON)
    parser.add_argument(
        "--output-dir",
        required=True,
        help=(
            "Local directory to save bryck_report.tgz into. "
            "Created if it does not exist."
        ),
    )
    parser.add_argument(
        "--filename",
        default=REPORT_FILENAME,
        help=f"Output filename (default: {REPORT_FILENAME}).",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    output_dir = os.path.abspath(args.output_dir)
    try:
        os.makedirs(output_dir, exist_ok=True)
    except OSError as exc:
        logger.error("Cannot create output directory %s: %s", output_dir, exc)
        return 2
    dest_path = os.path.join(output_dir, args.filename)

    session = ApiSession.from_login_json(args.login)
    try:
        session.login()
        api = BryckApi(session)

        logger.info("Requesting Bryck report generation")
        gen_resp = api.start_bryck_report_generate()
        if gen_resp is None:
            logger.error("start_bryck_report_generate: no response")
            return 1
        if gen_resp.status_code >= 400:
            logger.error(
                "start_bryck_report_generate HTTP %s: %s",
                gen_resp.status_code, gen_resp.text,
            )
            return 1

        # Give the server a moment to register the task before polling.
        _time.sleep(REPORT_INITIAL_DELAY)

        logger.info(
            "Waiting for report to reach state=COMPLETED (timeout=%ds)",
            REPORT_TIMEOUT,
        )
        try:
            ticker(lambda: _validate_report_generated(api), REPORT_TIMEOUT)
        except TimeoutError as exc:
            logger.error(
                "Report generation FAILED after %ds "
                "(did not reach COMPLETED): %s",
                REPORT_TIMEOUT, exc,
            )
            return 3
        logger.info("Report generation completed")

        logger.info("Downloading report to %s", dest_path)
        dl_resp = api.download_bryck_report()
        if dl_resp is None:
            logger.error("download_bryck_report: no response")
            return 1
        if dl_resp.status_code >= 400:
            logger.error(
                "download_bryck_report HTTP %s: %s",
                dl_resp.status_code, dl_resp.text,
            )
            return 1

        try:
            _write_report(dl_resp, dest_path)
        except OSError as exc:
            logger.error("Failed to write %s: %s", dest_path, exc)
            return 1

        size = os.path.getsize(dest_path)
        logger.info("Saved %s (%d bytes)", dest_path, size)
        return 0
    finally:
        session.close()


if __name__ == "__main__":
    sys.exit(main())
