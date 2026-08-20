#!/usr/bin/env python3
"""
Standalone Bryck cloud-transfer pause runner.

Pauses an active cloud transfer and validates that it enters PAUSED state.

Usage:
    python3 bryck_cloud_transfer_pause.py --transfer-id <id> [--login PATH]
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from typing import Any

from bryck_api import BryckApi, ticker, display_error, extract_error_info
from session import ApiSession

logger = logging.getLogger(__name__)

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_LOGIN_JSON = os.path.join(_SCRIPT_DIR, "login.json")

VALIDATION_TIMEOUT = 60

STATE_PAUSED = "PAUSED"
STATE_IN_PROGRESS = "IN_PROGRESS"


def _validate_paused(api: BryckApi, transfer_id: str) -> bool:
    """Poll callback: True once the transfer enters PAUSED state."""
    resp = api.get_cloud_transfer_status(transfer_id)
    if resp is None:
        return False
    
    try:
        body = resp.json()
    except ValueError:
        return False
    
    result = body.get("result", {})
    
    # Handle both list and dict formats
    entry: dict[str, Any] | None = None
    if isinstance(result, list):
        for item in result:
            if isinstance(item, dict) and item:
                entry = item
                break
    elif isinstance(result, dict) and result:
        entry = result
    
    if entry is None:
        return False
    
    state = str(entry.get("state") or "").upper()
    return state == STATE_PAUSED


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Pause an active cloud transfer and validate state change."
    )
    parser.add_argument(
        "--transfer-id",
        required=True,
        help="Transfer ID to pause.",
    )
    parser.add_argument("--login", default=DEFAULT_LOGIN_JSON)
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    session = ApiSession.from_login_json(args.login)
    try:
        session.login()
        api = BryckApi(session)
        
        transfer_id = str(args.transfer_id).strip()
        logger.info("Pausing transfer_id=%s", transfer_id)
        
        resp = api.pause_cloud_transfer(transfer_id)
        if resp is None:
            display_error("Pause Transfer", message="Request failed (see logs for details)")
            return 1
        
        if resp.status_code != 200:
            status_code, status_text, message = extract_error_info(resp)
            display_error(
                "Pause Transfer",
                status_code=status_code,
                status_text=status_text,
                message=message,
                endpoint="/api/bcloud/pause_transfer"
            )
            return 1
        
        logger.info("Pause request sent, validating state change...")
        try:
            ticker(
                lambda: _validate_paused(api, transfer_id),
                VALIDATION_TIMEOUT,
                message="Waiting for transfer to pause",
            )
        except TimeoutError:
            display_error(
                "Pause Transfer Validation",
                message=f"Transfer {transfer_id} did not reach PAUSED state in {VALIDATION_TIMEOUT}s"
            )
            return 3
        
        logger.info("Transfer %s successfully paused", transfer_id)
        return 0
        
    finally:
        session.close()


if __name__ == "__main__":
    sys.exit(main())
