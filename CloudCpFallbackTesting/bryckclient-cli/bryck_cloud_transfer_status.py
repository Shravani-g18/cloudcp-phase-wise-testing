#!/usr/bin/env python3
"""
Standalone Bryck cloud-transfer status runner.

Flow:
    1. Log in to the Bryck REST API.
    2. If ``--transfer-id`` is given, GET /api/bcloud/status_transfer
       for that ID and print a single summary line.
    3. If ``--state`` is given, POST /api/bcloud/list_transfer for
       all transfers, filter by the specified state (case-insensitive),
       and print one summary line per matching transfer.
    4. If neither flag is given, POST /api/bcloud/list_transfer for
       all transfers (no state filtering) and print one summary line
       per transfer.

The runner is intentionally minimal:
    - No SSH, no state precondition, no ticker polling -- this is a
      read-only status query.
    - Empty results ("transfer not found" / "no matching transfers")
      are NOT errors; the runner logs a message and exits 0.
    - ``--transfer-id`` and ``--state`` are mutually exclusive.

Usage:
    python3 bryck_cloud_transfer_status.py [--transfer-id ID] [--login PATH]
    python3 bryck_cloud_transfer_status.py [--state STATE] [--login PATH]
    python3 bryck_cloud_transfer_status.py [--login PATH]  # all transfers
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from typing import Any

from bryck_api import BryckApi, display_error, extract_error_info
from session import ApiSession

logger = logging.getLogger(__name__)

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_LOGIN_JSON = os.path.join(_SCRIPT_DIR, "login.json")

STATE_COMPLETED = "COMPLETED"
VALID_STATES = (
    "IN_PROGRESS",
    "COMPLETED",
    "PAUSED",
    "FAILED",
    "STOPPED",
    "CANCELLED",
)

_BYTES_PER_GB = 1024 ** 3
_MISSING = "-"


# =============================================================================
# Helpers
# =============================================================================

def _extract_result(resp: Any) -> Any:
    """Return ``result`` from a Response body, or None on any failure."""
    if resp is None:
        return None
    try:
        body = resp.json()
    except ValueError:
        return None
    if not isinstance(body, dict):
        return None
    return body.get("result")


def _to_gb(value: Any) -> str:
    """Format a byte count as 'X.XX GB'; return '- GB' if not numeric."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return f"{_MISSING} GB"
    return f"{value / _BYTES_PER_GB:.2f} GB"


def _fmt_percent(value: Any) -> str:
    """Format a percent as an integer when whole, else 2 decimals."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return _MISSING
    if float(value).is_integer():
        return str(int(value))
    return f"{value:.2f}"


def _fmt_str(value: Any) -> str:
    """Stringify a scalar; return '-' for None/empty."""
    if value is None:
        return _MISSING
    s = str(value).strip()
    return s if s else _MISSING


def _summarize(entry: dict[str, Any]) -> str:
    """Format one transfer entry with dividers and multi-line layout."""
    divider = "─" * 80
    return (
        f"\n{divider}\n"
        f"  TRANSFER_ID    : {_fmt_str(entry.get('task_id'))}\n"
        f"  STATE          : {_fmt_str(entry.get('state')).upper()}\n"
        f"  PROGRESS       : {_to_gb(entry.get('copied_bytes'))} / {_to_gb(entry.get('total_bytes'))} "
        f"({_fmt_percent(entry.get('percent_completed'))}% completed)\n"
        f"  SOURCE         : {_fmt_str(entry.get('src'))}\n"
        f"  DESTINATION    : {_fmt_str(entry.get('dst'))}\n"
        f"  STARTED_AT     : {_fmt_str(entry.get('started_at'))}\n"
        f"  LAST_UPDATED   : {_fmt_str(entry.get('last_updated'))}\n"
        f"{divider}"
    )


def _get_transfer_id(entry: dict[str, Any]) -> int:
    """Extract numeric transfer ID for sorting; return 0 if missing/invalid."""
    task_id = entry.get("task_id")
    if isinstance(task_id, int):
        return task_id
    if isinstance(task_id, str):
        try:
            return int(task_id)
        except ValueError:
            return 0
    return 0


# =============================================================================
# Entry point
# =============================================================================

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Display Bryck cloud-transfer status. With --transfer-id, "
            "show one specific transfer. With --state, show all "
            "transfers matching that state (case-insensitive). With "
            "neither, show all transfers. --transfer-id and --state "
            "are mutually exclusive."
        )
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--transfer-id",
        default=None,
        help="Show details for a single transfer ID.",
    )
    group.add_argument(
        "--state",
        default=None,
        help=(
            "Filter transfers by state (case-insensitive). "
            "Valid states: IN_PROGRESS, COMPLETED, PAUSED, "
            "FAILED, STOPPED, CANCELLED."
        ),
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

        # ---- Single-transfer lookup --------------------------------
        if args.transfer_id is not None:
            transfer_id = str(args.transfer_id).strip()
            
            # Empty ID is a user input error
            if not transfer_id:
                display_error(
                    operation="Get Transfer Status",
                    message="Transfer ID cannot be empty"
                )
                return 2
            
            resp = api.get_cloud_transfer_status(transfer_id)
            if resp is None:
                display_error(
                    operation="Get Transfer Status",
                    message="Failed to connect to Bryck API (see logs for details)"
                )
                return 1
            
            # Check for HTTP errors (4xx, 5xx)
            if not resp.ok:
                status_code, status_text, message = extract_error_info(resp)
                display_error(
                    operation="Get Transfer Status",
                    status_code=status_code,
                    status_text=status_text,
                    message=message or f"Transfer {transfer_id} not found",
                    endpoint="/api/bcloud/status_transfer"
                )
                return 1
            
            result = _extract_result(resp)
            # /api/bcloud/status_transfer returns ``result`` as a
            # list of dicts (typically one). Older/alt schemas may
            # send a bare dict -- accept both.
            entry: dict[str, Any] | None = None
            if isinstance(result, list):
                for item in result:
                    if isinstance(item, dict) and item:
                        entry = item
                        break
            elif isinstance(result, dict) and result:
                entry = result
            if entry is None:
                display_error(
                    operation="Get Transfer Status",
                    message=f"Transfer {transfer_id} not found on the Bryck"
                )
                return 1
            logger.info("%s", _summarize(entry))
            return 0

        # ---- List all transfers (optionally filtered by state) ----
        resp = api.get_list_of_cloud_transfers()
        if resp is None:
            display_error(
                operation="List Transfers",
                message="Failed to connect to Bryck API (see logs for details)"
            )
            return 1
        
        # Check for HTTP errors (4xx, 5xx)
        if not resp.ok:
            status_code, status_text, message = extract_error_info(resp)
            display_error(
                operation="List Transfers",
                status_code=status_code,
                status_text=status_text,
                message=message,
                endpoint="/api/bcloud/list_transfer"
            )
            return 1
        
        result = _extract_result(resp)
        if not isinstance(result, list):
            result = []
        
        # Filter by state if --state was provided
        if args.state:
            filter_state = args.state.strip().upper()
            if filter_state not in VALID_STATES:
                logger.warning(
                    "State %r is not in the canonical set %s; "
                    "filtering anyway (API may accept it).",
                    args.state, VALID_STATES,
                )
            matching = [
                e for e in result
                if isinstance(e, dict)
                and str(e.get("state") or "").upper() == filter_state
            ]
            if not matching:
                logger.info(
                    "No cloud transfers found with state=%r", args.state
                )
                return 0
            # Sort by transfer ID numerically
            matching.sort(key=_get_transfer_id)
            for entry in matching:
                logger.info("%s", _summarize(entry))
            return 0
        
        # No filter — show all transfers
        all_transfers = [e for e in result if isinstance(e, dict)]
        if not all_transfers:
            logger.info("No cloud transfers found")
            return 0
        # Sort by transfer ID numerically
        all_transfers.sort(key=_get_transfer_id)
        for entry in all_transfers:
            logger.info("%s", _summarize(entry))
        return 0
    finally:
        session.close()


if __name__ == "__main__":
    sys.exit(main())
