#!/usr/bin/env python3
"""
Standalone Bryck system-log runner.

Fetches system event logs from the Bryck platform and prints them in a
human-readable table.  Optionally saves the raw JSON to a file and/or
marks all retrieved entries as read.

Usage:
    python3 bryck_get_logs.py [--period today|week|month] [--login PATH]
                              [--output PATH] [--mark-read]

Arguments:
    --period    Time range to fetch.  Choices: today (default), week, month.
    --login     Path to login.json  (default: login.json next to this script).
    --output    Write raw JSON response to this file.
    --mark-read Mark all log entries as read after displaying them.

Exit codes:
    0 = success
    1 = API / network error
    2 = parameter validation error
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime
from typing import Any

from bryck_api import BryckApi, display_error, extract_error_info
from session import ApiSession

logger = logging.getLogger(__name__)

DEFAULT_LOGIN_JSON = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "login.json"
)

# Map CLI period names → API cursor values
PERIOD_CURSOR: dict[str, str] = {
    "today": "Today",
    "week": "This week",
    "month": "Last 30 days",
}

# Width of the terminal-like separator line
_SEP_WIDTH = 100


# =============================================================================
# Formatting helpers
# =============================================================================

def _fmt_timestamp(ts: Any) -> str:
    """Convert epoch float/int or ISO string to YYYY-MM-DD HH:MM:SS."""
    if ts is None:
        return "—"
    try:
        return datetime.fromtimestamp(float(ts)).strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, OSError, OverflowError):
        return str(ts)


def _priority_label(priority: Any) -> str:
    """Return a fixed-width, visually annotated priority label."""
    raw = str(priority).upper() if priority else "INFO"
    icons = {
        "ERROR":    "✗ ERROR  ",
        "CRITICAL": "✗ CRITICAL",
        "WARNING":  "⚠ WARNING",
        "INFO":     "  INFO   ",
        "DEBUG":    "  DEBUG  ",
    }
    for key, label in icons.items():
        if key in raw:
            return label
    return f"  {raw:<7}"


def _read_indicator(read_status: Any) -> str:
    """Return ● (unread) or ○ (read)."""
    val = str(read_status).lower() if read_status is not None else "true"
    if val in ("false", "0", "no", "unread"):
        return "●"
    return "○"


def _print_logs(entries: list[dict]) -> int:
    """Print formatted log table.  Returns count of unread entries."""
    sep = "─" * _SEP_WIDTH
    print(sep)
    unread = 0
    for entry in entries:
        log_id = entry.get("id", "?")
        timestamp = _fmt_timestamp(entry.get("timestamp"))
        priority = _priority_label(entry.get("priority"))
        message = entry.get("message", "")
        indicator = _read_indicator(entry.get("read_status"))
        if indicator == "●":
            unread += 1
        print(f"[{log_id:<6}]  {timestamp}   {indicator} {priority}  {message}")
    print(sep)
    total = len(entries)
    print(f"Total: {total} {'entry' if total == 1 else 'entries'}  |  Unread: {unread}")
    return unread


# =============================================================================
# Entry point
# =============================================================================

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Fetch and display Bryck system event logs.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--period",
        choices=["today", "week", "month"],
        default="today",
        help="Time range to fetch (default: today).",
    )
    parser.add_argument(
        "--login",
        default=DEFAULT_LOGIN_JSON,
        metavar="PATH",
        help="Path to login.json (default: %(default)s).",
    )
    parser.add_argument(
        "--output",
        metavar="PATH",
        help="Write raw JSON response to this file.",
    )
    parser.add_argument(
        "--mark-read",
        action="store_true",
        help="Mark all log entries as read after displaying them.",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable debug logging.",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    cursor = PERIOD_CURSOR[args.period]

    try:
        with ApiSession.from_login_json(args.login) as session:
            session.login()
            api = BryckApi(session)

            # Fetch logs
            resp = api.get_logs(cursor=cursor)
            if resp is None:
                print("Error: No response from server.", file=sys.stderr)
                return 1
            if not resp.ok:
                title, detail = extract_error_info(resp)
                display_error(
                    "Get Logs",
                    resp.status_code,
                    title,
                    detail,
                    f"/api/config/getlogs?cursor={cursor}",
                )
                return 1

            data = resp.json()
            # API may return a list directly or wrap it
            if isinstance(data, dict):
                entries: list[dict] = data.get("result", data.get("logs", []))
            elif isinstance(data, list):
                entries = data
            else:
                entries = []

            # Save raw JSON if requested
            if args.output:
                try:
                    with open(args.output, "w") as fh:
                        json.dump(data, fh, indent=2)
                    print(f"Raw JSON saved to: {args.output}")
                except OSError as exc:
                    print(f"Warning: could not write output file: {exc}", file=sys.stderr)

            if not entries:
                print(f"No log entries found for period: {args.period}.")
                return 0

            _print_logs(entries)

            # Mark all read if requested
            if args.mark_read:
                mark_resp = api.mark_logs_read(mark_all=True)
                if mark_resp is not None and mark_resp.ok:
                    print("All log entries marked as read.")
                else:
                    status = mark_resp.status_code if mark_resp is not None else "N/A"
                    print(
                        f"Warning: mark-read request returned status {status}.",
                        file=sys.stderr,
                    )

    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        return 130
    except Exception as exc:
        logger.debug("Unexpected error", exc_info=True)
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
