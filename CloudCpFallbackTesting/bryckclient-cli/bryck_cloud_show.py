#!/usr/bin/env python3
"""
Standalone Bryck cloud configuration list runner.

Flow:
    1. Load login.json from the current directory.
    2. Log in to the Bryck REST API.
    3. Call GET /api/bcloud/config_list and print the cloud configurations
       in a formatted multi-line layout with dividers.

Usage:
    python3 bryck_cloud_show.py [--login PATH] [--output PATH]
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

_MISSING = "-"


# =============================================================================
# Helpers
# =============================================================================

def _fmt_str(value: Any) -> str:
    """Stringify a scalar; return '-' for None/empty."""
    if value is None:
        return _MISSING
    s = str(value).strip()
    return s if s else _MISSING


def _fmt_timestamp(value: Any) -> str:
    """Format Unix timestamp as readable date; return '-' if invalid."""
    if value is None:
        return _MISSING
    try:
        ts = float(value)
        dt = datetime.fromtimestamp(ts)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, OSError):
        return _fmt_str(value)


def _summarize(entry: dict[str, Any]) -> str:
    """Format one cloud configuration entry with dividers and multi-line layout."""
    divider = "─" * 80
    cloud_type = _fmt_str(entry.get("bcloud_type") or entry.get("cloud_type")).upper()
    config_id = _fmt_str(entry.get("id"))
    username = _fmt_str(entry.get("username"))
    region = _fmt_str(entry.get("region"))
    timestamp = _fmt_timestamp(entry.get("timestamp"))
    
    # Build output with conditional region field (AWS only)
    output = (
        f"\n{divider}\n"
        f"  CLOUD_TYPE     : {cloud_type}\n"
        f"  CONFIG_ID      : {config_id}\n"
        f"  USERNAME       : {username}\n"
    )
    
    if region != _MISSING:
        output += f"  REGION         : {region}\n"
    
    output += (
        f"  CONFIGURED_AT  : {timestamp}\n"
        f"{divider}"
    )
    
    return output


def _get_config_id(entry: dict[str, Any]) -> int:
    """Extract numeric config ID for sorting; return 0 if missing/invalid."""
    config_id = entry.get("id")
    if isinstance(config_id, int):
        return config_id
    if isinstance(config_id, str):
        try:
            return int(config_id)
        except ValueError:
            return 0
    return 0


# =============================================================================
# Entry point
# =============================================================================

def main(argv: list[str] | None = None) -> int:
    """Main entry point for the cloud configuration list runner."""
    parser = argparse.ArgumentParser(
        description=(
            "Display cloud provider configurations from /api/bcloud/config_list. "
            "Shows all configured cloud providers (AWS, GCP, Azure) with their "
            "settings."
        ),
    )
    parser.add_argument(
        "--login",
        default=DEFAULT_LOGIN_JSON,
        help="Path to login.json (default: %(default)s)",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Optional file to write JSON to (in addition to stdout).",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    try:
        session = ApiSession.from_login_json(args.login)
    except Exception as exc:
        display_error(
            "Load Login",
            message=f"Failed to load {args.login}: {exc}"
        )
        return 2

    try:
        session.login()
        api = BryckApi(session)

        logger.info("Fetching /api/bcloud/config_list")
        resp = api.get_cloud_config_list()
        
        if resp is None:
            display_error(
                "Get Cloud Config List",
                message="Request failed (see logs for details)"
            )
            return 1
        
        if resp.status_code != 200:
            status_code, status_text, message = extract_error_info(resp)
            display_error(
                "Get Cloud Config List",
                status_code=status_code,
                status_text=status_text,
                message=message,
                endpoint="/api/bcloud/config_list"
            )
            return 1

        try:
            result = resp.json().get("result", [])
        except ValueError as exc:
            display_error(
                "Parse Response",
                message=f"Failed to parse JSON response: {exc}"
            )
            return 1

        if not result:
            print("No cloud providers configured.")
            return 0

        # Ensure result is a list
        if not isinstance(result, list):
            result = [result]

        # Sort by config ID for consistent output
        configs = sorted(result, key=_get_config_id)

        # Display formatted output
        for config in configs:
            print(_summarize(config))

        # Optional JSON output file
        if args.output:
            try:
                pretty = json.dumps(result, indent=2, sort_keys=True, default=str)
                with open(args.output, "w", encoding="utf-8") as fh:
                    fh.write(pretty)
                    fh.write("\n")
                logger.info("Wrote cloud config list to %s", args.output)
            except OSError as exc:
                display_error(
                    "Write Output",
                    message=f"Failed to write {args.output}: {exc}"
                )
                return 1

        return 0

    except KeyboardInterrupt:
        logger.warning("Interrupted by user")
        return 130
    except Exception as exc:
        display_error("Unexpected Error", message=str(exc))
        logger.exception("Unexpected error")
        return 1


if __name__ == "__main__":
    sys.exit(main())
