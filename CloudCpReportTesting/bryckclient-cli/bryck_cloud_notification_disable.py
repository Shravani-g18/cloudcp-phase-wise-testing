#!/usr/bin/env python3
"""Disable cloud transfer notifications.

Deactivates notification delivery while preserving configuration.
Use delete to remove configuration entirely.

Usage:
    python3 bryck_cloud_notification_disable.py
"""

import argparse
import json
import sys

from bryck_api import BryckApi, display_error
from cloud_notification import CloudNotification
from session import ApiSession


def _print_divider():
    """Print 80-char divider."""
    print("─" * 80)


def _fmt_str(value):
    """Format string, return '-' for None/empty."""
    return str(value) if value else "-"


def _fmt_bool(value):
    """Format boolean."""
    return "true" if value else "false"


def _fmt_states(states):
    """Format states array."""
    if not states:
        return "-"
    return ", ".join(states) if isinstance(states, list) else str(states)


def main():
    parser = argparse.ArgumentParser(
        description="Disable cloud transfer notifications"
    )
    parser.add_argument(
        "--login",
        default="login.json",
        help="Path to login.json (default: login.json)",
    )

    args = parser.parse_args()

    # Initialize session and API
    try:
        session = ApiSession.from_login_json(args.login)
    except FileNotFoundError:
        print(f"Error: {args.login} not found", file=sys.stderr)
        sys.exit(2)
    except (ValueError, json.JSONDecodeError) as e:
        print(f"Error loading {args.login}: {e}", file=sys.stderr)
        sys.exit(2)

    # Authenticate session
    try:
        session.login()
    except Exception as e:
        print(f"Error: Authentication failed: {e}", file=sys.stderr)
        sys.exit(1)

    api = BryckApi(session)
    notify = CloudNotification(api, session)

    # Disable notifications
    result = notify.disable()

    # Handle result
    if not result["success"]:
        display_error(
            operation="Cloud Notification Disable",
            status_code=result["error_details"].get("status_code"),
            status_text=result["error_details"].get("status_text"),
            message=result["error_details"].get("message"),
            endpoint=result["error_details"].get("endpoint"),
        )
        sys.exit(1)

    # Display success
    print(f"\n✓ {result['message']}\n")

    # Display configuration
    config = result["data"]
    _print_divider()
    print(f"SNS_TOPIC:      {_fmt_str(config.get('SNS_TOPIC_ARN'))}")
    print(f"SQS_QUEUE:      {_fmt_str(config.get('SQS_QUEUE_URL'))}")
    print(f"NOTIFY_STATES:  {_fmt_states(config.get('NOTIFY_STATES'))}")
    print(f"ENABLED:        {_fmt_bool(config.get('ENABLED', False))}")
    print(f"Note:           Configuration preserved for future use")
    _print_divider()
    print()

    sys.exit(0)


if __name__ == "__main__":
    main()
