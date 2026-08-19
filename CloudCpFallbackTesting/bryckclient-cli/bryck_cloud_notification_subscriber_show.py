#!/usr/bin/env python3
"""Display list of cloud transfer notification subscribers.

Shows all email addresses currently subscribed to notifications.

Usage:
    python3 bryck_cloud_notification_subscriber_show.py
    python3 bryck_cloud_notification_subscriber_show.py --output /path/to/subscribers.json
"""

import argparse
import json
import sys
from datetime import datetime

from bryck_api import BryckApi, display_error
from cloud_notification import CloudNotification
from session import ApiSession


def _print_divider():
    """Print 80-char divider."""
    print("─" * 80)


def _fmt_str(value):
    """Format string, return '-' for None/empty."""
    return str(value) if value else "-"


def _fmt_timestamp(unix_ts):
    """Convert Unix timestamp to YYYY-MM-DD HH:MM:SS."""
    if not unix_ts:
        return "-"
    try:
        return datetime.fromtimestamp(unix_ts).strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        return "-"


def main():
    parser = argparse.ArgumentParser(
        description="Display list of cloud transfer notification subscribers"
    )
    parser.add_argument(
        "--output",
        help="Optional path to save raw JSON output",
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

    # Get subscribers
    result = notify.subscribers()

    # Handle result
    if not result["success"]:
        display_error(
            operation="Cloud Notification Subscriber Show",
            status_code=result["error_details"].get("status_code"),
            status_text=result["error_details"].get("status_text"),
            message=result["error_details"].get("message"),
            endpoint=result["error_details"].get("endpoint"),
        )
        sys.exit(1)

    # Save JSON output if requested
    if args.output:
        try:
            with open(args.output, "w") as f:
                json.dump(result["data"], f, indent=2)
            print(f"Subscriber list saved to {args.output}\n")
        except IOError as e:
            print(f"Error writing to {args.output}: {e}", file=sys.stderr)
            sys.exit(1)

    # Display subscriber list
    subscribers = result["data"]
    if not subscribers:
        print("No subscribers found\n")
        sys.exit(0)

    _print_divider()
    print(f"{'EMAIL':<40} {'STATUS':<25} {'SUBSCRIPTION_ARN':<50}")
    _print_divider()

    for sub in sorted(subscribers, key=lambda x: x.get("endpoint", "")):
        email = _fmt_str(sub.get("endpoint"))
        status = _fmt_str(sub.get("status"))
        sub_arn = _fmt_str(sub.get("subscription_arn"))
        print(f"{email:<40} {status:<25} {sub_arn:<50}")

    _print_divider()
    print()

    sys.exit(0)


if __name__ == "__main__":
    main()
