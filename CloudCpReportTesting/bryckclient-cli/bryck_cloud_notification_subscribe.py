#!/usr/bin/env python3
"""Subscribe emails to cloud transfer notifications.

Adds email addresses to the notification subscriber list.

Usage:
    python3 bryck_cloud_notification_subscribe.py --email user@example.com
    python3 bryck_cloud_notification_subscribe.py --email user1@example.com --email user2@example.com
    python3 bryck_cloud_notification_subscribe.py --emails user1@example.com,user2@example.com

Parameters:
    --email EMAIL: Email address to subscribe (repeatable)
    --emails EMAILS: Comma-separated email addresses
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
        description="Subscribe emails to cloud transfer notifications"
    )
    parser.add_argument(
        "--email",
        action="append",
        dest="emails",
        help="Email address to subscribe (repeatable)",
    )
    parser.add_argument(
        "--emails",
        dest="emails_csv",
        help="Comma-separated email addresses to subscribe",
    )
    parser.add_argument(
        "--login",
        default="login.json",
        help="Path to login.json (default: login.json)",
    )

    args = parser.parse_args()

    # Collect emails from both --email and --emails arguments
    emails = []
    if args.emails:
        # --email with action="append" creates a list
        emails.extend(args.emails)
    if args.emails_csv:
        # --emails argument (comma-separated string)
        emails.extend([e.strip() for e in args.emails_csv.split(",")])

    # Remove duplicates and empty strings
    emails = list(set(e for e in emails if e))

    # Validate
    if not emails:
        print("Error: At least one email address must be provided", file=sys.stderr)
        sys.exit(2)

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

    # Subscribe
    result = notify.subscribe(emails)

    # Handle result
    if not result["success"]:
        display_error(
            operation="Cloud Notification Subscribe",
            status_code=result["error_details"].get("status_code"),
            status_text=result["error_details"].get("status_text"),
            message=result["error_details"].get("message"),
            endpoint=result["error_details"].get("endpoint"),
        )
        sys.exit(1)

    # Display success
    print(f"\n✓ {result['message']}\n")

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
