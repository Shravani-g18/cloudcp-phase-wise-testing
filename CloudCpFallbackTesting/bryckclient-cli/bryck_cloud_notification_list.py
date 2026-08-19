#!/usr/bin/env python3
"""Display cloud transfer notification configuration.

Shows the current notification settings including enabled status,
SNS/SQS configuration, and notification states.

Usage:
    python3 bryck_cloud_notification_list.py
    python3 bryck_cloud_notification_list.py --output /path/to/config.json
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
        description="Display cloud transfer notification configuration"
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

    # Get configuration
    result = notify.get_config()

    # Handle result
    if not result["success"]:
        display_error(
            operation="Cloud Notification List",
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
            print(f"Configuration saved to {args.output}\n")
        except IOError as e:
            print(f"Error writing to {args.output}: {e}", file=sys.stderr)
            sys.exit(1)

    # Display configuration
    config = result["data"]
    if not config:
        print("No notification configuration found\n")
        sys.exit(0)

    _print_divider()
    print(f"SNS_TOPIC:      {_fmt_str(config.get('SNS_TOPIC_ARN'))}")
    print(f"SQS_QUEUE:      {_fmt_str(config.get('SQS_QUEUE_URL'))}")
    print(f"NOTIFY_STATES:  {_fmt_states(config.get('NOTIFY_STATES'))}")
    print(f"ENABLED:        {_fmt_bool(config.get('ENABLED', False))}")
    _print_divider()
    print()

    sys.exit(0)


if __name__ == "__main__":
    main()
