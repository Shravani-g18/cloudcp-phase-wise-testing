#!/usr/bin/env python3
"""Delete cloud transfer notification configuration.

Removes all notification configuration and deactivates notifications.
This action cannot be undone without reconfiguring.

Usage:
    python3 bryck_cloud_notification_delete.py
    python3 bryck_cloud_notification_delete.py --force
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


def main():
    parser = argparse.ArgumentParser(
        description="Delete cloud transfer notification configuration"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Skip confirmation prompt",
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

    # Delete notification configuration
    result = notify.delete(force=args.force)

    # Handle result
    if not result["success"]:
        # Check if it was cancelled by user
        if result["error_details"].get("type") == "user_cancelled":
            print(f"{result['message']}\n")
            sys.exit(0)
        
        display_error(
            operation="Cloud Notification Delete",
            status_code=result["error_details"].get("status_code"),
            status_text=result["error_details"].get("status_text"),
            message=result["error_details"].get("message"),
            endpoint=result["error_details"].get("endpoint"),
        )
        sys.exit(1)

    # Display success
    print(f"\n✓ {result['message']}\n")
    _print_divider()
    print("Notification configuration has been deleted.")
    print("To reconfigure, use: python3 bryck_cloud_notification_setup.py")
    _print_divider()
    print()

    sys.exit(0)


if __name__ == "__main__":
    main()
