#!/usr/bin/env python3
"""Setup cloud notification configuration.

Loads notification parameters from cloud_ops.json and configures
notification delivery for cloud transfers via SNS/SQS.

Usage:
    python3 bryck_cloud_notification_setup.py
    python3 bryck_cloud_notification_setup.py --params /path/to/cloud_ops.json

Parameters (from cloud_ops.json):
    notification.sns_topic: AWS SNS topic ARN (optional if sqs_queue provided)
    notification.sqs_queue: AWS SQS queue ARN (optional if sns_topic provided)
    notification.emails: List of email addresses (optional)
    notification.states: List of states to notify on (optional, default: all)
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


def _load_cloud_ops(params_path: str) -> dict:
    """Load cloud_ops.json and extract notification parameters.
    
    Args:
        params_path: Path to cloud_ops.json.
    
    Returns:
        Dict with notification parameters.
    """
    try:
        with open(params_path, "r") as f:
            config = json.load(f)
        notification = config.get("notification", {})
        return notification
    except FileNotFoundError:
        print(f"Error: {params_path} not found", file=sys.stderr)
        sys.exit(2)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in {params_path}: {e}", file=sys.stderr)
        sys.exit(2)


def main():
    parser = argparse.ArgumentParser(
        description="Setup cloud notification configuration"
    )
    parser.add_argument(
        "--params",
        default="cloud_ops.json",
        help="Path to cloud_ops.json (default: cloud_ops.json)",
    )
    parser.add_argument(
        "--login",
        default="login.json",
        help="Path to login.json (default: login.json)",
    )

    args = parser.parse_args()

    # Load parameters
    params = _load_cloud_ops(args.params)
    sns_topic = params.get("sns_topic")
    sqs_queue = params.get("sqs_queue")
    emails = params.get("emails")
    states = params.get("states")

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

    # Setup notification
    result = notify.setup(sns_topic, sqs_queue, emails, states)
    
    #print("Notification setup result: ", result)

    # Handle result
    if not result["success"]:
        display_error(
            operation="Cloud Notification Setup",
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
    print(f"SNS_TOPIC:      {_fmt_str(config.get('TopicArn'))}")
    print(f"SQS_QUEUE:      {_fmt_str(config.get('QueueUrl'))}")
    
    # Display subscriptions
    subscriptions = config.get('Subscriptions', [])
    if subscriptions:
        print(f"SUBSCRIPTIONS:  {len(subscriptions)} email(s)")
        for sub in subscriptions:
            status = sub.get('subscription_arn', 'pending')
            if status == 'pending confirmation':
                status = '(pending confirmation)'
            print(f"  - {sub.get('endpoint')} {status}")
    
    _print_divider()
    print()

    sys.exit(0)


if __name__ == "__main__":
    main()
