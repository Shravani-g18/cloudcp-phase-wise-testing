"""Cloud notification management.

Orchestration layer for Bryck notification API. Provides validation, error
handling, and cross-validation logic for notification operations.

All methods return a consistent dict structure:
    {
        'success': bool,
        'message': str,           # User-friendly message
        'data': dict|list|None,   # Operation result (if successful)
        'error_details': dict|None  # Error info if failed
    }
"""

from asyncio.log import logger

from bryck_api import BryckApi, extract_error_info
from session import ApiSession


class CloudNotification:
    """Notification management for Bryck cloud transfers.
    
    Wraps notification API endpoints with validation, error handling, and
    cross-validation logic.
    """

    # Valid notification states
    VALID_STATES = {"COMPLETED", "FAILED", "PAUSED"}

    def __init__(self, api: BryckApi, session: ApiSession):
        """Initialize notification manager.
        
        Args:
            api: BryckApi instance for API calls.
            session: ApiSession instance for authentication.
        """
        self.api = api
        self.session = session

    def setup(
        self,
        sns_topic: str | None = None,
        sqs_queue: str | None = None,
        emails: list[str] | None = None,
        states: list[str] | None = None,
    ) -> dict:
        """Setup notification configuration.
        
        Args:
            sns_topic: AWS SNS topic ARN (optional if sqs_queue provided).
            sqs_queue: AWS SQS queue ARN (optional if sns_topic provided).
            emails: List of email addresses to notify (optional).
            states: List of transfer states to notify on (optional).
        
        Returns:
            Dict with success status, message, data, and error_details.
        """
        # Validate: at least one of sns_topic or sqs_queue
        if not sns_topic and not sqs_queue:
            return {
                "success": False,
                "message": "At least one of sns_topic or sqs_queue must be provided",
                "data": None,
                "error_details": {
                    "type": "validation",
                    "message": "Missing required parameter: sns_topic or sqs_queue",
                },
            }

        # Note: emails are optional - can be added later via subscribe

        # Validate: states array contains only valid states
        if states:
            invalid_states = set(states) - self.VALID_STATES
            if invalid_states:
                return {
                    "success": False,
                    "message": f"Invalid states: {', '.join(invalid_states)}. Valid states: {', '.join(self.VALID_STATES)}",
                    "data": None,
                    "error_details": {
                        "type": "validation",
                        "message": f"Invalid states: {invalid_states}",
                    },
                }

        # Call API
        resp = self.api.notification_setup(sns_topic, sqs_queue, emails, states)
        logger.info("Notification setup response: %s", resp.json())
        if resp is None:
            return {
                "success": False,
                "message": "Connection error or invalid login",
                "data": None,
                "error_details": {
                    "type": "connection",
                    "status_code": None,
                    "status_text": "",
                    "message": "Connection error",
                    "endpoint": "/api/bcloud/notification_setup",
                },
            }

        if resp.status_code != 200:
            status_code, status_text, message = extract_error_info(resp)
            return {
                "success": False,
                "message": f"Setup failed: {message}",
                "data": None,
                "error_details": {
                    "status_code": status_code,
                    "status_text": status_text,
                    "message": message,
                    "endpoint": "/api/bcloud/notification_setup",
                },
            }

        # Get setup response data
        setup_data = resp.json().get("result", {})
        
        # Cross-validate with notification_list
        validate_resp = self.api.notification_list()
        if validate_resp is None or validate_resp.status_code != 200:
            return {
                "success": False,
                "message": "Setup completed but validation failed",
                "data": setup_data,
                "error_details": {
                    "type": "validation",
                    "message": "Could not verify notification setup",
                    "endpoint": "/api/bcloud/notification_list",
                },
            }

        return {
            "success": True,
            "message": "Notification setup completed successfully",
            "data": setup_data,
            "error_details": None,
        }

    def get_config(self) -> dict:
        """Get notification configuration.
        
        Returns:
            Dict with success status, message, data (config), and error_details.
        """
        resp = self.api.notification_list()
        if resp is None:
            return {
                "success": False,
                "message": "Connection error or invalid login",
                "data": None,
                "error_details": {
                    "type": "connection",
                    "status_code": None,
                    "status_text": "",
                    "message": "Connection error",
                    "endpoint": "/api/bcloud/notification_list",
                },
            }

        if resp.status_code != 200:
            status_code, status_text, message = extract_error_info(resp)
            return {
                "success": False,
                "message": f"Failed to retrieve notification configuration: {message}",
                "data": None,
                "error_details": {
                    "status_code": status_code,
                    "status_text": status_text,
                    "message": message,
                    "endpoint": "/api/bcloud/notification_list",
                },
            }

        config = resp.json().get("result", {})
        return {
            "success": True,
            "message": "Notification configuration retrieved successfully",
            "data": config,
            "error_details": None,
        }

    def subscribe(self, emails: list[str]) -> dict:
        """Subscribe emails to notifications.
        
        Args:
            emails: List of email addresses to subscribe.
        
        Returns:
            Dict with success status, message, data (subscriber list), and error_details.
        """
        # Validate: emails not empty
        if not emails:
            return {
                "success": False,
                "message": "At least one email address must be provided",
                "data": None,
                "error_details": {
                    "type": "validation",
                    "message": "Empty email list",
                },
            }

        # Call API
        resp = self.api.notification_subscribe(emails)
        if resp is None:
            return {
                "success": False,
                "message": "Connection error or invalid login",
                "data": None,
                "error_details": {
                    "type": "connection",
                    "status_code": None,
                    "status_text": "",
                    "message": "Connection error",
                    "endpoint": "/api/bcloud/notification_subscribe",
                },
            }

        if resp.status_code != 200:
            status_code, status_text, message = extract_error_info(resp)
            return {
                "success": False,
                "message": f"Subscription failed: {message}",
                "data": None,
                "error_details": {
                    "status_code": status_code,
                    "status_text": status_text,
                    "message": message,
                    "endpoint": "/api/bcloud/notification_subscribe",
                },
            }

        # Cross-validate with notification_subscribers
        validate_resp = self.api.notification_subscribers()
        if validate_resp is None or validate_resp.status_code != 200:
            return {
                "success": False,
                "message": "Subscription completed but validation failed",
                "data": None,
                "error_details": {
                    "type": "validation",
                    "message": "Could not verify subscription",
                    "endpoint": "/api/bcloud/notification_subscribers",
                },
            }

        subscribers = validate_resp.json().get("result", [])
        return {
            "success": True,
            "message": "Email(s) subscribed successfully",
            "data": subscribers,
            "error_details": None,
        }

    def unsubscribe(self, email: str) -> dict:
        """Unsubscribe email from notifications.
        
        Args:
            email: Email address to unsubscribe.
        
        Returns:
            Dict with success status, message, data (subscriber list), and error_details.
        """
        # Validate: email not empty
        if not email:
            return {
                "success": False,
                "message": "Email address must be provided",
                "data": None,
                "error_details": {
                    "type": "validation",
                    "message": "Empty email",
                },
            }

        # Call API
        resp = self.api.notification_unsubscribe(email)
        if resp is None:
            return {
                "success": False,
                "message": "Connection error or invalid login",
                "data": None,
                "error_details": {
                    "type": "connection",
                    "status_code": None,
                    "status_text": "",
                    "message": "Connection error",
                    "endpoint": "/api/bcloud/notification_unsubscribe",
                },
            }

        if resp.status_code != 200:
            status_code, status_text, message = extract_error_info(resp)
            return {
                "success": False,
                "message": f"Unsubscription failed: {message}",
                "data": None,
                "error_details": {
                    "status_code": status_code,
                    "status_text": status_text,
                    "message": message,
                    "endpoint": "/api/bcloud/notification_unsubscribe",
                },
            }

        # Cross-validate with notification_subscribers
        validate_resp = self.api.notification_subscribers()
        if validate_resp is None or validate_resp.status_code != 200:
            return {
                "success": False,
                "message": "Unsubscription completed but validation failed",
                "data": None,
                "error_details": {
                    "type": "validation",
                    "message": "Could not verify unsubscription",
                    "endpoint": "/api/bcloud/notification_subscribers",
                },
            }

        subscribers = validate_resp.json().get("result", [])
        return {
            "success": True,
            "message": "Email unsubscribed successfully",
            "data": subscribers,
            "error_details": None,
        }

    def subscribers(self) -> dict:
        """Get list of subscribers.
        
        Returns:
            Dict with success status, message, data (subscriber list), and error_details.
        """
        resp = self.api.notification_subscribers()
        if resp is None:
            return {
                "success": False,
                "message": "Connection error or invalid login",
                "data": None,
                "error_details": {
                    "type": "connection",
                    "status_code": None,
                    "status_text": "",
                    "message": "Connection error",
                    "endpoint": "/api/bcloud/notification_subscribers",
                },
            }

        if resp.status_code != 200:
            status_code, status_text, message = extract_error_info(resp)
            return {
                "success": False,
                "message": f"Failed to retrieve subscribers: {message}",
                "data": None,
                "error_details": {
                    "status_code": status_code,
                    "status_text": status_text,
                    "message": message,
                    "endpoint": "/api/bcloud/notification_subscribers",
                },
            }

        subscribers = resp.json().get("result", [])
        return {
            "success": True,
            "message": "Subscribers retrieved successfully",
            "data": subscribers,
            "error_details": None,
        }

    def test(
        self,
        transfer_id: str | None = None,
        state: str | None = None,
        message: str | None = None,
    ) -> dict:
        """Send test notification.
        
        Args:
            transfer_id: Transfer ID for test (optional if state provided).
            state: Transfer state to test (optional if transfer_id provided).
            message: Custom message for test (optional).
        
        Returns:
            Dict with success status, message, data, and error_details.
        """
        # Validate: transfer_id or state required
        if not transfer_id and not state:
            return {
                "success": False,
                "message": "Either transfer_id or state must be provided",
                "data": None,
                "error_details": {
                    "type": "validation",
                    "message": "Missing required parameters: transfer_id or state",
                },
            }

        # Validate: state must be valid if provided
        if state and state not in self.VALID_STATES:
            return {
                "success": False,
                "message": f"Invalid state: {state}. Valid states: {', '.join(self.VALID_STATES)}",
                "data": None,
                "error_details": {
                    "type": "validation",
                    "message": f"Invalid state: {state}",
                },
            }

        # Call API
        resp = self.api.notification_test(transfer_id, state, message)
        if resp is None:
            return {
                "success": False,
                "message": "Connection error or invalid login",
                "data": None,
                "error_details": {
                    "type": "connection",
                    "status_code": None,
                    "status_text": "",
                    "message": "Connection error",
                    "endpoint": "/api/bcloud/notification_test",
                },
            }

        if resp.status_code != 200:
            status_code, status_text, message_err = extract_error_info(resp)
            return {
                "success": False,
                "message": f"Test notification failed: {message_err}",
                "data": None,
                "error_details": {
                    "status_code": status_code,
                    "status_text": status_text,
                    "message": message_err,
                    "endpoint": "/api/bcloud/notification_test",
                },
            }

        result = resp.json().get("result", {})
        return {
            "success": True,
            "message": "Test notification sent successfully",
            "data": result,
            "error_details": None,
        }

    def enable(self) -> dict:
        """Enable notifications.
        
        Returns:
            Dict with success status, message, data (config), and error_details.
        """
        # Call API
        resp = self.api.notification_enable()
        if resp is None:
            return {
                "success": False,
                "message": "Connection error or invalid login",
                "data": None,
                "error_details": {
                    "type": "connection",
                    "status_code": None,
                    "status_text": "",
                    "message": "Connection error",
                    "endpoint": "/api/bcloud/notification_enable",
                },
            }

        if resp.status_code != 200:
            status_code, status_text, message = extract_error_info(resp)
            return {
                "success": False,
                "message": f"Enable failed: {message}",
                "data": None,
                "error_details": {
                    "status_code": status_code,
                    "status_text": status_text,
                    "message": message,
                    "endpoint": "/api/bcloud/notification_enable",
                },
            }

        # Cross-validate with notification_list
        validate_resp = self.api.notification_list()
        if validate_resp is None or validate_resp.status_code != 200:
            return {
                "success": False,
                "message": "Enable completed but validation failed",
                "data": None,
                "error_details": {
                    "type": "validation",
                    "message": "Could not verify enable",
                    "endpoint": "/api/bcloud/notification_list",
                },
            }

        config = validate_resp.json().get("result", {})
        return {
            "success": True,
            "message": "Notifications enabled successfully",
            "data": config,
            "error_details": None,
        }

    def disable(self) -> dict:
        """Disable notifications (preserve configuration).
        
        Returns:
            Dict with success status, message, data (config), and error_details.
        """
        # Call API
        resp = self.api.notification_disable()
        if resp is None:
            return {
                "success": False,
                "message": "Connection error or invalid login",
                "data": None,
                "error_details": {
                    "type": "connection",
                    "status_code": None,
                    "status_text": "",
                    "message": "Connection error",
                    "endpoint": "/api/bcloud/notification_disable",
                },
            }

        if resp.status_code != 200:
            status_code, status_text, message = extract_error_info(resp)
            return {
                "success": False,
                "message": f"Disable failed: {message}",
                "data": None,
                "error_details": {
                    "status_code": status_code,
                    "status_text": status_text,
                    "message": message,
                    "endpoint": "/api/bcloud/notification_disable",
                },
            }

        # Cross-validate with notification_list
        validate_resp = self.api.notification_list()
        if validate_resp is None or validate_resp.status_code != 200:
            return {
                "success": False,
                "message": "Disable completed but validation failed",
                "data": None,
                "error_details": {
                    "type": "validation",
                    "message": "Could not verify disable",
                    "endpoint": "/api/bcloud/notification_list",
                },
            }

        config = validate_resp.json().get("result", {})
        return {
            "success": True,
            "message": "Notifications disabled successfully (configuration preserved)",
            "data": config,
            "error_details": None,
        }

    def delete(self, force: bool = False) -> dict:
        """Delete notification configuration.
        
        Args:
            force: If True, skip confirmation prompt.
        
        Returns:
            Dict with success status, message, data, and error_details.
        """
        # Optional: Prompt for confirmation if not forced
        if not force:
            import sys
            try:
                confirm = input(
                    "Delete notification configuration? This action cannot be undone. (yes/no): "
                )
                if confirm.lower() not in ("yes", "y"):
                    return {
                        "success": False,
                        "message": "Deletion cancelled by user",
                        "data": None,
                        "error_details": {"type": "user_cancelled"},
                    }
            except (EOFError, KeyboardInterrupt):
                return {
                    "success": False,
                    "message": "Deletion cancelled by user",
                    "data": None,
                    "error_details": {"type": "user_cancelled"},
                }

        # Call API
        resp = self.api.notification_delete()
        if resp is None:
            return {
                "success": False,
                "message": "Connection error or invalid login",
                "data": None,
                "error_details": {
                    "type": "connection",
                    "status_code": None,
                    "status_text": "",
                    "message": "Connection error",
                    "endpoint": "/api/bcloud/notification_delete",
                },
            }

        if resp.status_code != 200:
            status_code, status_text, message = extract_error_info(resp)
            return {
                "success": False,
                "message": f"Delete failed: {message}",
                "data": None,
                "error_details": {
                    "status_code": status_code,
                    "status_text": status_text,
                    "message": message,
                    "endpoint": "/api/bcloud/notification_delete",
                },
            }

        # Cross-validate with notification_list (should be empty)
        validate_resp = self.api.notification_list()
        if validate_resp is None or validate_resp.status_code != 200:
            return {
                "success": False,
                "message": "Delete completed but validation failed",
                "data": None,
                "error_details": {
                    "type": "validation",
                    "message": "Could not verify deletion",
                    "endpoint": "/api/bcloud/notification_list",
                },
            }

        config = validate_resp.json().get("result", {})
        return {
            "success": True,
            "message": "Notification configuration deleted successfully",
            "data": config,
            "error_details": None,
        }
