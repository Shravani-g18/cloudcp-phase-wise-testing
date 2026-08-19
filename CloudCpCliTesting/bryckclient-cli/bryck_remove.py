#!/usr/bin/env python3
"""
Standalone Bryck remove runner.

Preconditions:
    - result.bryck_info.State must equal " Ejected" (leading space).

Flow:
    1. Log in to the Bryck REST API.
    2. Read bryck_info.State; abort unless " Ejected".
    3. Call /api/config/remove on the single store UUID.
    4. Poll until State becomes " Removed" or the UUID disappears
       from result.logical_cards.

Usage:
    python3 bryck_remove.py [--login PATH]
"""
from __future__ import annotations

import argparse
import logging
import os
import sys

from bryck_api import BryckApi, ticker, display_error, extract_error_info
from session import ApiSession

logger = logging.getLogger(__name__)

DEFAULT_LOGIN_JSON = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "login.json"
)

REMOVE_TIMEOUT = 300

STATE_EJECTED = " Ejected"
STATE_REMOVED = " Removed"


# =============================================================================
# Helpers / validators
# =============================================================================

def _bryck_state(api: BryckApi) -> str:
    """Return current result.bryck_info.State (empty string if missing)."""
    sys_info = api.bryck_info() or {}
    return sys_info.get("bryck_info", {}).get("State", "")


def _pick_store_uuid(api: BryckApi) -> str:
    """Return the first (and only) key of result.logical_cards."""
    sys_info = api.bryck_info() or {}
    lcs = sys_info.get("logical_cards", {})
    if not lcs:
        raise RuntimeError("No logical cards reported by /api/config/info")
    uuid = next(iter(lcs))
    logger.info("Selected store UUID: %s", uuid)
    return uuid


def _validate_remove(api: BryckApi, target_uuid: str) -> bool:
    """Poll callback: True when the LC is removed or state==' Removed'."""
    sys_info = api.bryck_info() or {}
    state = sys_info.get("bryck_info", {}).get("State", "")
    lcs = sys_info.get("logical_cards", {})
    if state == STATE_REMOVED:
        return True
    if target_uuid not in lcs:
        logger.info("Store UUID %s no longer present in logical_cards", target_uuid)
        return True
    return False


# =============================================================================
# Entry point
# =============================================================================

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Remove the Bryck from the server.")
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

        state = _bryck_state(api)
        logger.info("Current bryck state: %r", state)

        # ---- State precondition: must be "Ejected" to remove -----------
        if state != STATE_EJECTED:
            if state == STATE_REMOVED:
                display_error(
                    "Remove Bryck",
                    message="Bryck is already in 'Removed' state. Nothing to remove."
                )
            else:
                display_error(
                    "Remove Bryck",
                    message=f"Bryck is in '{state}' state. Must eject first before removing."
                )
            return 2

        store_uuid = _pick_store_uuid(api)
        logger.info("Initiating remove on UUID %s", store_uuid)
        resp = api.remove(store_uuid)
        if resp is None:
            display_error("Remove Bryck", message="Request failed (see logs for details)")
            return 1
        
        # Check API response for errors
        if resp.status_code != 200:
            status_code, status_text, message = extract_error_info(resp)
            display_error("Remove Bryck", status_code, status_text, message, "/api/config/remove")
            return 1
        
        try:
            data = resp.json()
            if not data.get("success", False):
                error = data.get("error", {})
                message = error.get("message", "Unknown error") if isinstance(error, dict) else str(error)
                display_error("Remove Bryck", message=message)
                return 1
        except Exception as e:
            logger.debug("Failed to parse remove response: %s", e)
        logger.info("Remove initiated, validating (timeout=%ds)", REMOVE_TIMEOUT)
        try:
            ticker(lambda: _validate_remove(api, store_uuid), REMOVE_TIMEOUT, message="Removing store")
        except TimeoutError as exc:
            final_state = _bryck_state(api)
            logger.error(
                "Remove validation FAILED after %ds "
                "(expected changes did not happen; last state=%r): %s",
                REMOVE_TIMEOUT, final_state, exc,
            )
            return 3
        logger.info("Remove validated")
        return 0
    finally:
        session.close()


if __name__ == "__main__":
    sys.exit(main())
