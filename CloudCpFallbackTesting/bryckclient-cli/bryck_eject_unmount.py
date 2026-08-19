#!/usr/bin/env python3
"""
Standalone Bryck eject runner.

Preconditions:
    - result.bryck_info.State must equal " Mounted" (leading space).

Flow:
    1. Log in to the Bryck REST API.
    2. Read bryck_info.State; abort unless " Mounted".
    3. Call /api/config/eject on the single store UUID.
    4. Poll until State becomes " Ejected" or " Removed" (a Bryck
       physically pulled during the eject window jumps straight to
       " Removed", which is also treated as a successful eject).

Usage:
    python3 bryck_eject_unmount.py [--login PATH]
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

EJECT_TIMEOUT = 600

STATE_MOUNTED = " Mounted"
STATE_EJECTED = " Ejected"
STATE_REMOVED = " Removed"

# States that satisfy the eject validator — either the store finished
# ejecting (" Ejected") or it was pulled from the tray mid-flight and is
# now " Removed"; both mean the eject has landed from the runner's
# point of view.
STATE_EJECT_TERMINAL = (STATE_EJECTED, STATE_REMOVED)


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


def _validate_eject(api: BryckApi) -> bool:
    """Poll callback: True once ``/api/config/info`` reports both:

        result.bryck_info.State  in (" Ejected", " Removed")
        result.bryck_info.Erased == " False"                    (leading space)

    ``" Removed"`` is accepted alongside ``" Ejected"`` because a Bryck
    that gets physically pulled during the eject window transitions
    straight to ``" Removed"`` — from the runner's perspective the
    eject has still landed.

    Anything else (missing keys, transient errors, in-progress states)
    keeps the poll running.
    """
    try:
        sys_info = api.bryck_info()
    except Exception:
        logger.debug("bryck_info() raised during eject validation", exc_info=True)
        return False
    if not sys_info:
        return False

    bryck_info = sys_info.get("bryck_info", {})
    state = bryck_info.get("State", "")
    erased = bryck_info.get("Erased", "")

    logger.debug(
        "Eject validation: State=%r Erased=%r", state, erased,
    )

    if state in STATE_EJECT_TERMINAL and erased == " False":
        logger.info(
            "Eject complete: State=%r Erased=%r", state, erased,
        )
        return True
    return False


# =============================================================================
# Entry point
# =============================================================================

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Eject the Bryck.")
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

        # ---- Check for invalid states: Mounting is not allowed -----------
        if state == " Mounting":
            display_error(
                "Eject Bryck",
                message="Cannot eject the Bryck. Bryck is currently mounting."
            )
            return 2

        # ---- State precondition: must be "Mounted" to eject -----------
        if state != STATE_MOUNTED:
            if state == STATE_EJECTED:
                display_error(
                    "Eject Bryck",
                    message="Bryck is already ejected. Nothing to eject."
                )
            elif state == STATE_REMOVED:
                display_error(
                    "Eject Bryck",
                    message="Bryck is in 'Removed' state. Cannot eject."
                )
            else:
                display_error(
                    "Eject Bryck",
                    message=f"Bryck is in '{state}' state. Must be mounted to eject."
                )
            return 2

        store_uuid = _pick_store_uuid(api)
        logger.info("Initiating eject on UUID %s", store_uuid)
        resp = api.eject(store_uuid)
        if resp is None:
            display_error("Eject Bryck", message="Request failed (see logs for details)")
            return 1
        
        # Check API response for errors
        if resp.status_code != 200:
            status_code, status_text, message = extract_error_info(resp)
            display_error("Eject Bryck", status_code, status_text, message, "/api/config/eject")
            return 1
        
        try:
            data = resp.json()
            if not data.get("success", False):
                error = data.get("error", {})
                message = error.get("message", "Unknown error") if isinstance(error, dict) else str(error)
                display_error("Eject Bryck", message=message)
                return 1
        except Exception as e:
            logger.debug("Failed to parse eject response: %s", e)
        logger.info("Eject initiated, validating (timeout=%ds)", EJECT_TIMEOUT)
        try:
            ticker(lambda: _validate_eject(api), EJECT_TIMEOUT, message="Ejecting bryck")
        except TimeoutError as exc:
            final_state = _bryck_state(api)
            logger.error(
                "Eject validation FAILED after %ds "
                "(expected changes did not happen; last state=%r): %s",
                EJECT_TIMEOUT, final_state, exc,
            )
            return 3
        logger.info("Eject validated (state=%r)", _bryck_state(api))
        return 0
    finally:
        session.close()


if __name__ == "__main__":
    sys.exit(main())
