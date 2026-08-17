#!/usr/bin/env python3
"""
Standalone Bryck erase runner.

Preconditions:
    - result.bryck_info.State must equal " Ejected" (leading space).

Flow:
    1. Log in to the Bryck REST API.
    2. Read bryck_info.State; abort unless " Ejected".
    3. Scan the store UUID and poll until scan validates.
    4. Call /api/config/reset_store (erase) on the single store UUID.
    5. Poll until bryck_info.Erased == 'True' (or the store_type clears).

Usage:
    python3 bryck_erase.py [--login PATH]
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys

from bryck_api import BryckApi, ticker, display_error, extract_error_info
from session import ApiSession
from ssh_runner import SshRunner

logger = logging.getLogger(__name__)

DEFAULT_LOGIN_JSON = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "login.json"
)

SCAN_TIMEOUT = 180
ERASE_TIMEOUT = 600

STATE_EJECTED = " Ejected"
STATE_REMOVED = " Removed"

REMOTE_BRYCKUTIL = "/opt/bryck/.venv/bryck/bin/bryckutil"


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


def _validate_scan(api: BryckApi, ssh: SshRunner) -> bool:
    """Poll callback: True when the scan looks complete."""
    try:
        sys_info = api.bryck_info()
    except Exception:
        logger.debug("bryck_info() raised during scan validation", exc_info=True)
        return False
    if not sys_info:
        return False

    lcs = sys_info.get("logical_cards", {})
    bryck_state = sys_info.get("bryck_info", {}).get("State", "")

    for _lc_id, lc in lcs.items():
        store_status = lc.get("current_conditions", {}).get("store", {}).get("status")
        if store_status == "UP" and bryck_state != " Removed":
            for _ in range(2):
                rc, stdout, _ = ssh.run(
                    f"{REMOTE_BRYCKUTIL} --json bryck list", timeout=15
                )
                if rc == 0 and stdout.strip():
                    try:
                        payload = json.loads(stdout)
                    except json.JSONDecodeError:
                        payload = {}
                    if len(payload.get("device-list", [])) > 0:
                        return True
            return False
    return False


def _validate_erase(api: BryckApi, target_uuid: str) -> bool:
    """Poll callback: True only when BOTH of these are reported by
    ``/api/config/info``:

        result.bryck_info.State  == " Ejected"   (leading space)
        result.bryck_info.Erased == " True"      (leading space)

    Anything else (missing keys, ``" False"``, in-progress states,
    transient errors) keeps the poll running.
    """
    try:
        sys_info = api.bryck_info()
    except Exception:
        logger.debug("bryck_info() raised during erase validation", exc_info=True)
        return False
    if not sys_info:
        return False

    bryck_info = sys_info.get("bryck_info", {})
    state = bryck_info.get("State", "")
    erased = bryck_info.get("Erased", "")

    logger.debug(
        "Erase validation: uuid=%s State=%r Erased=%r",
        target_uuid, state, erased,
    )

    if state == STATE_EJECTED and erased == " True":
        logger.info(
            "Erase complete: State=%r Erased=%r", state, erased,
        )
        return True
    return False


# =============================================================================
# Entry point
# =============================================================================

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Erase the Bryck (reset_store).")
    parser.add_argument("--login", default=DEFAULT_LOGIN_JSON)
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    session = ApiSession.from_login_json(args.login)
    ssh = SshRunner.from_session(session)
    try:
        session.login()
        ssh.connect()
        api = BryckApi(session)

        state = _bryck_state(api)
        logger.info("Current bryck state: %r", state)

        store_uuid = _pick_store_uuid(api)

        if state == STATE_REMOVED:
            logger.info("Initiating scan on UUID %s", store_uuid)
            resp = api.scan(store_uuid)
            if resp is None:
                display_error("Scan", None, None, "Request failed (network or connection error)", "/api/config/scan")
                return 3
            if resp.status_code != 200:
                status_code, status_text, message = extract_error_info(resp)
                display_error("Scan", status_code, status_text, message, "/api/config/scan")
                return 3
            try:
                ticker(lambda: _validate_scan(api, ssh), SCAN_TIMEOUT, message="Scanning drives")
            except TimeoutError as exc:
                logger.error(
                    "Scan validation FAILED after %ds "
                    "(expected changes did not happen): %s",
                    SCAN_TIMEOUT, exc,
                )
                return 3
            logger.info("Scan validated")
        else:
            logger.info(
                "Bryck drives are detected (state=%r); skipping scan.",
                state,
            )

        logger.info("Initiating erase (reset_store) on UUID %s", store_uuid)
        resp = api.erase(store_uuid)
        if resp is None:
            display_error("Erase Store", None, None, "Request failed (network or connection error)", "/api/config/reset_store")
            return 3
        if resp.status_code != 200:
            status_code, status_text, message = extract_error_info(resp)
            display_error("Erase Store", status_code, status_text, message, "/api/config/reset_store")
            return 3
        logger.info("Erase initiated, validating (timeout=%ds)", ERASE_TIMEOUT)
        try:
            ticker(lambda: _validate_erase(api, store_uuid), ERASE_TIMEOUT, message="Erasing store")
        except TimeoutError as exc:
            info = api.bryck_info() or {}
            bi = info.get("bryck_info", {})
            logger.error(
                "Erase validation FAILED after %ds "
                "(expected changes did not happen; last State=%r Erased=%r): %s",
                ERASE_TIMEOUT, bi.get("State"), bi.get("Erased"), exc,
            )
            return 3
        logger.info("Erase validated")
        return 0
    finally:
        ssh.close()
        session.close()


if __name__ == "__main__":
    sys.exit(main())
