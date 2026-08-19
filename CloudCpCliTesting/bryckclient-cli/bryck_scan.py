#!/usr/bin/env python3
"""
Standalone Bryck scan runner.

Flow:
    1. Load login.json from the current directory.
    2. Log in to the Bryck REST API.
    3. Pick the single store UUID from result.logical_cards.
    4. If ``result.bryck_info.State == " Removed"`` call
       /api/config/scan and poll until the scan validates. Otherwise
       log an INFO message and exit 0 — the drives are already
       detected, nothing to do.

Usage:
    python3 bryck_scan.py [--login PATH]
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

STATE_REMOVED = " Removed"

REMOTE_BRYCKUTIL = "/opt/bryck/.venv/bryck/bin/bryckutil"


# =============================================================================
# Validators (self-contained)
# =============================================================================

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
                        logger.info("Drives are accessible. Bryck is logically attached.")
                        return True
            return False
    return False


# =============================================================================
# Helpers
# =============================================================================

def _bryck_state(api: BryckApi) -> str:
    """Return current ``result.bryck_info.State`` (empty string if missing)."""
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


# =============================================================================
# Entry point
# =============================================================================

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Scan the Bryck.")
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
        store_uuid = _pick_store_uuid(api)

        state = _bryck_state(api)
        logger.info("Current bryck state: %r", state)

        # ---- State check: only scan if "Removed" -----------
        if state != STATE_REMOVED:
            print(f"\n✓ Drives already detected (Bryck state: '{state}'). No need to scan.\n")
            logger.info("Skipping scan - drives already detected")
            return 0

        logger.info("Initiating scan on UUID %s", store_uuid)
        resp = api.scan(store_uuid)
        if resp is None:
            display_error("Scan Bryck", message="Request failed (see logs for details)")
            return 1
        
        # Check API response for errors
        if resp.status_code != 200:
            status_code, status_text, message = extract_error_info(resp)
            display_error("Scan Bryck", status_code, status_text, message, "/api/config/scan")
            return 1
        
        try:
            data = resp.json()
            if not data.get("success", False):
                error = data.get("error", {})
                message = error.get("message", "Unknown error") if isinstance(error, dict) else str(error)
                display_error("Scan Bryck", message=message)
                return 1
        except Exception as e:
            logger.debug("Failed to parse scan response: %s", e)
        logger.info("Scan initiated, validating (timeout=%ds)", SCAN_TIMEOUT)
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
        return 0
    finally:
        ssh.close()
        session.close()


if __name__ == "__main__":
    sys.exit(main())
