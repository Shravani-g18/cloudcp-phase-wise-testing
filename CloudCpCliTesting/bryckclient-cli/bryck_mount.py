#!/usr/bin/env python3
"""
Standalone Bryck mount runner.

Flow:
    1. Load login.json and format_mount_params.json from the current
       directory.
    2. Log in to the Bryck REST API.
    3. Pick the single store UUID from result.logical_cards.
    4. Scan the UUID and poll until scan validates.
    5. If the local key file exists, copy it locally to
       ``bryck_api.SERVER_KEY_FILE_PATH``.
    6. Call mount with the fixed mount_point ``/bryck`` and JSON params.
    7. Poll validate_mount until the Bryck reports Mounted.

Usage:
    python3 bryck_mount.py
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from typing import Any

from bryck_api import BryckApi, ticker, display_error, extract_error_info
from session import ApiSession
from ssh_runner import DEFAULT_KEY_FILE_REMOTE_PATH, SshRunner

logger = logging.getLogger(__name__)

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_LOGIN_JSON = os.path.join(_SCRIPT_DIR, "login.json")
DEFAULT_PARAMS_JSON = os.path.join(_SCRIPT_DIR, "format_mount_params.json")

SCAN_TIMEOUT = 180
CONFIGURE_TIMEOUT = 600
MOUNT_POINT = "/bryck"

# Values reported by /api/config/info -> result.bryck_info.State.
# NOTE: the API returns these strings with a leading space intact.
STATE_MOUNTED = " Mounted"
STATE_EJECTED = " Ejected"
STATE_REMOVED = " Removed"

REMOTE_BRYCKUTIL = "/opt/bryck/.venv/bryck/bin/bryckutil"


# =============================================================================
# Validators (duplicated locally so this file is self-contained)
# =============================================================================

def _validate_scan(api: BryckApi, ssh: SshRunner) -> bool:
    """Poll callback: True when scan looks complete.

    Mirrors ``MgmtOpsValidator.validate_scan`` — runs the ``bryckutil
    bryck list`` check on the Bryck via SSH.
    """
    try:
        sys_info = api.bryck_info()
    except Exception:
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


def _validate_mount(api: BryckApi) -> bool:
    """Poll callback: True when Bryck state is Mounted and FILE_STORE UP."""
    sys_info = api.bryck_info()
    if not sys_info:
        return False

    lcs = sys_info.get("logical_cards", {})
    if sys_info.get("bryck_info", {}).get("State") != " Mounted":
        return False

    num_fs = 0
    for _lc_id, lc in lcs.items():
        store_type = lc.get("properties", {}).get("store_type")
        store_status = lc.get("current_conditions", {}).get("store", {}).get("status")
        if store_status not in ("UP", "MOUNTING"):
            return False
        if store_type == "FILE_STORE":
            num_fs += 1
    return num_fs > 0


# =============================================================================
# Helpers
# =============================================================================

def _load_json(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def _pick_store_uuid(api: BryckApi) -> str:
    """Return the first (and only) key of result.logical_cards from /api/config/info."""
    sys_info = api.bryck_info() or {}
    lcs = sys_info.get("logical_cards", {})
    if not lcs:
        raise RuntimeError("No logical cards reported by /api/config/info")
    uuid = next(iter(lcs))
    logger.info("Selected store UUID: %s", uuid)
    return uuid


def _bryck_state(api: BryckApi) -> str:
    """Return the current ``result.bryck_info.State`` (empty string if missing).

    The API returns values like ``' Mounted'`` / ``' Ejected'`` /
    ``' Removed'`` (with a leading space).
    """
    sys_info = api.bryck_info() or {}
    return sys_info.get("bryck_info", {}).get("State", "")


def _normalize_key_file(raw: Any) -> str:
    """Return an absolute local path, or ``""`` when the value means 'no key'.

    Treats ``None``, ``""``, ``"None"``, and ``"null"`` (any case) as no
    key. Any other string is stripped, ``~`` is expanded, and if the
    path is relative it is resolved against the current working
    directory so the caller always sees an absolute path.
    """
    if not isinstance(raw, str):
        return ""
    s = raw.strip()
    if s.lower() in ("", "none", "null"):
        return ""
    s = os.path.expanduser(s)
    if not os.path.isabs(s):
        s = os.path.abspath(os.path.join(os.getcwd(), s))
    return s


def _place_key_file(ssh: SshRunner, local_path: str) -> str:
    """SFTP-upload the local key file and make it world-readable on the Bryck.

    Steps:
        1. SFTP ``local_path`` -> ``DEFAULT_KEY_FILE_REMOTE_PATH``.
        2. ``sudo -n chmod 0644`` the remote file so the API service can
           read it.

    Passwordless sudo for ``chmod`` on ``DEFAULT_KEY_FILE_REMOTE_PATH``
    is required for the remote ``bryckserver_username``.

    Returns:
        The server-side path to pass to the mount API.
    """
    ssh.put(local_path, DEFAULT_KEY_FILE_REMOTE_PATH)
    rc, out, err = ssh.run(
        f"sudo -n chmod 0644 {DEFAULT_KEY_FILE_REMOTE_PATH}", timeout=30
    )
    if rc != 0:
        raise RuntimeError(
            f"Failed to chmod remote key file (rc={rc}, "
            f"err={err.strip() or out.strip()})"
        )
    logger.info(
        "Uploaded key file %s -> %s (chmod 0644)",
        local_path, DEFAULT_KEY_FILE_REMOTE_PATH,
    )
    return DEFAULT_KEY_FILE_REMOTE_PATH


# =============================================================================
# Entry point
# =============================================================================

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Scan + mount the Bryck.")
    parser.add_argument("--login", default=DEFAULT_LOGIN_JSON)
    parser.add_argument("--params", default=DEFAULT_PARAMS_JSON)
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    params = _load_json(args.params)
    mnt = params["mount"]

    key_file_local = _normalize_key_file(mnt.get("key_file"))
    encryption_option = (mnt.get("encryption_option") or "").strip() or None

    session = ApiSession.from_login_json(args.login)
    ssh = SshRunner.from_session(session)
    try:
        session.login()
        ssh.connect()
        api = BryckApi(session)

        # ---- Check encryption status and validate key_file ----------
        sys_info = api.bryck_info() or {}
        bryck_info = sys_info.get("bryck_info", {})
        is_encrypted = str(bryck_info.get("Encryption", "False")).strip() == "True"
        
        logger.info("Bryck encryption status: %s", "Encrypted" if is_encrypted else "Not Encrypted")
        
        if is_encrypted:
            # Case C: Encrypted but no key_file provided
            if not key_file_local:
                display_error(
                    "Mount Bryck",
                    message="Bryck is encrypted but no key file provided.\nPlease provide a valid key file path in the mount parameters."
                )
                return 2
            # Case B: Encrypted but invalid key_file path
            if not os.path.isfile(key_file_local):
                display_error(
                    "Mount Bryck",
                    message=f"Bryck is encrypted but key file path does not exist or is invalid:\n{key_file_local}"
                )
                return 2
        else:
            # Case A: Non-encrypted - force key_file to None
            if key_file_local:
                logger.warning(
                    "Bryck is not encrypted. Ignoring provided key_file parameter: %s",
                    key_file_local,
                )
                key_file_local = ""

        # ---- Store selection (single UUID) --------------------------
        store_uuid = _pick_store_uuid(api)
        state = _bryck_state(api)
        logger.info("Current bryck state: %r", state)
        store_uuids = [store_uuid]

        # ---- State precondition: must be "Ejected" to mount -----------
        if state != STATE_EJECTED:
            if state == STATE_REMOVED:
                display_error(
                    "Mount Bryck",
                    message="Bryck is in 'Removed' state. Must run scan and format first."
                )
            elif state == STATE_MOUNTED:
                display_error(
                    "Mount Bryck",
                    message="Bryck is already mounted. Cannot mount twice."
                )
            else:
                display_error(
                    "Mount Bryck",
                    message=f"Bryck state '{state}' is not valid for mount operation. Expected 'Ejected' state."
                )
            return 2

        # ---- Key file placement (local copy) ------------------------
        server_key_path: str | None = None
        if not key_file_local:
            logger.info(
                "key_file is empty/None in params — skipping key transfer"
            )
        else:
            # At this point, key_file_local is validated (exists and Bryck is encrypted)
            server_key_path = _place_key_file(ssh, key_file_local)

        # ---- Mount call --------------------------------------------
        logger.info(
            "Mounting UUIDs=%s mount_point=%s mountonreboot=%s force_check=%s",
            store_uuids,
            MOUNT_POINT,
            mnt.get("mountonreboot"),
            mnt.get("force_check"),
        )
        resp = api.mount(
            uuids=store_uuids,
            mount_point=MOUNT_POINT,
            key_file=server_key_path,
            mountonreboot=bool(mnt.get("mountonreboot", False)),
            force_check=bool(mnt.get("force_check", False)),
            encryption_option=encryption_option,
        )
        if resp is None:
            display_error("Mount Bryck", message="Request failed (see logs for details)")
            return 1
        
        # Check API response for errors
        if resp.status_code != 200:
            status_code, status_text, message = extract_error_info(resp)
            display_error("Mount Bryck", status_code, status_text, message, "/api/config/mount")
            return 1
        
        try:
            data = resp.json()
            if not data.get("success", False):
                error = data.get("error", {})
                message = error.get("message", "Unknown error") if isinstance(error, dict) else str(error)
                display_error("Mount Bryck", message=message)
                return 1
        except Exception as e:
            logger.debug("Failed to parse mount response: %s", e)
            # Continue if we can't parse response but got 200 OK

        # ---- Validate mount ----------------------------------------
        logger.info("Mount initiated, validating mount completion")
        try:
            ticker(lambda: _validate_mount(api), CONFIGURE_TIMEOUT)
        except TimeoutError as exc:
            final_state = _bryck_state(api)
            logger.error(
                "Mount validation FAILED after %ds "
                "(expected changes did not happen; last state=%r): %s",
                CONFIGURE_TIMEOUT, final_state, exc,
            )
            return 3
        logger.info("Mount validated successfully")
        return 0
    finally:
        ssh.close()
        session.close()


if __name__ == "__main__":
    sys.exit(main())
