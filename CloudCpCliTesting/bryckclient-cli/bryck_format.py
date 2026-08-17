#!/usr/bin/env python3
"""
Standalone Bryck format runner.

Flow:
    1. Load login.json and format_mount_params.json from the current
       directory.
    2. Log in to the Bryck REST API.
    3. Pick the single store UUID from result.logical_cards.
    4. Scan the UUID and poll until scan validates.
    5. If the local key file exists, copy it locally to
       ``bryck_api.SERVER_KEY_FILE_PATH``.
    6. Call format_bryck with FILE_STORE + params from JSON.
    7. Poll validate_change until the target UUID reports FILE_STORE.
    8. Run validate_format against bryck_info.

Usage:
    python3 bryck_format.py
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
CONFIGURE_TIMEOUT = 900

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


def _validate_change(
    api: BryckApi,
    target_uuids: list[str],
    expected_store_type: str,
) -> bool:
    """Poll callback: True once every target UUID reports the expected store_type."""
    sys_info = api.bryck_info()
    if not sys_info:
        return False
    lcs = sys_info.get("logical_cards", {})
    for uuid in target_uuids:
        lc = lcs.get(uuid)
        if not lc:
            return False
        if lc.get("properties", {}).get("store_type") != expected_store_type:
            return False
    logger.info("All target logical cards report store_type=%s", expected_store_type)
    return True


def _validate_format(
    api: BryckApi,
    raid_level: int,
    enc: bool,
    exp_io_size: str | int | None,
    exp_data_sync: str | None,
    aws_key: str,
) -> None:
    """Final post-format check — matches MgmtOpsValidator.validate_format logic.

    Raises:
        ValueError: If any field diverges from the requested settings.
    """
    logger.info("Validating format")
    sys_info = api.bryck_info() or {}
    bryck_info = sys_info.get("bryck_info", {})

    is_enc = bryck_info.get("Encryption", bryck_info.get("encryption", "False"))
    is_aws_key = bryck_info.get("KeyType", bryck_info.get("key_type", ""))
    protection_mode = int(
        bryck_info.get("Protection", bryck_info.get("protection", 0)) or 0
    )
    expected_protection = int(raid_level)
    io_size = str(bryck_info.get("IoSize", bryck_info.get("io_size", 2048))).strip()
    data_sync = str(
        bryck_info.get("DataSync", bryck_info.get("data_sync", "application sync"))
    ).strip()

    logger.info(
        "Format validation | enc=%s api_enc=%s | want_prot=%d got_prot=%d | io_size=%s data_sync=%s",
        enc, is_enc, expected_protection, protection_mode, io_size, data_sync,
    )

    def _io_sync_ok() -> bool:
        if exp_io_size is None and exp_data_sync is None:
            return io_size == "2048" and data_sync == "application sync"
        return io_size == str(exp_io_size) and data_sync == str(exp_data_sync)

    if aws_key == "AWS_KMS":
        if not (
            str(is_enc).strip() == "True"
            and str(is_aws_key).strip() == aws_key
            and protection_mode == expected_protection
        ):
            raise ValueError("Protection mode or encryption type is different from input")
        if _io_sync_ok():
            return
        raise ValueError("IoSize or Data sync is different from input")

    if str(enc).strip() == str(is_enc).strip() and protection_mode == expected_protection:
        if _io_sync_ok():
            return
        raise ValueError("IoSize or Data sync is different from input")

    raise ValueError("Encryption or Protection mode is different from input")


# =============================================================================
# Helpers
# =============================================================================

def _load_json(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def _normalize_num_vols(raw: Any) -> int | None:
    if raw is None:
        return None
    if isinstance(raw, str):
        s = raw.strip()
        if s == "" or s.lower() == "none":
            return None
        return int(s)
    return int(raw)


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
        The server-side path to pass to the format API.
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
    parser = argparse.ArgumentParser(description="Scan + format the Bryck.")
    parser.add_argument("--login", default=DEFAULT_LOGIN_JSON)
    parser.add_argument("--params", default=DEFAULT_PARAMS_JSON)
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    params = _load_json(args.params)
    fmt = params["format"]

    key_file_local = _normalize_key_file(fmt.get("key_file"))
    encryption_option = (fmt.get("encryption_option") or "").strip() or None
    enc = bool(key_file_local) or bool(encryption_option)

    session = ApiSession.from_login_json(args.login)
    ssh = SshRunner.from_session(session)
    try:
        session.login()
        ssh.connect()
        api = BryckApi(session)

        # ---- Validate key_file path if provided ---------------------
        if key_file_local and not os.path.isfile(key_file_local):
            display_error(
                "Format Bryck",
                message=f"Invalid key file path. Provide a valid key file or set to None:\n{key_file_local}"
            )
            return 2

        # ---- Store selection (single UUID) --------------------------
        store_uuid = _pick_store_uuid(api)
        state = _bryck_state(api)
        logger.info("Current bryck state: %r", state)
        store_uuids = [store_uuid]

        # ---- State precondition: must be "Ejected" to format -----------
        if state != STATE_EJECTED:
            if state == STATE_REMOVED:
                display_error(
                    "Format Bryck",
                    message="Bryck is in 'Removed' state. Must run scan first to detect drives."
                )
            elif state == STATE_MOUNTED:
                display_error(
                    "Format Bryck",
                    message="Bryck is already mounted. Eject first before formatting."
                )
            else:
                display_error(
                    "Format Bryck",
                    message=f"Bryck state '{state}' is not valid for format operation. Expected 'Ejected' state."
                )
            return 2

        # ---- Key file placement (local copy) ------------------------
        server_key_path: str | None = None
        if not key_file_local:
            logger.info(
                "key_file is empty/None in params — skipping key transfer"
            )
        else:
            # At this point, key_file_local is validated (exists)
            server_key_path = _place_key_file(ssh, key_file_local)

        # ---- Format call -------------------------------------------
        raid_level = int(fmt["raid_level"])
        io_size = fmt.get("IoSize")
        data_sync = fmt.get("DataSync")
        num_vols = _normalize_num_vols(fmt.get("num_vols"))

        logger.info(
            "Formatting UUID=%s raid=%d filesystem=%s obj=%s",
            store_uuid, raid_level, fmt.get("filesystem"), fmt.get("obj"),
        )
        resp = api.format_bryck(
            uuids=store_uuids,
            store_type="FILE_STORE",
            raid_level=raid_level,
            key_file=server_key_path,
            description=fmt.get("description", ""),
            mountonreboot=bool(fmt.get("mountonreboot", False)),
            IoSize=io_size,
            DataSync=data_sync,
            encryption_option=encryption_option,
            compress=fmt.get("compress"),
            dedup=fmt.get("dedup"),
            filestore=bool(fmt.get("filestore", True)),
            obj=bool(fmt.get("obj", False)),
            filesystem=fmt.get("filesystem", "zfs"),
            num_vols=num_vols,
        )
        if resp is None:
            display_error("Format Bryck", None, None, "Request failed (network or connection error)", "/api/config/update")
            return 3
        if resp.status_code != 200:
            status_code, status_text, message = extract_error_info(resp)
            display_error("Format Bryck", status_code, status_text, message, "/api/config/update")
            return 3

        # ---- Validate change ---------------------------------------
        logger.info("Polling until logical card reports FILE_STORE")
        try:
            ticker(
                lambda: _validate_change(api, store_uuids, "FILE_STORE"),
                CONFIGURE_TIMEOUT,
                message="Formatting store",
            )
        except TimeoutError as exc:
            logger.error(
                "Format validation FAILED after %ds "
                "(expected changes did not happen; store_type never became "
                "FILE_STORE): %s",
                CONFIGURE_TIMEOUT, exc,
            )
            return 3

        # ---- Validate format ---------------------------------------
        _validate_format(
            api,
            raid_level=raid_level,
            enc=enc,
            exp_io_size=io_size,
            exp_data_sync=data_sync,
            aws_key=encryption_option or "",
        )
        logger.info("Format validated successfully")
        return 0
    finally:
        ssh.close()
        session.close()


if __name__ == "__main__":
    sys.exit(main())
