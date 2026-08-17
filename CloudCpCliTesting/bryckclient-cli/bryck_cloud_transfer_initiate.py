#!/usr/bin/env python3
"""
Standalone Bryck cloud-transfer runner.

Modes (selected via mandatory ``--mode``):
    upload    Kick off bryck_src  -> cloud_bucket only.
              Requires bryck_src + cloud_bucket in cloud_ops.json.
    download  Kick off cloud_bucket -> bryck_dst only.
              Requires cloud_bucket + bryck_dst in cloud_ops.json.
    both      Kick off both transfers (previous default behaviour).
              Requires bryck_src + cloud_bucket + bryck_dst.

Flow:
    1. Load login.json and cloud_ops.json from the current directory.
    2. Resolve per-cloud parameters (AWS / GCP / Azure) with client-side
       validation.
    3. For GCP: SFTP-upload the service-account keyfile to
       /opt/bryck/bryckapi/downloads/deployment/.gcloud/<basename> on
       the Bryck (staged in /tmp, then sudo mv + chmod 0644).
    4. Log in to the REST API.
    5. POST /api/bcloud/config to configure the cloud provider.
    6. Poll /api/bcloud/config_list until the provider is listed.
    7. If mode in {upload, both}: kick off the upload transfer
       (bryck_src -> cloud_bucket) and validate that it reaches state
       IN_PROGRESS.
    8. If mode in {download, both}: kick off the download transfer
       (cloud_bucket -> bryck_dst) and validate that it reaches state
       IN_PROGRESS.
    9. Highlight the transfer ID(s) that were actually started and
       print instructions to run ``bryck_cloud_transfer_status.py``
       for progress / completion, then exit.

The cloud configuration is intentionally left in place after the runner
exits — the Bryck's transfer engine still needs the credentials to
finish the transfer in the background. Remove it manually via
``bryck_cloud_deconfigure.py`` when it is no longer needed.

Usage:
    python3 bryck_cloud_transfer.py --mode {upload,download,both} \
        [--login PATH] [--params PATH]
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
from ssh_runner import SshRunner

logger = logging.getLogger(__name__)

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_LOGIN_JSON = os.path.join(_SCRIPT_DIR, "login.json")
DEFAULT_PARAMS_JSON = os.path.join(_SCRIPT_DIR, "cloud_ops.json")

CONFIGURE_TIMEOUT = 60
TRANSFER_START_TIMEOUT = 120

GCP_REMOTE_STAGE = "/tmp"
GCP_REMOTE_DIR = "/opt/bryck/bryckapi/downloads/deployment/.gcloud"

STATE_IN_PROGRESS = "IN_PROGRESS"
STATE_COMPLETED = "COMPLETED"
STATE_PAUSED = "PAUSED"
TERMINAL_FAILURE_STATES = {"FAILED", "STOPPED", "CANCELLED"}

# Bryck lifecycle state (from /api/config/info -> result.bryck_info.State).
# Leading space is intentional — that's the on-wire format.
STATE_MOUNTED = " Mounted"

DEFAULT_AWS_REGION = "us-west-1"


# =============================================================================
# Helpers
# =============================================================================

def _load_json(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def _clean(value: Any) -> str:
    """Return stripped string, treating None / 'None' / 'null' as empty."""
    if value is None:
        return ""
    s = str(value).strip()
    if s.lower() in ("", "none", "null"):
        return ""
    return s


def _require(cfg: dict[str, Any], cloud_type: str, fields: list[str]) -> None:
    """Raise ValueError if any required field is missing/empty."""
    missing = [f for f in fields if not _clean(cfg.get(f))]
    if missing:
        raise ValueError(
            f"cloud_type={cloud_type!r} requires field(s) {missing} "
            f"in cloud_ops.json"
        )


def _resolve_cloud_params(
    cfg: dict[str, Any],
) -> tuple[str, str | None, str | None, str | None, str | None, str | None]:
    """Validate cloud_ops.json and return the tuple passed to configure_cloud.

    Returns:
        (cloud_type, username, keyid, region, keyfile_local, tenant_id)

    - AWS: access_key_id + secret_access_key required; region optional
      (defaults to us-west-1). No keyfile / tenant_id.
    - GCP: keyfile required (local path to service-account JSON). No
      access_key_id / secret_access_key / tenant_id.
    - Azure: access_key_id + secret_access_key + tenant_id required. No
      region / keyfile.
    """
    ct = _clean(cfg.get("cloud_type")).lower()
    if ct == "aws":
        _require(cfg, ct, ["access_key_id", "secret_access_key"])
        region = _clean(cfg.get("region")) or DEFAULT_AWS_REGION
        return (
            ct,
            _clean(cfg["access_key_id"]),
            _clean(cfg["secret_access_key"]),
            region,
            None,
            None,
        )
    if ct == "gcp":
        _require(cfg, ct, ["keyfile"])
        keyfile_local = os.path.expanduser(_clean(cfg["keyfile"]))
        if not os.path.isabs(keyfile_local):
            keyfile_local = os.path.abspath(keyfile_local)
        if not os.path.isfile(keyfile_local):
            raise ValueError(
                f"GCP keyfile does not exist on this machine: {keyfile_local}"
            )
        return (ct, None, None, None, keyfile_local, None)
    if ct == "azure":
        _require(
            cfg, ct, ["access_key_id", "secret_access_key", "tenant_id"]
        )
        return (
            ct,
            _clean(cfg["access_key_id"]),
            _clean(cfg["secret_access_key"]),
            None,
            None,
            _clean(cfg["tenant_id"]),
        )
    raise ValueError(
        f"Unsupported cloud_type={ct!r}; expected one of aws / gcp / azure"
    )


def _place_gcp_keyfile(ssh: SshRunner, local_path: str) -> str:
    """SFTP-upload GCP keyfile into the on-server .gcloud directory.

    Stages the file in /tmp, then uses sudo -n to mkdir the destination
    directory (root-owned on stock installs), move the file into it,
    and chmod it 0644 so the API service can read it.

    Returns:
        The basename to pass as ``keyfile`` to ``configure_cloud``.
    """
    basename = os.path.basename(local_path)
    tmp_path = f"{GCP_REMOTE_STAGE}/{basename}"
    dst_path = f"{GCP_REMOTE_DIR}/{basename}"
    ssh.put(local_path, tmp_path)
    for cmd in (
        f"sudo -n mkdir -p {GCP_REMOTE_DIR}",
        f"sudo -n mv {tmp_path} {dst_path}",
        f"sudo -n chmod 0644 {dst_path}",
    ):
        rc, out, err = ssh.run(cmd, timeout=30)
        if rc != 0:
            raise RuntimeError(
                f"GCP keyfile placement failed: '{cmd}' "
                f"(rc={rc}, err={err.strip() or out.strip()})"
            )
    logger.info(
        "Uploaded GCP keyfile %s -> %s (chmod 0644)", local_path, dst_path
    )
    return basename


def _extract_transfer_id(resp_result: Any) -> str:
    """Pull the transfer_id out of the /api/bcloud/transfer response.

    The endpoint returns either a bare string ID, a bare integer ID, or
    a dict with a ``transfer_id`` key. Accept all variants.
    """
    if isinstance(resp_result, (str, int)):
        return str(resp_result)
    if isinstance(resp_result, dict):
        for key in ("transfer_id", "id"):
            if key in resp_result and resp_result[key]:
                return str(resp_result[key])
    raise RuntimeError(
        f"Cloud transfer response did not include a transfer_id: {resp_result!r}"
    )


# =============================================================================
# Validators
# =============================================================================

def _bryck_state(api: BryckApi) -> str:
    """Return current result.bryck_info.State (empty string if missing)."""
    sys_info = api.bryck_info() or {}
    return sys_info.get("bryck_info", {}).get("State", "")


def _validate_cloud_configured(api: BryckApi, cloud_type: str) -> bool:
    """Poll callback: True when the cloud provider appears in config_list."""
    resp = api.get_cloud_config_list()
    if resp is None:
        return False
    try:
        configs = resp.json().get("result", []) or []
    except ValueError:
        return False
    if not isinstance(configs, list):
        return False
    for entry in configs:
        if not isinstance(entry, dict):
            continue
        entry_type = str(
            entry.get("bcloud_type") or entry.get("cloud_type") or ""
        ).lower()
        if entry_type == cloud_type.lower():
            return True
    return False


def _validate_transfer_started(api: BryckApi, transfer_id: str) -> bool:
    """Poll callback: True once the transfer enters IN_PROGRESS.

    Also treat COMPLETED as OK (transfer finished so fast we missed the
    intermediate state — still a valid start). Raises RuntimeError on
    terminal-failure states (FAILED / STOPPED / CANCELLED) so the
    ticker aborts immediately rather than burning the whole
    ``TRANSFER_START_TIMEOUT`` budget.
    """
    resp = api.get_cloud_transfer_status(transfer_id)
    if resp is None:
        return False
    
    try:
        body = resp.json()
    except ValueError:
        return False
    
    result = body.get("result", {})
    
    # /api/bcloud/status_transfer returns result as a list of dicts
    # (typically one entry). Older/alt schemas may send a bare dict.
    entry: dict[str, Any] | None = None
    if isinstance(result, list):
        for item in result:
            if isinstance(item, dict) and item:
                entry = item
                break
    elif isinstance(result, dict) and result:
        entry = result
    
    if entry is None:
        return False
    
    state = str(entry.get("state") or "").upper()
    if not state:
        return False
    
    if state in TERMINAL_FAILURE_STATES:
        raise RuntimeError(
            f"Cloud transfer {transfer_id} entered terminal state {state}"
        )
    
    if state in (STATE_IN_PROGRESS, STATE_COMPLETED, STATE_PAUSED):
        return True
    
    return False


# =============================================================================
# Entry point
# =============================================================================

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Configure a cloud provider on the Bryck and kick off an "
            "upload transfer, a download transfer, or both. The direction "
            "is selected with the mandatory --mode flag."
        )
    )
    parser.add_argument("--login", default=DEFAULT_LOGIN_JSON)
    parser.add_argument("--params", default=DEFAULT_PARAMS_JSON)
    parser.add_argument(
        "--mode",
        required=True,
        choices=["upload", "download", "both"],
        help=(
            "Transfer direction: 'upload' (bryck_src -> cloud_bucket), "
            "'download' (cloud_bucket -> bryck_dst), or 'both'. "
            "Required; no default."
        ),
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    params = _load_json(args.params)

    # ---- Client-side parameter validation ---------------------------
    try:
        (
            cloud_type,
            username,
            keyid,
            region,
            keyfile_local,
            tenant_id,
        ) = _resolve_cloud_params(params)
    except (KeyError, ValueError) as exc:
        display_error("Cloud Parameters", message=f"Bad cloud parameters: {exc}")
        return 2

    bryck_src = _clean(params.get("bryck_src"))
    cloud_bucket = _clean(params.get("cloud_bucket"))
    bryck_dst = _clean(params.get("bryck_dst"))

    # Mode-aware path validation. cloud_bucket is always required;
    # bryck_src only when uploading; bryck_dst only when downloading.
    missing: list[str] = []
    if not cloud_bucket:
        missing.append("cloud_bucket")
    if args.mode in ("upload", "both") and not bryck_src:
        missing.append("bryck_src")
    if args.mode in ("download", "both") and not bryck_dst:
        missing.append("bryck_dst")
    if missing:
        display_error(
            "Cloud Parameters",
            message=f"cloud_ops.json missing required field(s) for --mode {args.mode}: {', '.join(missing)}"
        )
        return 2

    session = ApiSession.from_login_json(args.login)
    ssh: SshRunner | None = None

    try:
        # ---- GCP keyfile placement (needs SSH before REST) ---------
        keyfile_remote: str | None = None
        if cloud_type == "gcp":
            ssh = SshRunner.from_session(session)
            ssh.connect()
            try:
                keyfile_remote = _place_gcp_keyfile(ssh, keyfile_local or "")
            except RuntimeError as exc:
                display_error("GCP Keyfile Upload", message=str(exc))
                return 3

        session.login()
        api = BryckApi(session)

        # ---- Log current state (no precondition check) -------------
        state = _bryck_state(api)
        logger.info("Current bryck state: %r", state)

        # ---- Configure cloud ---------------------------------------
        logger.info("Configuring cloud provider %s", cloud_type)
        if api.configure_cloud(
            bcloud_type=cloud_type,
            username=username,
            keyid=keyid,
            region=region,
            keyfile=keyfile_remote,
            tenant_id=tenant_id,
        ) is None:
            display_error("Configure Cloud", message="configure_cloud request failed (see logs for details)")
            return 3
        try:
            ticker(
                lambda: _validate_cloud_configured(api, cloud_type),
                CONFIGURE_TIMEOUT,
            )
        except TimeoutError as exc:
            display_error(
                "Configure Cloud",
                message=f"Cloud configuration validation FAILED after {CONFIGURE_TIMEOUT}s: {exc}"
            )
            return 3
        logger.info("Cloud configuration for %s applied", cloud_type)

        upload_id: str | None = None
        download_id: str | None = None

        # ---- Upload transfer ---------------------------------------
        if args.mode in ("upload", "both"):
            logger.info(
                "Initiating UPLOAD transfer: src=%s dst=%s",
                bryck_src, cloud_bucket,
            )
            up_resp = api.initiate_cloud_transfer(
                cloud_type, bryck_src, cloud_bucket,
            )
            if up_resp is None:
                display_error("Initiate Upload Transfer", message="Request failed (see logs for details)")
                return 3
            
            if up_resp.status_code != 200:
                status_code, status_text, message = extract_error_info(up_resp)
                display_error(
                    "Initiate Upload Transfer",
                    status_code=status_code,
                    status_text=status_text,
                    message=message,
                    endpoint="/api/bcloud/transfer"
                )
                return 3
            
            try:
                upload_id = _extract_transfer_id(up_resp.json().get("result"))
            except RuntimeError as exc:
                display_error("Initiate Upload Transfer", message=str(exc))
                return 3
            logger.info("Upload transfer_id=%s — validating start", upload_id)
            try:
                ticker(
                    lambda: _validate_transfer_started(api, upload_id),
                    TRANSFER_START_TIMEOUT,
                    message="Waiting for upload transfer to start",
                )
            except RuntimeError as exc:
                display_error("Upload Transfer Validation", message=str(exc))
                return 4
            except TimeoutError as exc:
                display_error(
                    "Upload Transfer Validation",
                    message=f"Transfer {upload_id} did not reach IN_PROGRESS in {TRANSFER_START_TIMEOUT}s: {exc}"
                )
                return 3
            logger.info("Upload transfer %s reached IN_PROGRESS", upload_id)

        # ---- Download transfer -------------------------------------
        if args.mode in ("download", "both"):
            logger.info(
                "Initiating DOWNLOAD transfer: src=%s dst=%s",
                cloud_bucket, bryck_dst,
            )
            dn_resp = api.initiate_cloud_transfer(
                cloud_type, cloud_bucket, bryck_dst,
            )
            if dn_resp is None:
                display_error("Initiate Download Transfer", message="Request failed (see logs for details)")
                return 3
            
            if dn_resp.status_code != 200:
                status_code, status_text, message = extract_error_info(dn_resp)
                display_error(
                    "Initiate Download Transfer",
                    status_code=status_code,
                    status_text=status_text,
                    message=message,
                    endpoint="/api/bcloud/transfer"
                )
                return 3
            
            try:
                download_id = _extract_transfer_id(dn_resp.json().get("result"))
            except RuntimeError as exc:
                display_error("Initiate Download Transfer", message=str(exc))
                return 3
            logger.info(
                "Download transfer_id=%s — validating start", download_id,
            )
            try:
                ticker(
                    lambda: _validate_transfer_started(api, download_id),
                    TRANSFER_START_TIMEOUT,
                    message="Waiting for download transfer to start",
                )
            except RuntimeError as exc:
                display_error("Download Transfer Validation", message=str(exc))
                return 4
            except TimeoutError as exc:
                display_error(
                    "Download Transfer Validation",
                    message=f"Transfer {download_id} did not reach IN_PROGRESS in {TRANSFER_START_TIMEOUT}s: {exc}"
                )
                return 3
            logger.info(
                "Download transfer %s reached IN_PROGRESS", download_id,
            )

        # ---- Done --------------------------------------------------
        # Highlight the transfer ID(s) actually started and point the
        # user at the status runner. This runner intentionally does
        # NOT wait for COMPLETED; the user must poll via
        # bryck_cloud_transfer_status.py.
        if args.mode == "upload":
            banner_lead = "Cloud UPLOAD transfer STARTED successfully."
        elif args.mode == "download":
            banner_lead = "Cloud DOWNLOAD transfer STARTED successfully."
        else:
            banner_lead = (
                "Cloud UPLOAD + DOWNLOAD transfers STARTED successfully."
            )
        logger.info("=" * 72)
        logger.info(
            "%s Transfer completion is NOT validated by this runner.",
            banner_lead,
        )
        if upload_id is not None:
            logger.info("  UPLOAD   transfer_id = %s", upload_id)
        if download_id is not None:
            logger.info("  DOWNLOAD transfer_id = %s", download_id)
        logger.info(
            "To check progress / final state, use "
            "bryck_cloud_transfer_status.py:"
        )
        if upload_id is not None:
            logger.info(
                "  python3 bryck_cloud_transfer_status.py --transfer-id %s",
                upload_id,
            )
        if download_id is not None:
            logger.info(
                "  python3 bryck_cloud_transfer_status.py --transfer-id %s",
                download_id,
            )
        logger.info(
            "(or run it with no --transfer-id to list all active transfers)"
        )
        logger.info("=" * 72)
        logger.info("Cloud configuration for %s left in place", cloud_type)
        return 0
    finally:
        if ssh is not None:
            ssh.close()
        session.close()


if __name__ == "__main__":
    sys.exit(main())
