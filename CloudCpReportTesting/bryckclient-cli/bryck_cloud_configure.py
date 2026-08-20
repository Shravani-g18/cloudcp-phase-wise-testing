#!/usr/bin/env python3
"""
Standalone Bryck cloud configuration runner.

Configures a cloud provider (AWS, GCP, or Azure) on the Bryck device.

Supported cloud types:
    AWS    - Requires access_key_id and secret_access_key. Region is optional
             (defaults to us-west-1).
    GCP    - Requires keyfile (path to service account JSON). The keyfile is
             uploaded to the Bryck via SFTP.
    Azure  - Requires access_key_id, secret_access_key, and tenant_id.

Flow:
    1. Load cloud configuration parameters from cloud_ops.json.
    2. Validate required fields for the specified cloud type.
    3. For GCP: Upload the service-account keyfile to the Bryck via SFTP.
    4. Log in to the REST API.
    5. POST /api/bcloud/config to configure the cloud provider.
    6. Poll /api/bcloud/config_list to validate the provider is listed.
    7. Print success message and exit.

The cloud configuration remains active on the Bryck after the runner exits.
Remove it manually via bryck_cloud_deconfigure.py when no longer needed.

Usage:
    python3 bryck_cloud_configure.py [--login PATH] [--params PATH]
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

GCP_REMOTE_STAGE = "/tmp"
GCP_REMOTE_DIR = "/opt/bryck/bryckapi/downloads/deployment/.gcloud"

DEFAULT_AWS_REGION = "us-west-1"


# =============================================================================
# Helpers
# =============================================================================

def _load_json(path: str) -> dict[str, Any]:
    """Load and parse a JSON file."""
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
    """Validate cloud_ops.json and return the tuple for configure_cloud.

    Returns:
        (cloud_type, username, keyid, region, keyfile_local, tenant_id)

    Cloud-specific requirements:
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


# =============================================================================
# Validators
# =============================================================================

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


# =============================================================================
# Entry point
# =============================================================================

def main(argv: list[str] | None = None) -> int:
    """Main entry point for the cloud configuration runner."""
    parser = argparse.ArgumentParser(
        description=(
            "Configure a cloud provider (AWS, GCP, or Azure) on the Bryck. "
            "Reads cloud parameters from cloud_ops.json and validates the "
            "configuration by polling the cloud config list."
        )
    )
    parser.add_argument(
        "--login",
        default=DEFAULT_LOGIN_JSON,
        help="Path to login.json (default: %(default)s)",
    )
    parser.add_argument(
        "--params",
        default=DEFAULT_PARAMS_JSON,
        help="Path to cloud_ops.json (default: %(default)s)",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    # ---- Load and validate cloud parameters -------------------------
    try:
        params = _load_json(args.params)
    except (OSError, json.JSONDecodeError) as exc:
        display_error(
            "Load Parameters",
            message=f"Failed to load {args.params}: {exc}"
        )
        return 2

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
        display_error(
            "Cloud Parameters",
            message=f"Bad cloud parameters: {exc}"
        )
        return 2

    # ---- Load session and API ---------------------------------------
    try:
        session = ApiSession.from_login_json(args.login)
    except Exception as exc:
        display_error(
            "Load Login",
            message=f"Failed to load {args.login}: {exc}"
        )
        return 2

    ssh: SshRunner | None = None

    try:
        # ---- GCP keyfile placement (needs SSH before REST) ---------
        keyfile_remote: str | None = None
        if cloud_type == "gcp":
            logger.info("Uploading GCP keyfile to Bryck...")
            ssh = SshRunner.from_session(session)
            ssh.connect()
            try:
                keyfile_remote = _place_gcp_keyfile(ssh, keyfile_local or "")
            except RuntimeError as exc:
                display_error("GCP Keyfile Upload", message=str(exc))
                return 3

        # ---- Login to API -------------------------------------------
        session.login()
        api = BryckApi(session)

        # ---- Configure cloud ----------------------------------------
        logger.info("Configuring cloud provider: %s", cloud_type.upper())
        resp = api.configure_cloud(
            bcloud_type=cloud_type,
            username=username,
            keyid=keyid,
            region=region,
            keyfile=keyfile_remote,
            tenant_id=tenant_id,
        )
        
        if resp is None:
            display_error(
                "Configure Cloud",
                message="Request failed (see logs for details)"
            )
            return 3
        
        if resp.status_code != 200:
            status_code, status_text, message = extract_error_info(resp)
            display_error(
                "Configure Cloud",
                status_code=status_code,
                status_text=status_text,
                message=message,
                endpoint="/api/bcloud/config"
            )
            return 3

        logger.info("Cloud configuration request accepted")

        # ---- Validate configuration ---------------------------------
        logger.info("Validating cloud configuration...")
        try:
            ticker(
                lambda: _validate_cloud_configured(api, cloud_type),
                CONFIGURE_TIMEOUT,
                message="Waiting for cloud configuration",
            )
        except TimeoutError:
            display_error(
                "Configure Cloud Validation",
                message=f"Cloud configuration validation FAILED after {CONFIGURE_TIMEOUT}s"
            )
            return 3

        # ---- Success ------------------------------------------------
        print(f"\n✓ Cloud provider '{cloud_type.upper()}' configured successfully!")
        print(f"\nThe cloud configuration is now active on the Bryck.")
        print(f"Use 'bryck_cloud_deconfigure.py' to remove it when no longer needed.")
        
        return 0

    except KeyboardInterrupt:
        logger.warning("Interrupted by user")
        return 130
    except Exception as exc:
        display_error("Unexpected Error", message=str(exc))
        logger.exception("Unexpected error")
        return 1
    finally:
        if ssh is not None:
            try:
                ssh.close()
            except Exception:
                pass


if __name__ == "__main__":
    sys.exit(main())
