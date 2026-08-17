#!/usr/bin/env python3
"""
Standalone Bryck cloud-deconfigure runner.

Flow:
    1. Parse ``--cloud-type`` (aws / gcp / azure) from the CLI.
    2. Log in to the Bryck REST API.
    3. POST /api/bcloud/config_remove for the requested cloud type.
    4. Poll /api/bcloud/config_list until the cloud type no longer
       appears in the returned list (i.e. the deconfiguration has
       landed).

The runner is intentionally minimal:
    - No SSH / GCP keyfile handling (removal is a REST-only op; the
      staged keyfile on the server can be cleaned up manually).
    - No " Mounted" precondition -- removing cloud config is safe
      regardless of Bryck lifecycle state.

Usage:
    python3 bryck_cloud_deconfigure.py --cloud-type {aws,gcp,azure} \
        [--login PATH]
"""
from __future__ import annotations

import argparse
import logging
import os
import sys

from bryck_api import BryckApi, ticker, display_error, extract_error_info
from session import ApiSession

logger = logging.getLogger(__name__)

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_LOGIN_JSON = os.path.join(_SCRIPT_DIR, "login.json")

DECONFIGURE_TIMEOUT = 60

SUPPORTED_CLOUD_TYPES = ("aws", "gcp", "azure")


# =============================================================================
# Validators
# =============================================================================

def _validate_cloud_deconfigured(api: BryckApi, cloud_type: str) -> bool:
    """Poll callback: True when the cloud provider is absent from config_list.

    Inverse of ``_validate_cloud_configured`` in bryck_cloud_transfer.py.
    A missing / empty ``result`` list is also treated as success (the
    config is genuinely gone). Transient errors (non-JSON body, wrong
    result shape) return False so the ticker keeps polling.
    """
    resp = api.get_cloud_config_list()
    if resp is None:
        return False
    try:
        configs = resp.json().get("result", []) or []
    except ValueError:
        return False
    if not isinstance(configs, list):
        return False
    target = cloud_type.lower()
    for entry in configs:
        if not isinstance(entry, dict):
            continue
        entry_type = str(
            entry.get("bcloud_type") or entry.get("cloud_type") or ""
        ).lower()
        if entry_type == target:
            return False
    return True


# =============================================================================
# Entry point
# =============================================================================

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Remove a previously configured cloud provider from the "
            "Bryck and validate its deconfiguration."
        )
    )
    parser.add_argument(
        "--cloud-type",
        required=True,
        choices=SUPPORTED_CLOUD_TYPES,
        help="Cloud provider to deconfigure.",
    )
    parser.add_argument("--login", default=DEFAULT_LOGIN_JSON)
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    cloud_type = args.cloud_type.lower()

    session = ApiSession.from_login_json(args.login)
    try:
        session.login()
        api = BryckApi(session)

        logger.info("Removing cloud configuration for %s", cloud_type)
        resp = api.remove_cloud_config(cloud_type)
        if resp is None:
            display_error(
                "Cloud Deconfigure",
                message="Request failed (network or connection error)",
                endpoint="/api/bcloud/config_remove"
            )
            return 1
        if resp.status_code != 200:
            status_code, status_text, message = extract_error_info(resp)
            display_error(
                "Cloud Deconfigure",
                status_code,
                status_text,
                message,
                "/api/bcloud/config_remove"
            )
            return 1
        
        try:
            ticker(
                lambda: _validate_cloud_deconfigured(api, cloud_type),
                DECONFIGURE_TIMEOUT,
            )
        except TimeoutError as exc:
            logger.error(
                "Cloud deconfiguration validation FAILED after %ds "
                "(%s still present in config_list): %s",
                DECONFIGURE_TIMEOUT, cloud_type, exc,
            )
            return 3
        logger.info("Cloud configuration for %s removed", cloud_type)
        return 0
    finally:
        session.close()


if __name__ == "__main__":
    sys.exit(main())
