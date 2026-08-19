#!/usr/bin/env python3
"""
Standalone Bryck network configuration runner.

Flow:
    1. Load login.json and change_ip_params.json from the script directory.
    2. Log in to the Bryck REST API.
    3. Pick the single logical-card UUID from result.logical_cards.
    4. Call /api/network/configure with the params.
    5. Poll /api/config/info result.server_info.ethernet[] until the
       target interface reflects the requested values.

Usage:
    python3 change_ip.py [--login PATH] [--params PATH]

Exit codes:
    0 = success
    1 = HTTP / configure call failed
    3 = validation timed out (change did not converge)
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from typing import Any

from bryck_api import BryckApi, ticker
from session import ApiSession

logger = logging.getLogger(__name__)

DEFAULT_LOGIN_JSON = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "login.json"
)
DEFAULT_PARAMS_JSON = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "change_ip_params.json"
)

CHANGE_IP_TIMEOUT = 120


# =============================================================================
# Helpers
# =============================================================================

def _pick_lc_uuid(api: BryckApi) -> str:
    """Return the first (and only) key of result.logical_cards."""
    sys_info = api.bryck_info() or {}
    lcs = sys_info.get("logical_cards", {})
    if not lcs:
        raise RuntimeError("No logical cards reported by /api/config/info")
    uuid = next(iter(lcs))
    logger.info("Selected logical-card UUID: %s", uuid)
    return uuid


def _find_interface(
    api: BryckApi, interface_name: str
) -> dict[str, Any] | None:
    """Return the ethernet dict matching ``interface_name``, or None."""
    try:
        info = api.bryck_info()
    except Exception:
        logger.debug("bryck_info() raised during validation", exc_info=True)
        return None
    if not info:
        return None
    eths = info.get("server_info", {}).get("ethernet", []) or []
    for entry in eths:
        if entry.get("name") == interface_name:
            return entry
    return None


# =============================================================================
# Validator
# =============================================================================

def _validate_network(api: BryckApi, params: dict[str, Any]) -> bool:
    """Poll callback: True when server_info.ethernet reflects params.

    Only ``interface_name``, ``ip``, and ``netmask`` are checked.
    """
    interface_name = params.get("interface_name")
    if not interface_name:
        return True  # nothing to validate

    iface = _find_interface(api, interface_name)
    if iface is None:
        logger.debug("Interface %r not found yet in ethernet[]", interface_name)
        return False

    ip_block = iface.get("IP", {}) or {}

    expected_ip = params.get("ip")
    if expected_ip is not None and ip_block.get("addr") != expected_ip:
        logger.debug(
            "IP mismatch on %s: got=%r want=%r",
            interface_name, ip_block.get("addr"), expected_ip,
        )
        return False

    expected_netmask = params.get("netmask")
    if (
        expected_netmask is not None
        and ip_block.get("netmask") != expected_netmask
    ):
        logger.debug(
            "netmask mismatch on %s: got=%r want=%r",
            interface_name, ip_block.get("netmask"), expected_netmask,
        )
        return False

    logger.info(
        "Interface %r matches requested config: %s",
        interface_name, iface,
    )
    return True


# =============================================================================
# Entry point
# =============================================================================

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Configure Bryck network via /api/network/configure."
    )
    parser.add_argument("--login", default=DEFAULT_LOGIN_JSON)
    parser.add_argument("--params", default=DEFAULT_PARAMS_JSON)
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    with open(args.params, "r", encoding="utf-8") as fh:
        params: dict[str, Any] = json.load(fh)

    interface_name = params.get("interface_name")
    logger.warning(
        "If configuring the interface used for this session, connectivity "
        "may drop mid-run and validation may time out."
    )

    session = ApiSession.from_login_json(args.login)
    try:
        session.login()
        api = BryckApi(session)
        uuid = _pick_lc_uuid(api)

        logger.info(
            "Configuring network on UUID %s (interface=%s, dhcp=%s, ip=%s)",
            uuid, interface_name, params.get("dhcp"), params.get("ip"),
        )
        response = api.configure_network(
            uuid,
            interface_name=interface_name,
            dhcp=params.get("dhcp"),
            ip=params.get("ip"),
            netmask=params.get("netmask"),
            gateway=params.get("gateway"),
            nameservers=params.get("nameservers"),
            ntp_server=params.get("ntp_server"),
            mtu=params.get("mtu"),
        )
        if response is None:
            logger.error("configure_network returned no response (HTTP error)")
            return 1
        if response.status_code >= 400:
            logger.error(
                "configure_network HTTP %s: %s",
                response.status_code, response.text,
            )
            return 1
        logger.info("configure_network accepted (HTTP %s)", response.status_code)

        if not interface_name:
            logger.warning(
                "interface_name not set in params; skipping validation."
            )
            return 0

        logger.info(
            "Validating network config (timeout=%ds)", CHANGE_IP_TIMEOUT
        )
        try:
            ticker(lambda: _validate_network(api, params), CHANGE_IP_TIMEOUT)
        except TimeoutError as exc:
            last = _find_interface(api, interface_name)
            logger.error(
                "Network config validation FAILED after %ds "
                "(expected changes did not happen; last state=%r): %s",
                CHANGE_IP_TIMEOUT, last, exc,
            )
            return 3
        logger.info("Network config validated")
        return 0
    finally:
        session.close()


if __name__ == "__main__":
    sys.exit(main())
