#!/usr/bin/env python3
"""
Standalone Bryck date/time configuration runner.

Flow:
    1. Load login.json and change_time_params.json from the script directory.
    2. Log in to the Bryck REST API.
    3. Build payload based on ``option``:
         - "NTP":    {option:"NTP", date:null, time:null,
                      ntp_server:"time.google.com"}  (fixed)
         - "Manual": all fields from JSON verbatim.
    4. POST /api/settings/set_date.
    5. Manual: poll result.server_info.server_time until it matches
       (set_moment + elapsed) within tolerance. NTP: skip validation.

Usage:
    python3 change_time.py [--login PATH] [--params PATH]

Exit codes:
    0 = success
    1 = HTTP / set_date call failed
    2 = params error (bad option / missing fields)
    3 = validation timed out
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time as _time
from datetime import datetime, timedelta
from typing import Any

from bryck_api import BryckApi, ticker
from session import ApiSession

logger = logging.getLogger(__name__)

DEFAULT_LOGIN_JSON = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "login.json"
)
DEFAULT_PARAMS_JSON = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "change_time_params.json"
)

NTP_SERVER = "time.google.com"
CHANGE_TIME_TIMEOUT = 60
TIME_TOLERANCE_SECONDS = 120


def _parse_set_moment(date_str: str, time_str: str) -> datetime:
    """Parse 'MM/DD/YYYY' + 'HH:MM:SS' into a naive datetime."""
    return datetime.strptime(f"{date_str} {time_str}", "%m/%d/%Y %H:%M:%S")


def _parse_server_time(server_time: str) -> datetime | None:
    """Parse '2026-07-17 09:30:42' into a naive datetime."""
    try:
        return datetime.strptime(server_time, "%Y-%m-%d %H:%M:%S")
    except (TypeError, ValueError):
        return None


def _server_time(api: BryckApi) -> str | None:
    try:
        info = api.bryck_info()
    except Exception:
        logger.debug("bryck_info() raised during validation", exc_info=True)
        return None
    if not info:
        return None
    return info.get("server_info", {}).get("server_time")


def _validate_manual_time(
    api: BryckApi, set_moment: datetime, call_epoch: float
) -> bool:
    """True when server_time is within TIME_TOLERANCE_SECONDS of expected."""
    server_time_str = _server_time(api)
    if not server_time_str:
        return False
    server_dt = _parse_server_time(server_time_str)
    if server_dt is None:
        logger.debug("Unparseable server_time: %r", server_time_str)
        return False
    elapsed = _time.time() - call_epoch
    expected = set_moment + timedelta(seconds=elapsed)
    diff = abs((server_dt - expected).total_seconds())
    logger.debug(
        "server_time=%s expected≈%s diff=%.1fs",
        server_dt, expected, diff,
    )
    return diff <= TIME_TOLERANCE_SECONDS


def _build_payload(params: dict[str, Any]) -> dict[str, Any] | None:
    option = params.get("option")
    if option == "NTP":
        return {
            "option": "NTP",
            "date": None,
            "time": None,
            "ntp_server": NTP_SERVER,
        }
    if option == "Manual":
        if not params.get("date") or not params.get("time"):
            logger.error("Manual mode requires 'date' and 'time' in params")
            return None
        return {
            "option": "Manual",
            "date": params.get("date"),
            "time": params.get("time"),
            "ntp_server": params.get("ntp_server"),
        }
    logger.error("Unknown option %r (expected 'Manual' or 'NTP')", option)
    return None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Configure Bryck date/time via /api/settings/set_date."
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

    payload = _build_payload(params)
    if payload is None:
        return 2

    session = ApiSession.from_login_json(args.login)
    try:
        session.login()
        api = BryckApi(session)

        logger.info("Sending set_date payload: %s", payload)
        response = api.set_date(
            option=payload["option"],
            date=payload["date"],
            time=payload["time"],
            ntp_server=payload["ntp_server"],
        )
        call_epoch = _time.time()
        if response is None:
            logger.error("set_date returned no response (HTTP error)")
            return 1
        if response.status_code >= 400:
            logger.error(
                "set_date HTTP %s: %s", response.status_code, response.text
            )
            return 1
        logger.info("set_date accepted (HTTP %s)", response.status_code)

        if payload["option"] != "Manual":
            logger.info("Option=%s; skipping validation.", payload["option"])
            return 0

        # The clock may have jumped (forward or backward). The JWT we
        # obtained before set_date now has an `iat` that is out of sync
        # with the new server time; flask-jwt rejects such tokens with
        # HTTP 401 ("Signature has expired" when clock jumped forward)
        # or HTTP 422 ("Token is not yet valid" when clock jumped
        # backward). Re-login to get a fresh token before polling.
        logger.info("Re-authenticating after time change to refresh JWT")
        try:
            session.login()
        except Exception as exc:
            logger.error("Re-login after set_date failed: %s", exc)
            return 1

        set_moment = _parse_set_moment(payload["date"], payload["time"])
        logger.info(
            "Validating Manual time (target=%s ±%ds, timeout=%ds)",
            set_moment, TIME_TOLERANCE_SECONDS, CHANGE_TIME_TIMEOUT,
        )
        try:
            ticker(
                lambda: _validate_manual_time(api, set_moment, call_epoch),
                CHANGE_TIME_TIMEOUT,
            )
        except TimeoutError as exc:
            last = _server_time(api)
            logger.error(
                "Time validation FAILED after %ds "
                "(expected changes did not happen; last server_time=%r): %s",
                CHANGE_TIME_TIMEOUT, last, exc,
            )
            return 3
        logger.info("Time change validated")
        return 0
    finally:
        session.close()


if __name__ == "__main__":
    sys.exit(main())
