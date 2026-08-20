#!/usr/bin/env python3
"""
Standalone Bryck info runner.

Flow:
    1. Load login.json from the current directory.
    2. Log in to the Bryck REST API.
    3. Call GET /api/config/info and print only the ``result.bryck_info``
       section as pretty JSON to stdout.

Usage:
    python3 bryck_info.py [--login PATH] [--output PATH]
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys

from bryck_api import BryckApi
from session import ApiSession

logger = logging.getLogger(__name__)

DEFAULT_LOGIN_JSON = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "login.json"
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Display the bryck_info section from /api/config/info response.",
    )
    parser.add_argument("--login", default=DEFAULT_LOGIN_JSON)
    parser.add_argument(
        "--output",
        default=None,
        help="Optional file to write JSON to (in addition to stdout).",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    session = ApiSession.from_login_json(args.login)
    try:
        session.login()
        api = BryckApi(session)

        logger.info("Fetching /api/config/info")
        payload = api.bryck_info()
        if payload is None:
            logger.error("Failed to fetch /api/config/info")
            return 1

        # Extract only the bryck_info section
        bryck_info_data = payload.get("bryck_info", {})
        if not bryck_info_data:
            logger.warning("No bryck_info found in response")
            bryck_info_data = payload  # Fallback to full payload if bryck_info missing

        # Append current_conditions from logical_cards if available
        logical_cards = payload.get("logical_cards", {})
        if logical_cards:
            # Get the first logical card's current_conditions
            first_card_uuid = next(iter(logical_cards), None)
            if first_card_uuid:
                current_conditions = logical_cards[first_card_uuid].get("current_conditions", {})
                if current_conditions:
                    bryck_info_data["current_conditions"] = current_conditions

        pretty = json.dumps(bryck_info_data, indent=2, sort_keys=True, default=str)
        print(pretty)

        if args.output:
            try:
                with open(args.output, "w", encoding="utf-8") as fh:
                    fh.write(pretty)
                    fh.write("\n")
                logger.info("Wrote info payload to %s", args.output)
            except OSError as exc:
                logger.error("Failed to write %s: %s", args.output, exc)
                return 1

        return 0
    finally:
        session.close()


if __name__ == "__main__":
    sys.exit(main())
