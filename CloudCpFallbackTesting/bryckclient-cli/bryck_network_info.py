#!/usr/bin/env python3
"""
Standalone Bryck network info runner.

Flow:
    1. Load login.json from the current directory.
    2. Log in to the Bryck REST API.
    3. Call GET /api/config/info and print
       ``result.server_info.ethernet`` (network details) as pretty
       JSON to stdout.

Usage:
    python3 bryck_network_info.py [--login PATH] [--output PATH]
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
        description=(
            "Display server_info.ethernet (network details) from "
            "/api/config/info."
        ),
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
        result = api.bryck_info()
        if result is None:
            logger.error("Failed to fetch /api/config/info")
            return 1

        ethernet = result.get("server_info", {}).get("ethernet")
        if ethernet is None:
            logger.error("server_info.ethernet missing from response")
            return 1

        pretty = json.dumps(ethernet, indent=2, sort_keys=True, default=str)
        print(pretty)

        if args.output:
            try:
                with open(args.output, "w", encoding="utf-8") as fh:
                    fh.write(pretty)
                    fh.write("\n")
                logger.info("Wrote network info to %s", args.output)
            except OSError as exc:
                logger.error("Failed to write %s: %s", args.output, exc)
                return 1

        return 0
    finally:
        session.close()


if __name__ == "__main__":
    sys.exit(main())
