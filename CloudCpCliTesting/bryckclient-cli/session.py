"""
Standalone REST API session module for Bryck platform.

Mirrors backend.system_connectors.restful_client.ApiSession with zero
backend dependencies. Provides JWT-authenticated session management via
/api/auth and a factory helper to build a session from login.json.
"""
from __future__ import annotations

import ipaddress
import json
import logging
import time
from pathlib import Path
from typing import Any

import requests
import urllib3

logger = logging.getLogger(__name__)


# =============================================================================
# Module helpers
# =============================================================================

def _validate_ipv4(host: str) -> str:
    """Ensure ``host`` is a valid IPv4 address (rejects hostnames like 'localhost').

    Args:
        host: Value from ``bryckapi_host``.

    Returns:
        The same address, unchanged, once validated.

    Raises:
        ValueError: If ``host`` is not a well-formed IPv4 address.
    """
    try:
        ipaddress.IPv4Address(host)
    except (ipaddress.AddressValueError, ValueError) as exc:
        raise ValueError(
            f"bryckapi_host must be an IPv4 address, got {host!r} "
            f"({exc}). Hostnames such as 'localhost' are no longer accepted "
            f"because the runners must be portable across machines."
        ) from exc
    return host


def _coerce_port(scheme: str, port: int | str | None) -> int:
    """Return a port compatible with ``scheme``, auto-correcting known mismatches.

    Rules:
        * ``scheme='https'`` with port 80 -> corrected to 443 (INFO log).
        * ``scheme='http'``  with port 443 -> corrected to 80  (INFO log).
        * Any other combination is returned as-is.
        * ``port=None`` -> 443 for https, 80 for http.

    Args:
        scheme: 'http' or 'https'.
        port: Requested port (int, numeric string, or None).

    Returns:
        Final port to use (always int).
    """
    if port is None:
        return 443 if scheme == "https" else 80

    try:
        port_int = int(port)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"bryckapi_port must be numeric, got {port!r}") from exc

    if scheme == "https" and port_int == 80:
        logger.info("Correcting port 80 -> 443 for scheme=https")
        return 443
    if scheme == "http" and port_int == 443:
        logger.info("Correcting port 443 -> 80 for scheme=http")
        return 80
    return port_int


class ApiSession:
    """REST API session with JWT authentication via /api/auth.

    Supports context manager for automatic cleanup:
        with ApiSession(host, ...) as session:
            session.login()
            response = session.get("/api/config/info")

    Examples:
        ApiSession(host='10.0.0.1', username='admin', password='pass')
        ApiSession(host='10.0.0.1', port=80, scheme='http', username='admin', password='pass')
        ApiSession(host='10.0.0.1', scheme='https', verify=True, username='admin', password='pass')
    """

    def __init__(
        self,
        host: str,
        port: int | None = None,
        scheme: str = "http",
        username: str = "admin",
        password: str = "admin",
        timeout: int = 30,
        max_retries: int = 3,
        verify: bool | str = False,
    ) -> None:
        """Initialize API session.

        Args:
            host: Target host IP or hostname.
            port: Port number (defaults to 80 for http, 443 for https).
            scheme: Protocol scheme ('http' or 'https').
            username: Login username.
            password: Login password.
            timeout: Request timeout in seconds.
            max_retries: Number of retries for transient failures.
            verify: SSL verification setting (False, True, or CA bundle path).
        """
        self.host = _validate_ipv4(host)
        self.scheme = scheme
        self.port = _coerce_port(scheme, port)

        self.username = username
        self.password = password
        self.timeout = int(timeout)
        self.max_retries = int(max_retries)
        self.verify = verify
        self.base_url = f"{scheme}://{self.host}:{self.port}"
        self.token: str | None = None
        self.headers: dict[str, str] = {"Content-Type": "application/json"}

        # SSH credentials for the Bryck server (populated by from_login_json;
        # left as None for constructions that don't need SSH access).
        self.ssh_username: str | None = None
        self.ssh_password: str | None = None

        if verify is False:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        self._session = requests.Session()
        self._session.verify = verify

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @classmethod
    def from_login_json(cls, path: str | Path) -> ApiSession:
        """Build an ApiSession from a login.json file.

        Expected JSON keys:
            bryckapi_host, bryckapi_scheme, bryckapi_port,
            bryckapi_username, bryckapi_password, timeout,
            bryckserver_username, bryckserver_password

        ``bryckapi_host`` MUST be an IPv4 address (hostnames such as
        ``localhost`` are rejected). ``bryckapi_scheme`` / ``bryckapi_port``
        are auto-corrected when they disagree with the well-known ports
        (80/http, 443/https). ``bryckserver_username`` /
        ``bryckserver_password`` are optional here but required by any
        SSH-based operation (key-file upload, remote validators).

        Args:
            path: Path to the login.json file.

        Returns:
            Configured (but not yet logged-in) ApiSession instance.
        """
        with open(path, "r", encoding="utf-8") as fh:
            cfg = json.load(fh)

        session = cls(
            host=cfg["bryckapi_host"],
            port=cfg.get("bryckapi_port"),
            scheme=cfg.get("bryckapi_scheme", "http"),
            username=cfg.get("bryckapi_username", "admin"),
            password=cfg.get("bryckapi_password", "admin"),
            timeout=int(cfg.get("timeout", 30)),
        )
        session.ssh_username = cfg.get("bryckserver_username")
        session.ssh_password = cfg.get("bryckserver_password")
        return session

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self) -> ApiSession:
        """Context manager entry - returns self (call login() separately if needed)."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        """Context manager exit - close session."""
        self.close()
        return False

    @property
    def address(self) -> str:
        """Return the base URL address."""
        return self.base_url

    # ------------------------------------------------------------------
    # Auth
    # ------------------------------------------------------------------

    def login(self, silent: bool = False) -> dict[str, Any]:
        """Authenticate via /api/auth and extract JWT token.

        Args:
            silent: When True, suppress all error output and logging.
                    Use for probe/validation logins where the caller
                    handles failure itself.

        Returns:
            API response data containing token.

        Raises:
            Exception: If login fails or no token in response.
        """
        url = f"{self.base_url}/api/auth"
        payload = {"username": self.username, "password": self.password}
        if not silent:
            logger.info("Logging in to %s as %s", url, self.username)

        try:
            resp = self._session.post(
                url,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=self.timeout,
            )
            resp.raise_for_status()
        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code if e.response else None
            if status_code in (401, 403, 422):
                if not silent:
                    # Import here to avoid circular dependency
                    from bryck_api import display_error
                    display_error(
                        "Login",
                        status_code,
                        "Authentication Failed",
                        "Please check your credentials (username/password).",
                        "/api/auth"
                    )
                raise Exception("Authentication failed") from e
            if not silent:
                logger.error("Login HTTP error: %s", e)
            raise
        except requests.exceptions.RequestException as e:
            if not silent:
                logger.error("Login request failed: %s", e)
            raise Exception(f"Login request failed: {e}") from e

        data = resp.json()
        self.token = data.get("token") or data.get("access_token")
        if not self.token:
            raise Exception(f"Login failed: no token in response. Response: {data}")

        self.headers["Authorization"] = f"JWT {self.token}"
        logger.info("Login successful. JWT token acquired.")
        return data

    def change_password(
        self, username: str, old_password: str, new_password: str,
        new_password_confirm: str | None = None,
    ) -> dict:
        """POST /api/auth/change_password — change the password of a user.

        Args:
            username:             The account whose password is being changed.
            old_password:         Current (existing) password.
            new_password:         Desired new password.
            new_password_confirm: Confirmation of the new password (forwarded
                                  to the API as ``new_password_confirm``).

        Returns:
            API response data as a dict.

        Raises:
            requests.exceptions.HTTPError: On 4xx/5xx responses.
            Exception: On network or unexpected errors.
        """
        url = f"{self.base_url}/api/auth/change_password"
        payload = {
            "username": username,
            "old_password": old_password,
            "new_password": new_password,
            "new_password_confirm": new_password_confirm if new_password_confirm is not None else new_password,
        }
        logger.info("Changing password for user %r at %s", username, url)
        try:
            resp = self._session.post(
                url,
                json=payload,
                headers=self.headers,
                timeout=self.timeout,
            )
            resp.raise_for_status()
        except requests.exceptions.HTTPError as e:
            logger.error("change_password HTTP error: %s", e)
            raise
        except requests.exceptions.RequestException as e:
            logger.error("change_password request failed: %s", e)
            raise Exception(f"change_password request failed: {e}") from e
        try:
            return resp.json()
        except ValueError:
            return {}

    # ------------------------------------------------------------------
    # HTTP
    # ------------------------------------------------------------------

    def _request_with_retry(
        self,
        method: str,
        url: str,
        retries: int | None = None,
        **kwargs: Any,
    ) -> requests.Response:
        """Make HTTP request with retry logic for transient failures."""
        retries = retries if retries is not None else self.max_retries
        last_exception: Exception | None = None

        for attempt in range(retries):
            try:
                resp = getattr(self._session, method)(url, **kwargs)
                resp.raise_for_status()
                return resp
            except requests.exceptions.ConnectionError as e:
                last_exception = e
                if attempt < retries - 1:
                    wait_time = 2 ** attempt
                    logger.warning(
                        "Connection error, retrying in %ds... (%d/%d)",
                        wait_time, attempt + 1, retries,
                    )
                    time.sleep(wait_time)

        raise last_exception or Exception("Request failed after retries")

    def get(
        self,
        api_path: str,
        params: dict[str, Any] | None = None,
        retry: bool = False,
        stream: bool = False,
    ) -> requests.Response:
        """Generic GET request.

        Args:
            api_path: API endpoint path (e.g. '/api/config/info').
            params: Optional query parameters dict.
            retry: Whether to retry on connection errors.
            stream: When True, do not immediately download the response
                body (used for file downloads via ``iter_content``).

        Returns:
            requests.Response
        """
        url = f"{self.base_url}{api_path}"

        if retry:
            return self._request_with_retry(
                "get", url, headers=self.headers, params=params,
                timeout=self.timeout, stream=stream,
            )

        resp = self._session.get(
            url, headers=self.headers, params=params, timeout=self.timeout,
            stream=stream,
        )
        resp.raise_for_status()
        return resp

    def post(
        self,
        api_path: str,
        payload: dict[str, Any] | None = None,
        retry: bool = False,
    ) -> requests.Response:
        """Generic POST request with JSON payload.

        Args:
            api_path: API endpoint path.
            payload: Optional JSON payload dict.
            retry: Whether to retry on connection errors.

        Returns:
            requests.Response
        """
        url = f"{self.base_url}{api_path}"

        if retry:
            return self._request_with_retry(
                "post", url, json=payload, headers=self.headers, timeout=self.timeout
            )

        resp = self._session.post(
            url, json=payload, headers=self.headers, timeout=self.timeout
        )
        resp.raise_for_status()
        return resp

    def close(self) -> None:
        """Close the underlying requests session."""
        self._session.close()
        logger.info("API session closed.")
