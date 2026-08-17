"""
Unified Bryck REST API client.

Single-class facade merging every endpoint from
backend.system_connectors.bryckapi_libs into one BryckApi class. All
methods accept UUID strings (or lists of UUIDs) directly — no
LogicalCard dependency.

Module-level helpers:
    ticker(callback, timeout, message, show_progress) — poll with progress bar.
    display_error(operation, status_code, ...) — formatted error display.
    extract_error_info(resp) — extract error details from API response.

For remote shell / SFTP operations against the Bryck server, use
``ssh_runner.SshRunner`` (paramiko-based, platform-independent).
"""
from __future__ import annotations

import logging
from enum import Enum, unique
from time import sleep, time
from typing import Any
from urllib.parse import urljoin

from requests import Response
from requests.exceptions import ConnectionError, HTTPError, RequestException, Timeout

from session import ApiSession

logger = logging.getLogger(__name__)


# =============================================================================
# Task enums (kept API-compatible with backend.system_connectors.bryckapi_libs)
# =============================================================================

@unique
class TaskState(Enum):
    """Task state enumeration."""
    COMPLETED = 2
    ACTIVE = 1
    STALE = 0
    FAILED = -1


@unique
class TaskType(Enum):
    """Task type enumeration."""
    TRANSFER = 0
    VERIFICATION = 1
    CAPTURE_BRYCK_STATE = 2


# =============================================================================
# Module helpers
# =============================================================================

def ticker(callback: Any, timeout: int, message: str = "", show_progress: bool = True) -> None:
    """Poll ``callback`` every second until it returns truthy or timeout.

    Args:
        callback: Zero-arg callable returning truthy when done.
        timeout: Maximum seconds to wait.
        message: Optional message to display with progress (e.g., "Formatting store").
        show_progress: If True, display progress bar and elapsed time.

    Raises:
        TimeoutError: If callback does not return truthy within ``timeout``.
    """
    import sys
    
    start = time()
    
    while not callback():
        if show_progress and timeout:
            elapsed = int(time() - start)
            
            # Animated progress indicator (cycling animation)
            bar_length = 30
            cycle_pos = elapsed % (bar_length + 5)  # Cycle through bar with wrap
            bar = '░' * bar_length
            if cycle_pos < bar_length:
                bar = bar[:cycle_pos] + '▶' + bar[cycle_pos+1:]
            
            # Display progress bar with clear timeout indication
            if message:
                progress = f"\r{message}: [{bar}] {elapsed}s elapsed (max wait: {timeout}s)"
            else:
                progress = f"\rProgress: [{bar}] {elapsed}s elapsed (max wait: {timeout}s)"
            
            sys.stdout.write(progress)
            sys.stdout.flush()
        
        sleep(1)
        
        if timeout and time() - start >= timeout:
            if show_progress:
                sys.stdout.write("\n")
                sys.stdout.flush()
            raise TimeoutError(
                f"Expected changes did not occur in {timeout} second(s)"
            )
    
    if show_progress:
        elapsed = int(time() - start)
        if message:
            sys.stdout.write(f"\r✓ {message} complete ({elapsed}s){' ' * 50}\n")
        else:
            sys.stdout.write(f"\r✓ Complete ({elapsed}s){' ' * 50}\n")
        sys.stdout.flush()


def display_error(
    operation: str,
    status_code: int | None = None,
    status_text: str = "",
    message: str = "",
    endpoint: str = "",
) -> None:
    """Display a formatted error message to the user.

    Args:
        operation: Name of the operation that failed (e.g., "Format Bryck", "Eject Bryck")
        status_code: HTTP status code (e.g., 409, 500) or None
        status_text: HTTP status text (e.g., "Conflict", "Internal Server Error")
        message: Detailed error message from the server
        endpoint: API endpoint that was called (optional)
    """
    line = "━" * 66
    print(f"\n{line}\n")
    print("❌ OPERATION FAILED\n")
    print(f"  Operation:  {operation}")
    
    if status_code:
        status_display = f"{status_code} {status_text}" if status_text else str(status_code)
        print(f"  Status:     {status_display}")
    
    if endpoint:
        print(f"  Endpoint:   {endpoint}")
    
    if message:
        print(f"\n  Error Message:")
        # Handle multi-line messages
        for line_text in message.split('\n'):
            if line_text.strip():
                print(f"    {line_text.strip()}")
    
    print(f"\n{line}\n")


def extract_error_info(resp: Response | None) -> tuple[int | None, str, str]:
    """Extract status code, status text, and error message from API response.
    
    Returns:
        Tuple of (status_code, status_text, message)
    """
    if resp is None:
        return (None, "", "Request failed")
    
    status_code = resp.status_code
    status_text = resp.reason
    message = "Unknown error"
    
    try:
        data = resp.json()
        error = data.get("error", {})
        if isinstance(error, dict):
            message = error.get("message", str(error))
        else:
            message = str(error) if error else resp.text[:500]
    except Exception:
        message = resp.text[:500] if resp.text else "No error message"
    
    return (status_code, status_text, message)


def _as_list(uuids: str | list[str]) -> list[str]:
    """Normalize a single UUID or a list of UUIDs to list[str]."""
    if isinstance(uuids, str):
        return [uuids]
    return list(uuids)


# =============================================================================
# BryckApi — unified client
# =============================================================================

class BryckApi:
    """Unified REST client for the Bryck platform.

    Wraps every endpoint originally split across BryckMgmtConfiguration,
    NetworkConfiguration, NfsConfiguration, BcloudConfiguration,
    BryckTasks, and MediaApplication.

    Uses an existing authenticated ``ApiSession`` — call ``session.login()``
    before using this client.
    """

    # URL prefixes
    _cfg_prefix = "/api/config/"
    _download_prefix = "/api/download"
    _network_prefix = "/api/network/"
    _external_storage_prefix = "/api/external_storage/"
    _bcloud_prefix = "/api/bcloud/"
    _tasks_prefix = "/api/tasks/"
    _application_prefix = "/api/application/"
    _settings_prefix = "/api/settings/"

    def __init__(self, session: ApiSession, name: str | None = None) -> None:
        """Initialize with an authenticated ApiSession.

        Args:
            session: Authenticated ApiSession instance.
            name: Optional identifier for logging.
        """
        self._session = session
        self._name = name

    @property
    def name(self) -> str | None:
        """Optional identifier."""
        return self._name

    @property
    def address(self) -> str:
        """Base URL of the underlying session."""
        return self._session.address

    # ------------------------------------------------------------------
    # Central HTTP dispatcher
    # ------------------------------------------------------------------

    def _call(
        self,
        method: str,
        url_path: str,
        data: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Response | None:
        """Centralized HTTP call with consistent error handling.

        Args:
            method: 'get' or 'post'.
            url_path: API endpoint path.
            data: JSON payload (POST) or query params (GET).
            **kwargs: Additional request kwargs.

        Returns:
            Response on success or error (for caller to check status), None on connection/timeout errors.
        """
        response: Response | None = None
        try:
            if method == "get":
                response = self._session.get(url_path, params=data, **kwargs)
            elif method == "post":
                response = self._session.post(url_path, payload=data, **kwargs)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
        except HTTPError as http_err:
            # Return the response so caller can extract error details
            resp = http_err.response
            body = ""
            if resp is not None:
                try:
                    payload = resp.json()
                    err = (
                        payload.get("error")
                        if isinstance(payload, dict) else None
                    )
                    if isinstance(err, dict) and err.get("message"):
                        body = f" | server: {err['message']}"
                    else:
                        body = f" | body: {resp.text[:500]}"
                except ValueError:
                    body = f" | body: {resp.text[:500]}"
            logger.error("[%s] HTTP error: %s%s", url_path, http_err, body)
            return resp  # Return response instead of None so caller can check status
        except ConnectionError as conn_err:
            logger.error("[%s] Connection error: %s", url_path, conn_err)
            return None
        except Timeout as timeout_err:
            logger.error("[%s] Timeout: %s", url_path, timeout_err)
            return None
        except RequestException as err:
            logger.error("[%s] Request error: %s", url_path, err)
            return None
        return response

    # ==================================================================
    # Bryck management  (/api/config/, /api/download)
    # ==================================================================

    def get_hardware_info(self) -> dict[str, Any] | None:
        """GET /api/config/info — raw hardware info payload.

        Returns:
            Parsed JSON dict, or None on error.
        """
        url_path = urljoin(self._cfg_prefix, "info")
        resp = self._call("get", url_path)
        return resp.json() if resp else None

    def bryck_info(self) -> dict[str, Any] | None:
        """GET /api/config/info — return the ``result`` sub-dict.

        Contains ``bryck_info``, ``server_info``, ``tray_info``,
        ``logical_cards``, etc.

        Returns:
            ``result`` dict, or None on error.
        """
        url_path = urljoin(self._cfg_prefix, "info")
        response = self._call("get", url_path)
        return response.json().get("result", {}) if response else None

    def format_bryck(
        self,
        uuids: str | list[str],
        store_type: str,
        raid_level: int = 0,
        key_file: str | None = None,
        acls: list[str] | None = None,
        suffix: str | None = None,
        iqn: str | None = None,
        description: str = "",
        mountonreboot: bool = False,
        IoSize: str | int | None = None,
        DataSync: str | None = None,
        encryption_option: str | None = None,
        compress: bool | None = None,
        dedup: bool | None = None,
        filestore: bool = True,
        obj: bool = False,
        filesystem: str = "zfs",
        num_vols: int | None = None,
    ) -> Response | None:
        """POST /api/config/update — format logical cards.

        Args:
            uuids: Logical-card UUID or list of UUIDs to format.
            store_type: 'FILE_STORE', 'BLOCK_STORE', or 'OBJECT_STORE'.
            raid_level: RAID level. Defaults to 0.
            key_file: Server-side key file path (from upload_key_file).
            acls: ACL list (BLOCK_STORE only).
            suffix: Suffix (BLOCK_STORE only).
            iqn: iSCSI IQN (BLOCK_STORE only).
            description: Free-form description.
            mountonreboot: Auto-mount on reboot.
            IoSize: I/O block size.
            DataSync: DataSync mode string.
            encryption_option: Encryption option (e.g. 'AWS_KMS').
            compress: Enable compression.
            dedup: Enable deduplication.
            filestore: Create as filestore.
            obj: Object storage mode.
            filesystem: Filesystem type (default 'zfs').
            num_vols: Number of volumes.

        Returns:
            Response object, or None on error.
        """
        ids = _as_list(uuids)
        encryption_check = bool(key_file or encryption_option)

        data: dict[str, Any] = {
            "store_type": store_type,
            "uuids": ids,
            "raid_level": raid_level,
            "key_file": key_file,
            "description": description,
            "encryption_check": encryption_check,
            "encryption_option": encryption_option,
            "mountonreboot": mountonreboot,
            "IoSize": IoSize,
            "DataSync": DataSync,
            "compress": compress,
            "dedup": dedup,
            "filestore": filestore,
            "obj": obj,
            "filesystem": filesystem,
            "num_vols": num_vols,
        }

        if store_type == "BLOCK_STORE":
            data.update({"acls": acls, "suffix": suffix, "iqn": iqn})

        url_path = urljoin(self._cfg_prefix, "update")
        return self._call("post", url_path, data)

    def erase(self, uuids: str | list[str]) -> Response | None:
        """POST /api/config/reset_store — re-initialize store(s).

        Args:
            uuids: Logical-card UUID or list of UUIDs.

        Returns:
            Response, or None on error.
        """
        url_path = urljoin(self._cfg_prefix, "reset_store")
        return self._call("post", url_path, {"uuids": _as_list(uuids)})

    def eject(
        self,
        uuids: str | list[str],
        no_fs_check: str | None = None,
    ) -> Response | None:
        """POST /api/config/eject — eject logical card(s).

        Args:
            uuids: Logical-card UUID or list of UUIDs.
            no_fs_check: Skip filesystem check when set truthy.

        Returns:
            Response object, or None on connection error.
        """
        data = {"uuids": _as_list(uuids), "no_fs_check": no_fs_check}
        url_path = urljoin(self._cfg_prefix, "eject")
        response = self._call("post", url_path, data)
        return response

    def mount(
        self,
        uuids: str | list[str],
        mount_point: str,
        key_file: str | None,
        mountonreboot: bool = False,
        force_check: bool = False,
        encryption_option: str | None = None,
    ) -> Response | None:
        """POST /api/config/mount — mount logical card(s).

        Args:
            uuids: Logical-card UUID or list of UUIDs to mount.
            mount_point: Filesystem mount point path.
            key_file: Server-side key file path, or None.
            mountonreboot: Persist mount across reboots.
            force_check: Force filesystem check.
            encryption_option: Encryption option string.

        Returns:
            Response, or None on error.
        """
        ids = _as_list(uuids)
        encryption_check = bool(key_file or encryption_option)
        data = {
            "uuids": ids,
            "mount_point": mount_point,
            "key_file": key_file,
            "encryption_check": encryption_check,
            "force_mount": force_check,
            "mountonreboot": mountonreboot,
            "encryption_option": encryption_option,
        }
        url_path = urljoin(self._cfg_prefix, "mount")
        return self._call("post", url_path, data)

    def tray_info(self) -> Response | None:
        """GET /api/config/tray_info — tray info."""
        url_path = urljoin(self._cfg_prefix, "tray_info")
        return self._call("get", url_path)

    def server_info(self) -> Response | None:
        """GET /api/config/server_info — server info."""
        url_path = urljoin(self._cfg_prefix, "server_info")
        return self._call("get", url_path)

    def shutdown(self) -> Response | None:
        """POST /api/config/shutdown — shut down the Bryck."""
        url_path = urljoin(self._cfg_prefix, "shutdown")
        return self._call("post", url_path)

    def upgrade(self) -> Response | None:
        """POST /api/config/upgrade — upgrade firmware."""
        url_path = urljoin(self._cfg_prefix, "upgrade")
        return self._call("post", url_path)

    def get_logs(self, cursor: str | None = None) -> Response | None:
        """GET /api/config/getlogs — fetch system event logs.

        Args:
            cursor: Time-range filter. One of "Today", "This week",
                    "Last 30 days", or a journal cursor string.
                    When omitted the server returns its default range.

        Returns:
            Response whose JSON is a list of log-entry dicts, or None on error.
        """
        import urllib.parse
        if cursor:
            encoded = urllib.parse.quote(cursor, safe="")
            url_path = urljoin(self._cfg_prefix, f"getlogs?cursor={encoded}")
        else:
            url_path = urljoin(self._cfg_prefix, "getlogs")
        return self._call("get", url_path)

    def mark_logs_read(
        self, log_id: int | None = None, mark_all: bool = False
    ) -> Response | None:
        """POST /api/config/marklog — mark one or all log entries as read.

        Args:
            log_id:   ID of a specific log entry to mark read.
                      Ignored when ``mark_all`` is True.
            mark_all: When True, marks every log entry as read.

        Returns:
            Response, or None on error.
        """
        data: dict = {}
        if mark_all:
            data["all"] = True
        elif log_id is not None:
            data["id"] = log_id
        url_path = urljoin(self._cfg_prefix, "marklog")
        return self._call("post", url_path, data)

    def scan(self, uuids: str | list[str]) -> Response | None:
        """POST /api/config/scan — scan logical card(s) for stores.

        Args:
            uuids: UUID or list of UUIDs.

        Returns:
            Response, or None on error.
        """
        url_path = urljoin(self._cfg_prefix, "scan")
        return self._call("post", url_path, {"uuids": _as_list(uuids)})

    def remove(self, uuids: str | list[str]) -> Response | None:
        """POST /api/config/remove — remove logical card(s).

        Args:
            uuids: UUID or list of UUIDs.

        Returns:
            Response, or None on error.
        """
        url_path = urljoin(self._cfg_prefix, "remove")
        return self._call("post", url_path, {"uuids": _as_list(uuids)})

    def get_client_package(self, package_type: str) -> Response | None:
        """GET /api/download?name=bryckcp_client&type=<package_type>.

        Args:
            package_type: e.g. 'deb', 'rpm'.
        """
        url_path = urljoin(
            self._download_prefix, f"?name=bryckcp_client&type={package_type}"
        )
        return self._call("get", url_path)

    def download_bryck_report(self) -> Response | None:
        """GET /api/download?name=bryck_report — download diagnostic report."""
        url_path = urljoin(self._download_prefix, "?name=bryck_report")
        return self._call("get", url_path)

    def download_cloud_transfer_log(self, transfer_id: str) -> Response | None:
        """GET /api/download?name=cloud_log&type=<transfer_id> — download log.

        Args:
            transfer_id: Cloud transfer identifier.
        """
        url_path = urljoin(
            self._download_prefix, f"?name=cloud_log&type={transfer_id}"
        )
        return self._call("get", url_path, stream=True)

    # ---- Object-store IPs / buckets / access keys --------------------

    def list_object_ip(self) -> Response | None:
        """GET /api/config/list_object_ip — list object-store IP interfaces."""
        url_path = urljoin(self._cfg_prefix, "list_object_ip")
        return self._call("get", url_path)

    def add_object_ip(self, interface: str) -> Response | None:
        """POST /api/config/add_object_ip — add an object-store IP interface.

        Args:
            interface: Network interface name.
        """
        url_path = urljoin(self._cfg_prefix, "add_object_ip")
        return self._call("post", url_path, {"interface": interface})

    def create_object_store_bucket(
        self, bucket_name: str | None = None
    ) -> Response | None:
        """POST /api/config/create_bucket — create an object-store bucket."""
        url_path = urljoin(self._cfg_prefix, "create_bucket")
        return self._call("post", url_path, {"bucket_name": bucket_name})

    def delete_object_store_bucket(
        self, bucket_name: str | None = None
    ) -> Response | None:
        """POST /api/config/delete_bucket — delete an object-store bucket."""
        url_path = urljoin(self._cfg_prefix, "delete_bucket")
        return self._call("post", url_path, {"bucket_name": bucket_name})

    def get_object_store_bucket_list(self) -> Response | None:
        """GET /api/config/list_bucket — list all object-store buckets."""
        url_path = urljoin(self._cfg_prefix, "list_bucket")
        return self._call("get", url_path)

    def create_object_store_access_key(
        self,
        access_key: str | None = None,
        secret_key: str | None = None,
    ) -> Response | None:
        """POST /api/config/create_key — create object-store access keys.

        Args:
            access_key: Access key string (auto-generated if None).
            secret_key: Secret key string (auto-generated if None).
        """
        data = {"accessKey": access_key, "secretKey": secret_key}
        url_path = urljoin(self._cfg_prefix, "create_key")
        return self._call("post", url_path, data)

    def delete_object_store_access_key(self, access_key: str) -> Response | None:
        """POST /api/config/delete_key — delete an object-store access key."""
        url_path = urljoin(self._cfg_prefix, "delete_key")
        return self._call("post", url_path, {"access_key": access_key})

    def get_object_store_access_key_list(self) -> Response | None:
        """GET /api/config/list_keys — list all object-store access keys."""
        url_path = urljoin(self._cfg_prefix, "list_keys")
        return self._call("get", url_path)

    # ---- NTP / logs / alerts / email ---------------------------------

    def configure_ntp(
        self,
        uuids: str | list[str],
        ntp_server: str,
    ) -> Response | None:
        """POST /api/config/configure_ntp — configure NTP on logical cards."""
        data = {"uuids": _as_list(uuids), "ntp_server": ntp_server}
        url_path = urljoin(self._cfg_prefix, "configure_ntp")
        return self._call("post", url_path, data)

    def marklog(
        self,
        id: str | None = None,
        all: bool | None = None,
    ) -> Response | None:
        """POST /api/config/marklog — mark log entries as read.

        Args:
            id: Specific log entry ID, or None.
            all: Mark all entries when True.
        """
        data = {"id": id, "all": all}
        url_path = urljoin(self._cfg_prefix, "marklog")
        return self._call("post", url_path, data)

    def getlogs(self, cursor: str | None = None) -> Response | None:
        """GET /api/config/getlogs — fetch log entries.

        Args:
            cursor: Pagination cursor.
        """
        url_path = urljoin(self._cfg_prefix, "getlogs")
        return self._call("get", url_path, {"cursor": cursor})

    def alert_user(
        self,
        user: str,
        mailid: str,
        alert_type: str,
    ) -> Response | None:
        """POST /api/config/alert_user — configure an alert recipient."""
        data = {"user": user, "mailid": mailid, "alert_type": alert_type}
        url_path = urljoin(self._cfg_prefix, "alert_user")
        return self._call("post", url_path, data)

    def alert_user_delete(self, mailid: str) -> Response | None:
        """POST /api/config/alert_user_delete — delete an alert recipient."""
        url_path = urljoin(self._cfg_prefix, "alert_user_delete")
        return self._call("post", url_path, {"mailid": mailid})

    def alert_user_list(self) -> Response | None:
        """GET /api/config/alert_user_list — list all alert recipients."""
        url_path = urljoin(self._cfg_prefix, "alert_user_list")
        return self._call("get", url_path)

    def config_email_sender(
        self,
        email_type: str,
        email_id: str,
        email_pass: str,
        smtp_url: str,
        smtp_port: int,
        imap_url: str,
        imap_port: int,
    ) -> Response | None:
        """POST /api/config/config_email_sender — configure alert email sender."""
        data = {
            "email_type": email_type,
            "email_id": email_id,
            "email_pass": email_pass,
            "smtp_url": smtp_url,
            "smtp_port": smtp_port,
            "imap_url": imap_url,
            "imap_port": imap_port,
        }
        url_path = urljoin(self._cfg_prefix, "config_email_sender")
        return self._call("post", url_path, data)

    def list_email_sender(self) -> Response | None:
        """POST /api/config/list_email_sender — list configured email senders."""
        url_path = urljoin(self._cfg_prefix, "list_email_sender")
        return self._call("post", url_path)

    def del_email_sender(self) -> Response | None:
        """POST /api/config/del_email_sender — delete the configured email sender."""
        url_path = urljoin(self._cfg_prefix, "del_email_sender")
        return self._call("post", url_path)

    # ==================================================================
    # Network  (/api/network/)
    # ==================================================================

    def network_info(
        self, uuids: str | list[str]
    ) -> dict[str, Any] | None:
        """GET /api/network/info — network info for given UUIDs.

        Args:
            uuids: Logical-card UUID or list of UUIDs to filter the result.

        Returns:
            Dict keyed by UUID, or None on error.
        """
        ids = _as_list(uuids)
        url_path = urljoin(self._network_prefix, "info")
        resp = self._call("get", url_path)
        if resp is None:
            return None
        result = resp.json().get("result", {})
        return {i: result[i] for i in ids if i in result}

    def configure_network(
        self,
        uuids: str | list[str],
        interface_name: str | None = None,
        dhcp: bool | None = None,
        ip: str | None = None,
        netmask: str | None = None,
        gateway: str | None = None,
        nameservers: list[str] | None = None,
        ntp_server: str | None = None,
        mtu: int | None = None,
    ) -> Response | None:
        """POST /api/network/configure — configure network on logical cards.

        Args:
            uuids: Logical-card UUID or list of UUIDs.
            interface_name: Target NIC name (e.g. ``eth0``).
            dhcp: Enable DHCP. When True, ip/netmask/gateway are ignored.
            ip: Static IPv4 address.
            netmask: IPv4 netmask (dotted or prefix, per API).
            gateway: Default gateway.
            nameservers: DNS servers.
            ntp_server: NTP server hostname / IP.
            mtu: Interface MTU in bytes.
        """
        url_path = urljoin(self._network_prefix, "configure")
        return self._call(
            "post",
            url_path,
            {
                "uuids": _as_list(uuids),
                "interface_name": interface_name,
                "dhcp": dhcp,
                "ip": ip,
                "netmask": netmask,
                "gateway": gateway,
                "nameservers": nameservers,
                "ntp_server": ntp_server,
                "mtu": mtu,
            },
        )

    # ==================================================================
    # Settings  (/api/settings/)
    # ==================================================================

    def set_date(
        self,
        option: str,
        date: str | None = None,
        time: str | None = None,
        ntp_server: str | None = None,
    ) -> Response | None:
        """POST /api/settings/set_date — set system date/time or NTP.

        Args:
            option: 'Manual' or 'NTP'.
            date: Manual date 'MM/DD/YYYY' (Manual only; null for NTP).
            time: Manual time 'HH:MM:SS' 24h (Manual only; null for NTP).
            ntp_server: NTP server hostname (NTP mode).
        """
        url_path = urljoin(self._settings_prefix, "set_date")
        return self._call(
            "post",
            url_path,
            {
                "option": option,
                "date": date,
                "time": time,
                "ntp_server": ntp_server,
            },
        )

    # ==================================================================
    # NFS external storage  (/api/external_storage/)
    # ==================================================================

    def nfs_mount(
        self,
        uuids: str | list[str],
        host: str,
        export_path: str,
        mount_point: str,
    ) -> Response | None:
        """POST /api/external_storage/mount — mount an NFS export.

        Args:
            uuids: Logical cards receiving the mount.
            host: NFS server host / IP.
            export_path: Server-side export path.
            mount_point: Local mount point on the Bryck.
        """
        url_path = urljoin(self._external_storage_prefix, "mount")
        return self._call(
            "post",
            url_path,
            {
                "export_path": export_path,
                "mount_point": mount_point,
                "remote_address": host,
                "uuids": _as_list(uuids),
            },
        )

    def nfs_unmount(
        self,
        uuids: str | list[str],
        mount_point: str,
    ) -> Response | None:
        """POST /api/external_storage/unmount — unmount an NFS export.

        Args:
            uuids: Logical cards to unmount from.
            mount_point: Mount point path to unmount.
        """
        url_path = urljoin(self._external_storage_prefix, "unmount")
        return self._call(
            "post",
            url_path,
            {"mount_point": mount_point, "uuids": _as_list(uuids)},
        )

    # ==================================================================
    # Cloud  (/api/bcloud/)
    # ==================================================================

    def configure_cloud(
        self,
        bcloud_type: str,
        username: str | None = None,
        keyid: str | None = None,
        region: str | None = None,
        keyfile: str | None = None,
        tenant_id: str | None = None,
    ) -> Response | None:
        """POST /api/bcloud/config — configure a cloud provider."""
        data = {
            "bcloud_type": bcloud_type,
            "username": username,
            "keyid": keyid,
            "keyfile": keyfile,
            "region": region,
            "tenant_id": tenant_id,
        }
        url_path = urljoin(self._bcloud_prefix, "config")
        return self._call("post", url_path, data)

    def get_cloud_config_list(self) -> Response | None:
        """GET /api/bcloud/config_list — list all cloud configurations."""
        url_path = urljoin(self._bcloud_prefix, "config_list")
        return self._call("get", url_path)

    def remove_cloud_config(self, bcloud_type: str) -> Response | None:
        """POST /api/bcloud/config_remove — remove a cloud configuration."""
        url_path = urljoin(self._bcloud_prefix, "config_remove")
        return self._call("post", url_path, {"bcloud_type": bcloud_type})

    def initiate_cloud_transfer(
        self, cloud_type: str, src: str, dst: str
    ) -> Response | None:
        """POST /api/bcloud/transfer — start a cloud transfer."""
        data = {"cloud_type": cloud_type, "src": src, "dst": dst}
        url_path = urljoin(self._bcloud_prefix, "transfer")
        return self._call("post", url_path, data)

    def pause_cloud_transfer(self, transfer_id: str) -> Response | None:
        """POST /api/bcloud/pause_transfer — pause a cloud transfer."""
        url_path = urljoin(self._bcloud_prefix, "pause_transfer")
        return self._call("post", url_path, {"transfer_id": transfer_id})

    def resume_cloud_transfer(self, transfer_id: str) -> Response | None:
        """POST /api/bcloud/resume_transfer — resume a paused cloud transfer."""
        url_path = urljoin(self._bcloud_prefix, "resume_transfer")
        return self._call("post", url_path, {"transfer_id": transfer_id})

    def cancel_cloud_transfer(self, transfer_id: str) -> Response | None:
        """POST /api/bcloud/cancel_transfer — cancel a cloud transfer."""
        url_path = urljoin(self._bcloud_prefix, "cancel_transfer")
        return self._call("post", url_path, {"transfer_id": transfer_id})

    def get_cloud_transfer_status(self, transfer_id: str) -> Response | None:
        """GET /api/bcloud/status_transfer — get status of one cloud transfer."""
        url_path = urljoin(self._bcloud_prefix, "status_transfer")
        return self._call("get", url_path, {"transfer_id": transfer_id})

    def get_list_of_cloud_transfers(
        self, transfer_state: str = "ALL"
    ) -> Response | None:
        """POST /api/bcloud/list_transfer — list cloud transfers by state."""
        url_path = urljoin(self._bcloud_prefix, "list_transfer")
        return self._call("post", url_path, {"transfer_state": transfer_state})

    # ------------------------------------------------------------------
    # Notifications  (/api/bcloud/notification_*)
    # ------------------------------------------------------------------

    def notification_setup(
        self,
        sns_topic: str | None = None,
        sqs_queue: str | None = None,
        emails: list[str] | None = None,
        states: list[str] | None = None,
    ) -> Response | None:
        """POST /api/bcloud/notification_setup — configure notifications."""
        data = {
            "sns_topic": sns_topic,
            "sqs_queue": sqs_queue,
            "emails": emails,
            "states": states,
        }
        url_path = urljoin(self._bcloud_prefix, "notification_setup")
        return self._call("post", url_path, data)

    def notification_list(self) -> Response | None:
        """GET /api/bcloud/notification_list — get notification configuration."""
        url_path = urljoin(self._bcloud_prefix, "notification_list")
        return self._call("get", url_path)

    def notification_subscribe(self, emails: list[str]) -> Response | None:
        """POST /api/bcloud/notification_subscribe — subscribe emails."""
        url_path = urljoin(self._bcloud_prefix, "notification_subscribe")
        return self._call("post", url_path, {"emails": emails})

    def notification_unsubscribe(self, email: str) -> Response | None:
        """POST /api/bcloud/notification_unsubscribe — unsubscribe email."""
        url_path = urljoin(self._bcloud_prefix, "notification_unsubscribe")
        return self._call("post", url_path, {"email": email})

    def notification_subscribers(self) -> Response | None:
        """GET /api/bcloud/notification_subscribers — get list of subscribers."""
        url_path = urljoin(self._bcloud_prefix, "notification_subscribers")
        return self._call("get", url_path)

    def notification_test(
        self,
        transfer_id: str | None = None,
        state: str | None = None,
        message: str | None = None,
    ) -> Response | None:
        """POST /api/bcloud/notification_test — send test notification."""
        data = {
            "transfer_id": transfer_id,
            "state": state,
            "message": message,
        }
        url_path = urljoin(self._bcloud_prefix, "notification_test")
        return self._call("post", url_path, data)

    def notification_enable(self) -> Response | None:
        """POST /api/bcloud/notification_enable — enable notifications."""
        url_path = urljoin(self._bcloud_prefix, "notification_enable")
        return self._call("post", url_path, {})

    def notification_disable(self) -> Response | None:
        """POST /api/bcloud/notification_disable — disable notifications."""
        url_path = urljoin(self._bcloud_prefix, "notification_disable")
        return self._call("post", url_path, {})

    def notification_delete(self) -> Response | None:
        """POST /api/bcloud/notification_delete — delete notification configuration."""
        url_path = urljoin(self._bcloud_prefix, "notification_delete")
        return self._call("post", url_path, {})

    # ==================================================================
    # Tasks  (/api/tasks/)
    # ==================================================================

    def tasks_get(self, task_type: TaskType) -> dict[str, Any] | None:
        """GET /api/tasks/list?task_type=<name> — list tasks of a given type."""
        url_path = urljoin(self._tasks_prefix, f"list?task_type={task_type.name}")
        resp = self._call("get", url_path)
        return resp.json() if resp else None

    def tasks_reset_stats(
        self,
        lc: str,
        task_id: str | None,
        task_type: TaskType,
        task_states: list[TaskState] | None,
    ) -> Response | None:
        """POST /api/tasks/dismiss — reset / dismiss task statistics.

        Args:
            lc: Logical-card hostname.
            task_id: Specific task ID, or None for all.
            task_type: Task type.
            task_states: State filter, or None for all.
        """
        url_path = urljoin(self._tasks_prefix, "dismiss")
        data: dict[str, Any] = {"logical_card": lc, "task_type": task_type.name}
        if task_states:
            data["states"] = [s.name for s in task_states]
        if task_id:
            data["task_id"] = task_id
        return self._call("post", url_path, data)

    def tasks_transfer(
        self, hostname: str, src: str, dst: str
    ) -> Response | None:
        """POST /api/tasks/transfer — start a data-transfer task."""
        url_path = urljoin(self._tasks_prefix, "transfer")
        return self._call(
            "post",
            url_path,
            {"src": src, "dst": dst, "logical_card": hostname},
        )

    def start_bryck_report_generate(self) -> Response | None:
        """POST /api/tasks/capture_bryck_state — start diagnostic capture."""
        url_path = urljoin(self._tasks_prefix, "capture_bryck_state")
        return self._call("post", url_path)

    def check_bryck_report_generate(self) -> Response | None:
        """GET /api/tasks/list?task_type=CAPTURE_BRYCK_STATE — capture status."""
        url_path = urljoin(
            self._tasks_prefix, "list?task_type=CAPTURE_BRYCK_STATE"
        )
        return self._call("get", url_path)

    # ==================================================================
    # Media application  (/api/application/)
    # ==================================================================

    def add_media(
        self,
        media_type: str,
        destination: str,
        clip_id: str,
        reel_id: str,
        media_name: str,
        ip_address: str | None = None,
        file_size: int | None = None,
        port: int | None = None,
        session_type: str | None = None,
        payload_type: str | None = None,
        video_format: str | None = None,
        pg_format: str | None = None,
        audio_format: str | None = None,
        audio_sampling: str | None = None,
    ) -> Response | None:
        """POST /api/application/add_media — add a new media stream."""
        data = {
            "media_type": media_type,
            "media_name": media_name,
            "ip_address": ip_address,
            "destination": destination,
            "clip_id": clip_id,
            "reel_id": reel_id,
            "port": port,
            "file_size": file_size,
            "session_type": session_type,
            "payload_type": payload_type,
            "video_format": video_format,
            "pg_format": pg_format,
            "audio_format": audio_format,
            "audio_sampling": audio_sampling,
        }
        url_path = urljoin(self._application_prefix, "add_media")
        return self._call("post", url_path, data)

    def edit_media(
        self,
        media_id: str,
        media_type: str,
        destination: str,
        clip_id: str,
        reel_id: str,
        media_name: str,
        ip_address: str | None = None,
        file_size: int | None = None,
        port: int | None = None,
        session_type: str | None = None,
        payload_type: str | None = None,
        video_format: str | None = None,
        pg_format: str | None = None,
        audio_format: str | None = None,
        audio_sampling: str | None = None,
    ) -> Response | None:
        """POST /api/application/edit_media — edit an existing media stream."""
        data = {
            "media_id": media_id,
            "media_type": media_type,
            "media_name": media_name,
            "ip_address": ip_address,
            "destination": destination,
            "clip_id": clip_id,
            "reel_id": reel_id,
            "port": port,
            "file_size": file_size,
            "session_type": session_type,
            "payload_type": payload_type,
            "video_format": video_format,
            "pg_format": pg_format,
            "audio_format": audio_format,
            "audio_sampling": audio_sampling,
        }
        url_path = urljoin(self._application_prefix, "edit_media")
        return self._call("post", url_path, data)

    def list_media(self) -> Response | None:
        """GET /api/application/list_media — list all media streams."""
        url_path = urljoin(self._application_prefix, "list_media")
        return self._call("get", url_path)

    def pause_media(
        self,
        media_id: str,
        media_type: str,
        session_type: str | None = None,
        video_format: str | None = None,
        pg_format: str | None = None,
        audio_format: str | None = None,
        audio_sampling: str | None = None,
        address: str | None = None,
        port: int | None = None,
        stream_dir: str | None = None,
        clipid: str | None = None,
        reelid: str | None = None,
        pause: bool = True,
    ) -> Response | None:
        """POST /api/application/pause_media — pause a media stream."""
        data = {
            "media_id": media_id,
            "cam_type": media_type,
            "session_type": session_type,
            "video_format": video_format,
            "pg_format": pg_format,
            "audio_format": audio_format,
            "audio_sampling": audio_sampling,
            "address": address,
            "port": port,
            "stream_dir": stream_dir,
            "clipid": clipid,
            "reelid": reelid,
            "pause": pause,
        }
        url_path = urljoin(self._application_prefix, "pause_media")
        return self._call("post", url_path, data)

    def resume_media(
        self,
        media_id: str,
        media_type: str,
        session_type: str | None = None,
        video_format: str | None = None,
        pg_format: str | None = None,
        audio_format: str | None = None,
        audio_sampling: str | None = None,
        address: str | None = None,
        port: int | None = None,
        stream_dir: str | None = None,
        clipid: str | None = None,
        reelid: str | None = None,
    ) -> Response | None:
        """POST /api/application/resume_media — resume a paused stream."""
        data = {
            "media_id": media_id,
            "cam_type": media_type,
            "session_type": session_type,
            "video_format": video_format,
            "pg_format": pg_format,
            "audio_format": audio_format,
            "audio_sampling": audio_sampling,
            "address": address,
            "port": port,
            "stream_dir": stream_dir,
            "clipid": clipid,
            "reelid": reelid,
        }
        url_path = urljoin(self._application_prefix, "resume_media")
        return self._call("post", url_path, data)

    def remove_media(
        self,
        media_id: str,
        media_type: str,
        session_type: str | None = None,
        video_format: str | None = None,
        pg_format: str | None = None,
        audio_format: str | None = None,
        audio_sampling: str | None = None,
        address: str | None = None,
        port: int | None = None,
        stream_dir: str | None = None,
        clipid: str | None = None,
        reelid: str | None = None,
        pause: bool = False,
    ) -> Response | None:
        """POST /api/application/remove_media — remove a media stream."""
        data = {
            "media_id": media_id,
            "cam_type": media_type,
            "session_type": session_type,
            "video_format": video_format,
            "pg_format": pg_format,
            "audio_format": audio_format,
            "audio_sampling": audio_sampling,
            "address": address,
            "port": port,
            "stream_dir": stream_dir,
            "clipid": clipid,
            "reelid": reelid,
            "pause": pause,
        }
        url_path = urljoin(self._application_prefix, "remove_media")
        return self._call("post", url_path, data)
