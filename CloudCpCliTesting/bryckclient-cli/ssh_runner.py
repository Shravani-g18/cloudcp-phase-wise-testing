"""
Paramiko-based SSH/SFTP helper for the Bryck server.

Wraps a single ``paramiko.SSHClient`` so any runner can execute remote
shell commands and push files to the Bryck without depending on being
executed *on* the Bryck. All operations share one authenticated
transport; each ``run()`` / ``put()`` opens a fresh channel.

Typical use::

    from session import ApiSession
    from ssh_runner import SshRunner

    session = ApiSession.from_login_json("login.json")
    session.login()
    with SshRunner.from_session(session) as ssh:
        rc, out, err = ssh.run(
            "/opt/bryck/.venv/bryck/bin/bryckutil --json bryck list"
        )
        ssh.put("./keyfile", "/opt/bryck/bryckapi/downloads/keyfile")
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import paramiko

if TYPE_CHECKING:
    from session import ApiSession

logger = logging.getLogger(__name__)

DEFAULT_KEY_FILE_REMOTE_PATH = "/opt/bryck/bryckapi/downloads/keyfile"
"""Fixed on-server destination for uploaded key files."""

DEFAULT_SSH_PORT = 22


class SshRunnerError(Exception):
    """Raised when an SSH transport or command dispatch fails."""


class SshRunner:
    """One-connection SSH/SFTP helper.

    Reuses a single ``paramiko.SSHClient`` across ``run()`` and ``put()``
    calls, so subsequent commands cost only a new channel (no re-auth).

    Attributes:
        host: Target IPv4 address of the Bryck server.
        username: Remote Unix username (``bryckserver_username``).
        port: SSH port (default 22).
        timeout: Transport-level connect timeout in seconds.
    """

    def __init__(
        self,
        host: str,
        username: str,
        password: str,
        port: int = DEFAULT_SSH_PORT,
        timeout: int = 15,
    ) -> None:
        """Initialize the runner (does NOT connect until :meth:`connect`).

        Args:
            host: IPv4 address of the Bryck server.
            username: Remote Unix username.
            password: Remote Unix password.
            port: SSH port. Defaults to 22.
            timeout: Connection timeout in seconds.

        Raises:
            SshRunnerError: If ``username`` or ``password`` is missing.
        """
        if not host:
            raise SshRunnerError("SshRunner requires a host address")
        if not username or not password:
            raise SshRunnerError(
                "SshRunner requires both username and password "
                "(populate bryckserver_username / bryckserver_password "
                "in login.json)."
            )
        self.host = host
        self.username = username
        self._password = password
        self.port = int(port)
        self.timeout = int(timeout)
        self._client: paramiko.SSHClient | None = None

    # ------------------------------------------------------------------
    # Factories
    # ------------------------------------------------------------------

    @classmethod
    def from_session(
        cls,
        session: ApiSession,
        port: int = DEFAULT_SSH_PORT,
        timeout: int = 15,
    ) -> SshRunner:
        """Build an SshRunner from an already-configured :class:`ApiSession`.

        Reuses ``session.host`` and the ``bryckserver_*`` credentials
        that :meth:`ApiSession.from_login_json` stashed on the session.

        Args:
            session: Configured (login-optional) ApiSession.
            port: SSH port. Defaults to 22.
            timeout: Connection timeout in seconds.
        """
        if not session.ssh_username or not session.ssh_password:
            raise SshRunnerError(
                "ApiSession has no SSH credentials; add "
                "bryckserver_username / bryckserver_password to login.json."
            )
        return cls(
            host=session.host,
            username=session.ssh_username,
            password=session.ssh_password,
            port=port,
            timeout=timeout,
        )

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    def connect(self) -> None:
        """Open the underlying SSH transport (idempotent)."""
        if self._client is not None:
            return
        client = paramiko.SSHClient()
        # Auto-accept unknown host keys. See OPERATIONS.md §9 for the
        # trade-off; switch to RejectPolicy + a known_hosts file for
        # stricter deployments.
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        logger.info(
            "Opening SSH connection to %s@%s:%d", self.username, self.host, self.port
        )
        try:
            client.connect(
                hostname=self.host,
                port=self.port,
                username=self.username,
                password=self._password,
                timeout=self.timeout,
                allow_agent=False,
                look_for_keys=False,
            )
        except (paramiko.SSHException, OSError) as exc:
            raise SshRunnerError(
                f"SSH connect to {self.username}@{self.host}:{self.port} failed: {exc}"
            ) from exc
        self._client = client

    def close(self) -> None:
        """Close the SSH transport (safe to call more than once)."""
        if self._client is not None:
            try:
                self._client.close()
            finally:
                self._client = None
            logger.info("SSH connection to %s closed", self.host)

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self) -> SshRunner:
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:  # noqa: ANN001
        self.close()
        return False

    # ------------------------------------------------------------------
    # Operations
    # ------------------------------------------------------------------

    def run(self, cmd: str, timeout: int = 60) -> tuple[int, str, str]:
        """Execute ``cmd`` on the remote host and return its result.

        The command is passed to the remote user's default shell via
        ``exec_command`` — so a plain command string is expected (NOT an
        argv list). Callers must ``shlex.quote()`` any untrusted input.

        Args:
            cmd: Command line to run remotely.
            timeout: Per-command wall clock timeout in seconds.

        Returns:
            ``(returncode, stdout, stderr)``. ``returncode`` is ``-1`` if
            the channel timed out before the command exited.

        Raises:
            SshRunnerError: If the transport is not connected or a
                paramiko-level failure occurs.
        """
        self.connect()
        assert self._client is not None  # for type-checkers
        logger.debug("[ssh %s] %s", self.host, cmd)
        try:
            _stdin, stdout, stderr = self._client.exec_command(cmd, timeout=timeout)
            out = stdout.read().decode("utf-8", errors="replace")
            err = stderr.read().decode("utf-8", errors="replace")
            rc = stdout.channel.recv_exit_status()
        except paramiko.SSHException as exc:
            raise SshRunnerError(f"SSH exec failed: {exc}") from exc
        except TimeoutError:
            logger.warning("[ssh %s] command timed out after %ss: %s", self.host, timeout, cmd)
            return -1, "", ""
        if rc != 0:
            logger.debug("[ssh %s] rc=%s stderr=%s", self.host, rc, err.strip())
        return rc, out, err

    def put(self, local_path: str, remote_path: str) -> None:
        """Upload ``local_path`` to ``remote_path`` via SFTP.

        Args:
            local_path: File on the machine running the runner.
            remote_path: Destination path on the Bryck server.

        Raises:
            SshRunnerError: If the SFTP transfer fails.
        """
        self.connect()
        assert self._client is not None  # for type-checkers
        logger.info(
            "SFTP put %s -> %s@%s:%s", local_path, self.username, self.host, remote_path
        )
        try:
            sftp = self._client.open_sftp()
            try:
                sftp.put(local_path, remote_path)
            finally:
                sftp.close()
        except (paramiko.SSHException, OSError) as exc:
            raise SshRunnerError(
                f"SFTP put {local_path!r} -> {remote_path!r} failed: {exc}"
            ) from exc

    def get(self, remote_path: str, local_path: str) -> None:
        """Download ``remote_path`` from the Bryck to ``local_path`` via SFTP.

        Args:
            remote_path: File on the Bryck server.
            local_path: Destination path on the machine running the runner.

        Raises:
            SshRunnerError: If the SFTP transfer fails.
        """
        self.connect()
        assert self._client is not None  # for type-checkers
        logger.info(
            "SFTP get %s@%s:%s -> %s", self.username, self.host, remote_path, local_path
        )
        try:
            sftp = self._client.open_sftp()
            try:
                sftp.get(remote_path, local_path)
            finally:
                sftp.close()
        except (paramiko.SSHException, OSError) as exc:
            raise SshRunnerError(
                f"SFTP get {remote_path!r} -> {local_path!r} failed: {exc}"
            ) from exc
