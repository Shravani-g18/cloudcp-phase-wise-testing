from __future__ import annotations

import copy
import io
import json
import socket
import shlex
import stat
import time as _time
from typing import Dict, Tuple

import paramiko


def connect_ssh(host: str, username: str, password: str, port: int = 22) -> paramiko.SSHClient:
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(hostname=host, port=port, username=username, password=password, timeout=30)
    return client


def run_remote(
    client: paramiko.SSHClient,
    command: str,
    cwd: str | None = None,
    timeout_sec: float | None = None,
) -> Tuple[int, str, str]:
    final_cmd = command if not cwd else f"cd {shlex.quote(cwd)} && {command}"
    _, stdout, stderr = client.exec_command(final_cmd)
    if timeout_sec and timeout_sec > 0:
        stdout.channel.settimeout(timeout_sec)
        stderr.channel.settimeout(timeout_sec)
    try:
        out = stdout.read().decode("utf-8", errors="replace")
        err = stderr.read().decode("utf-8", errors="replace")
        code = stdout.channel.recv_exit_status()
        return code, out, err
    except socket.timeout as exc:
        stdout.channel.close()
        stderr.channel.close()
        raise TimeoutError(f"Remote command timed out after {timeout_sec}s: {final_cmd}") from exc


def expand_remote_home(client: paramiko.SSHClient, path: str) -> str:
    if not path.startswith("~/"):
        return path
    _, stdout, _ = client.exec_command("printf %s \"$HOME\"")
    home = stdout.read().decode("utf-8", errors="replace").strip()
    return f"{home}/{path[2:]}"


def ensure_remote_dir(sftp: paramiko.SFTPClient, remote_dir: str) -> None:
    try:
        attrs = sftp.stat(remote_dir)
        if not stat.S_ISDIR(attrs.st_mode):
            raise NotADirectoryError(f"Remote path exists but is not a directory: {remote_dir}")
    except FileNotFoundError as exc:
        raise FileNotFoundError(f"Remote directory not found: {remote_dir}") from exc


def count_remote_batch_files(client: paramiko.SSHClient, output_dir: str, cwd: str, timeout_sec: float | None = None) -> int:
    # Count only numbered batch files and exclude batch_summary.txt.
    cmd = f"find {shlex.quote(output_dir)} -type f -name 'batch_[0-9]*.txt' | wc -l"
    code, out, err = run_remote(client, cmd, cwd=cwd, timeout_sec=timeout_sec)
    if code != 0:
        raise RuntimeError(f"Unable to count batch files: {err.strip() or out.strip()}")
    return int(out.strip())


# ---------------------------------------------------------------------------
# Remote batch config management
# ---------------------------------------------------------------------------

def read_remote_json_config(client: paramiko.SSHClient, remote_path: str) -> Dict[str, object]:
    """Read and parse a JSON config file from a remote host via SSH cat."""
    code, out, err = run_remote(client, f"cat {shlex.quote(remote_path)}")
    if code != 0:
        raise RuntimeError(f"Failed to read remote config {remote_path}: {err.strip() or out.strip()}")
    try:
        return json.loads(out)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Remote config at {remote_path} is not valid JSON: {exc}") from exc


def write_remote_json_config(
    client: paramiko.SSHClient,
    remote_path: str,
    config: Dict[str, object],
    use_sudo: bool = False,
) -> None:
    """
    Write a JSON config to a remote file.

    Uses a temp file + cp strategy so a partial write never corrupts the target.
    Set use_sudo=True if the remote user needs elevated privileges to write to remote_path.
    """
    content = json.dumps(config, indent=2)
    tmp_remote = f"/tmp/bryck_cfg_tmp_{int(_time.time())}.json"
    with client.open_sftp() as sftp:
        sftp.putfo(io.BytesIO(content.encode("utf-8")), tmp_remote)
    prefix = "sudo " if use_sudo else ""
    move_cmd = (
        f"{prefix}cp {shlex.quote(tmp_remote)} {shlex.quote(remote_path)} "
        f"&& rm -f {shlex.quote(tmp_remote)}"
    )
    code, out, err = run_remote(client, move_cmd)
    if code != 0:
        run_remote(client, f"rm -f {shlex.quote(tmp_remote)}")
        raise RuntimeError(
            f"Failed to write remote config {remote_path}: {err.strip() or out.strip()}"
        )


def apply_remote_batch_overrides(
    client: paramiko.SSHClient,
    remote_config_path: str,
    batch_overrides: Dict[str, object],
    use_sudo: bool = False,
) -> Dict[str, object]:
    """
    Merge batch_overrides['tiers'] into the BATCH section of the remote config.

    Only the keys explicitly listed in batch_overrides are changed — all other
    values in the remote config are preserved exactly.

    Returns the ORIGINAL config dict so it can be restored with restore_remote_config().
    """
    original = read_remote_json_config(client, remote_config_path)
    modified = copy.deepcopy(original)
    batch_section = modified.setdefault("BATCH", {})
    for tier_name, params in (batch_overrides.get("tiers") or {}).items():
        tier_key = tier_name.upper()
        if tier_key not in batch_section:
            batch_section[tier_key] = {}
        for param, value in (params or {}).items():
            batch_section[tier_key][param.upper()] = value
    write_remote_json_config(client, remote_config_path, modified, use_sudo=use_sudo)
    return original


def restore_remote_config(
    client: paramiko.SSHClient,
    remote_config_path: str,
    original_config: Dict[str, object],
    use_sudo: bool = False,
) -> None:
    """Restore the remote config to the state captured by apply_remote_batch_overrides."""
    write_remote_json_config(client, remote_config_path, original_config, use_sudo=use_sudo)
