"""Thin live-transfer client for Phase 4 (Reporting & Verification) RT-* cases.

Rather than duplicating the ~30 files under CloudCpCliTesting/bryckclient-cli/
(session.py, bryck_api.py, ssh_runner.py, ...), this module puts that folder on
sys.path and drives it directly. This keeps exactly one copy of the
JWT/SSH/REST plumbing to maintain while still giving every RT-* live case a
single, focused entry point for: SSH cleanup, datagen invocation, source
enumeration, transfer initiate/poll, and report-ZIP download.

See cloud_cp_report_test_case_plan.md for the full live-mode design this
implements.
"""
from __future__ import annotations

import json
import logging
import os
import re
import subprocess
import sys
import tempfile
import time
from pathlib import Path

logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_CONFIG_PATH = SCRIPT_DIR / "config.json"

TERMINAL_STATES = {"COMPLETED", "FAILED", "STOPPED", "CANCELLED"}


class LiveClientError(Exception):
    """Raised for setup/config errors distinct from case assertion failures."""


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
def load_config(path=None):
    """Load config.json, resolving cli_dir/login_json relative to the config
    file's own directory so --config alt.json works from any cwd."""
    cfg_path = Path(path) if path else DEFAULT_CONFIG_PATH
    if not cfg_path.is_file():
        raise LiveClientError(f"config file not found: {cfg_path}")
    with open(cfg_path, encoding="utf-8") as f:
        cfg = json.load(f)
    base = cfg_path.resolve().parent
    cfg["_base_dir"] = str(base)
    cfg["cli_dir"] = str((base / cfg["cli_dir"]).resolve())
    cfg["login_json"] = str((base / cfg["login_json"]).resolve())
    return cfg


def _ensure_cli_on_path(cli_dir):
    if cli_dir not in sys.path:
        sys.path.insert(0, cli_dir)


# ---------------------------------------------------------------------------
# API / SSH session
# ---------------------------------------------------------------------------
def get_session(cfg):
    """Log in to the Bryck REST API and return the authenticated ApiSession."""
    _ensure_cli_on_path(cfg["cli_dir"])
    from session import ApiSession  # noqa: PLC0415 - path is set up just above

    if not os.path.isfile(cfg["login_json"]):
        raise LiveClientError(f"login.json not found: {cfg['login_json']}")
    session = ApiSession.from_login_json(cfg["login_json"])
    session.login()
    return session


def get_api(session):
    _ensure_cli_on_path(str(SCRIPT_DIR))  # no-op safeguard, cli_dir already added
    from bryck_api import BryckApi  # noqa: PLC0415

    return BryckApi(session)


def connect_ssh(session):
    from ssh_runner import SshRunner  # noqa: PLC0415

    ssh = SshRunner.from_session(session)
    ssh.connect()
    return ssh


# ---------------------------------------------------------------------------
# Remote path helpers
# ---------------------------------------------------------------------------
def remote_case_dir(cfg, case_id, suffix=None):
    name = case_id if not suffix else f"{case_id}-{suffix}"
    return f"{cfg['bryck_local_root'].rstrip('/')}/{name}"


def s3_case_uri(cfg, case_id, suffix=None):
    name = case_id if not suffix else f"{case_id}-{suffix}"
    return f"{cfg['s3_bucket_prefix'].rstrip('/')}/{name}"


# ---------------------------------------------------------------------------
# SSH-driven setup: datagen + source enumeration + cleanup
# ---------------------------------------------------------------------------
def ssh_rm_rf(ssh, remote_path):
    """Idempotent remote cleanup; never fails the caller (logged instead)."""
    rc, out, err = ssh.run(f"rm -rf -- '{remote_path}'", timeout=60)
    if rc != 0:
        logger.warning("rm -rf %s failed (rc=%s): %s", remote_path, rc, err.strip() or out.strip())
    return rc == 0


def ssh_dir_exists(ssh, remote_path):
    rc, _out, _err = ssh.run(f"[ -d '{remote_path}' ]", timeout=30)
    return rc == 0


def push_spec_file(ssh, local_spec_path, case_id, root_override=None):
    """SFTP-upload a datagen YAML spec to a per-case /tmp path and return it.

    root_override replaces the spec's root: field so datagen writes to the
    directory this case actually expects, regardless of what the YAML hardcodes.
    """
    basename = os.path.basename(local_spec_path)
    remote_path = f"/tmp/report_testing_{case_id}_{basename}"
    if root_override is not None:
        content = Path(local_spec_path).read_text(encoding="utf-8")
        content = re.sub(r"^root:[ \t]*.*$", f"root: {root_override}", content, flags=re.MULTILINE)
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False, encoding="utf-8") as tf:
            tf.write(content)
            tmp_local = tf.name
        try:
            ssh.put(tmp_local, remote_path)
        finally:
            os.unlink(tmp_local)
    else:
        ssh.put(str(local_spec_path), remote_path)
    return remote_path


def run_datagen(ssh, cfg, remote_spec_path, timeout=1800):
    """Run the datagen binary against an already-uploaded spec file."""
    binary = cfg["datagen_binary"]
    cmd = f"{binary} --spec '{remote_spec_path}'"
    rc, out, err = ssh.run(cmd, timeout=timeout)
    if rc != 0:
        raise LiveClientError(f"datagen failed (rc={rc}): {err.strip() or out.strip()}")
    return out


def enumerate_remote_files(ssh, remote_dir):
    """SSH find+stat fallback source enumeration (datagen has no manifest
    output today - see plan §16 open question 1). Returns list of
    (relpath, size_bytes) relative to remote_dir."""
    cmd = f"find '{remote_dir}' -type f -printf '%s\\t%P\\n'"
    rc, out, err = ssh.run(cmd, timeout=120)
    if rc != 0:
        raise LiveClientError(f"remote enumeration failed (rc={rc}): {err.strip() or out.strip()}")
    entries = []
    for line in out.splitlines():
        if not line.strip():
            continue
        size_str, relpath = line.split("\t", 1)
        entries.append((relpath, int(size_str)))
    return entries


# ---------------------------------------------------------------------------
# S3 cleanup / listing (run locally against the S3-compatible endpoint)
# ---------------------------------------------------------------------------
def _aws_cmd(cfg, *args):
    return [cfg.get("aws_cli_path", "aws"), *args, "--endpoint-url", cfg["s3_endpoint_url"]]


def s3_rm_recursive(cfg, s3_uri):
    cmd = _aws_cmd(cfg, "s3", "rm", s3_uri, "--recursive")
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=300, check=False)
    except (OSError, subprocess.SubprocessError) as exc:
        logger.warning("aws s3 rm failed to launch for %s: %s", s3_uri, exc)
        return False
    if proc.returncode != 0:
        logger.warning("aws s3 rm --recursive %s failed: %s", s3_uri, proc.stderr.strip())
        return False
    return True


def s3_object_count(cfg, s3_uri):
    """Return the number of objects under s3_uri, or None if the listing itself failed."""
    cmd = _aws_cmd(cfg, "s3", "ls", s3_uri.rstrip("/") + "/", "--recursive")
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=120, check=False)
    except (OSError, subprocess.SubprocessError) as exc:
        logger.warning("aws s3 ls failed to launch for %s: %s", s3_uri, exc)
        return None
    if proc.returncode != 0:
        return None
    lines = [ln for ln in proc.stdout.splitlines() if ln.strip()]
    return len(lines)


# ---------------------------------------------------------------------------
# Transfer initiate / poll / download
# ---------------------------------------------------------------------------
def _extract_transfer_id(resp_result):
    if isinstance(resp_result, (str, int)):
        return str(resp_result)
    if isinstance(resp_result, dict):
        for key in ("transfer_id", "task_id", "id"):
            if key in resp_result:
                return str(resp_result[key])
    raise LiveClientError(f"could not extract transfer_id from response: {resp_result!r}")


def initiate_transfer(api, cfg, src, dst):
    """POST /api/bcloud/transfer for an upload (src local, dst s3://...) or a
    download (src s3://..., dst local). Assumes the cloud provider is already
    configured (bryck_cloud_configure.py) - out of scope here per the plan's
    assumption that credentials/config are a one-time setup step."""
    resp = api.initiate_cloud_transfer(cfg["cloud_type"], src, dst)
    if resp is None or not getattr(resp, "ok", False):
        raise LiveClientError(f"initiate_cloud_transfer failed: src={src} dst={dst} resp={resp}")
    body = resp.json()
    return _extract_transfer_id(body.get("result"))


def poll_until_terminal(api, transfer_id, interval_sec, timeout_sec):
    """Poll /api/bcloud/status_transfer until a terminal state or timeout.

    Returns (state, last_entry, history) where history is the list of
    sampled entries (used by progress-counter monotonicity checks).
    """
    history = []
    deadline = time.time() + timeout_sec
    last_entry = {}
    while True:
        resp = api.get_cloud_transfer_status(transfer_id)
        entry = {}
        if resp is not None and getattr(resp, "ok", False):
            body = resp.json()
            entry = body.get("result") or {}
        history.append(entry)
        last_entry = entry
        state = str(entry.get("state", "")).upper()
        if state in TERMINAL_STATES:
            return state, last_entry, history
        if time.time() >= deadline:
            return "TIMEOUT", last_entry, history
        time.sleep(interval_sec)


def download_report(api, transfer_id, dest_dir):
    """Download the report ZIP for transfer_id into dest_dir; returns the path."""
    dest_dir = Path(dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)
    zip_path = dest_dir / f"cloud_transfer_{transfer_id}.zip"
    resp = api.download_cloud_transfer_log(str(transfer_id))
    if resp is None:
        raise LiveClientError(f"download_cloud_transfer_log returned no response for {transfer_id}")
    if not getattr(resp, "ok", False):
        raise LiveClientError(
            f"report download failed for {transfer_id}: HTTP {resp.status_code} {resp.reason}"
        )
    bytes_written = 0
    try:
        with open(zip_path, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=8192):
                if chunk:
                    fh.write(chunk)
                    bytes_written += len(chunk)
    finally:
        resp.close()
    if bytes_written == 0:
        raise LiveClientError(f"report ZIP for {transfer_id} downloaded empty (0 bytes)")
    return zip_path
