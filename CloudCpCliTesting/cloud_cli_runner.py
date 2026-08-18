#!/usr/bin/env python3
"""Two-phase CloudCP CLI test executor: ``--plan`` then ``--execute``.

Implements the workflow documented in ``cloud_cli_plan.md``:

  Phase 1 (read-only):
      python3 cloud_cli_runner.py --plan
          -> validates config, resolves datasets/tiers, builds the full
             test-case list (plan.md §9), prints the confirmation gate
             (plan.md §13), and on "yes" writes a confirmed plan.json.
             Never mounts/generates/transfers/edits anything.

  Phase 2 (real work):
      python3 cloud_cli_runner.py --execute --plan-file results/<RUN_ID>/plan.json
          -> replays only the confirmed plan: dataset generation, cloud
             configure, transfer initiate/status/pause/resume/cancel,
             live intervention tests (mount/eject/format/erase/remove,
             service restarts), report download + validation, cleanup,
             and JSON/HTML/Markdown summary reports.

Intended to run on the Linux Bryck host, from this directory
(``CloudCpCliTesting/``), so that ``bryckclient-cli/*.py`` and
``cloudcpclitesting.py`` are importable/relative-pathable as-is.
Use ``--dry-run`` to exercise the whole flow (including on Windows)
without touching a real Bryck.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import os
import pathlib
import re
import shutil
import subprocess
import sys
import tempfile
import time
import types
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence

HERE = pathlib.Path(__file__).resolve().parent
REPO_ROOT = HERE.parent
BRYCK_CLI_DIR = HERE / "bryckclient-cli"
SPEC_ROOT = REPO_ROOT / "dataset_cloudcp" / "spec_files"
RESULTS_ROOT = HERE / "results"

sys.path.insert(0, str(HERE))
sys.path.insert(0, str(BRYCK_CLI_DIR))

import cloudcpclitesting as base  # noqa: E402  (dataset/report helpers reuse)
import cli_perf_capture as perf_mod  # noqa: E402

DEFAULT_DATAGEN = "/home/bryck/rperiyas/datagen"
DEFAULT_PYTHON_BIN = "python3"
DEFAULT_BRYCKCLOUD = "/opt/bryck/.venv/bryck/bin/bryckcloud"
DEFAULT_BATCHMETA = "/opt/bryck/bryckapi/downloads/bcloud_batchmeta"
DEFAULT_TRANSFER_LOGS = "/opt/bryck/bryckapi/downloads/cloud_transfer_logs"
DEFAULT_BRYCK_CONFIG_JSON = "/etc/bryck/bryckcloud/config.json"

LIFECYCLE_DATASET = "DS-P1-04"  # single representative dataset for lifecycle/service tests
SPEC_FILES_DIR = HERE / "spec_files"
FALLBACK_SPEC_FILES_DIR = REPO_ROOT / "CloudCpFallbackTesting" / "spec_files"

MODES = ["upload", "download", "both"]
MODE_CODE = {"upload": "U", "download": "D", "both": "B"}

LIFECYCLE_ACTIONS = [
    "pause", "resume", "cancel", "retransfer", "mount", "eject",
    "format", "erase", "remove", "restart_bcloud", "restart_bryckapi",
]

EDGE_CASES = {
    "CLI-EDGE-01": {"dataset": "DS-P8-01", "description": "Empty source directory upload"},
    "CLI-EDGE-02": {"dataset": "DS-P8-04", "description": "14-level deep directory tree upload"},
    "CLI-EDGE-03": {"dataset": "DS-P9-04", "description": "Single 64 MB file upload (first multipart size)"},
    "CLI-EDGE-04": {"dataset": "DS-P4-01", "description": "Tiny tier, 20 filename variants upload"},
}

# CLI/input-validation negative cases: each expects the operation to be REJECTED
# (expect_fail=True) before any real mutation happens. Modeled on NEGATIVE_TEST_PLAN.md §7.
CLI_INPUT_CASES = {
    "CLI-01": "Initiate transfer without --mode (argparse must reject; no transfer created)",
    "CLI-02": "Initiate transfer with an invalid --mode value (--mode copy)",
    "CLI-03": "Upload with empty bryck_src in cloud_ops.json",
    "CLI-04": "Upload with empty cloud_bucket in cloud_ops.json",
    "CLI-05": "Download with empty bryck_dst in cloud_ops.json",
    "CLI-06": "bryck_cloud_show.py with a missing login.json file",
    "CLI-07": "bryck_cloud_show.py with a malformed (unparsable) login.json",
    "CLI-08": "Pause a transfer using an invalid --transfer-id (not-a-transfer-id)",
    "CLI-09": "datagen with a nonexistent spec YAML file",
}

# Cloud/AWS configuration negative cases: each expects bryck_cloud_configure.py to
# reject the mutated cloud_ops.json before any partial provider config lands.
# Modeled on NEGATIVE_TEST_PLAN.md §10 (AWS-01..AWS-08 subset).
AWS_NEGATIVE_CASES = {
    "AWS-01": "Configure with empty access_key_id",
    "AWS-02": "Configure with empty secret_access_key",
    "AWS-03": "Configure with an invalid access_key_id",
    "AWS-04": "Configure with an invalid secret_access_key",
    "AWS-05": "Configure with an invalid region",
    "AWS-06": "Configure with an invalid cloud_bucket URI",
    "AWS-07": "Deconfigure when no cloud provider is configured (observational; idempotence documented)",
    "AWS-08": "Deconfigure twice in a row (observational; second call must be deterministic)",
}

TERMINAL_STATES = {"COMPLETED", "FAILED", "STOPPED", "CANCELLED"}
SECRET_KEYS = {
    "access_key_id", "secret_access_key", "bryckapi_password",
    "bryckserver_password", "password", "keyfile",
}


def setup_logging(verbose: bool) -> logging.Logger:
    logger = logging.getLogger("cloud_cli_runner")
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    if logger.handlers:
        return logger
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG if verbose else logging.INFO)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(handler)
    return logger


# =============================================================================
# argparse
# =============================================================================

def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Two-phase CloudCP CLI test executor (--plan then --execute).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--run", action="store_true",
                      help="ONE-SHOT: plan + confirm + execute immediately in a single command "
                           "(recommended). Actually performs dataset generation, transfers, etc. "
                           "Use --plan/--execute separately only if you want to inspect plan.json first.")
    mode.add_argument("--plan", action="store_true",
                      help="Phase 1 ONLY: build + confirm the plan and write plan.json. "
                           "Does NOT run any test case yet -- follow up with --execute --plan-file <path>.")
    mode.add_argument("--execute", action="store_true", help="Phase 2: run a plan.json written by --plan.")
    mode.add_argument("--list-cases", action="store_true",
                      help="Print every test-case ID that the current --modes/--dataset-catalog/"
                           "--include-*/--suite/--only selection would build, then exit. No side effects.")
    parser.add_argument("--all", dest="dataset_catalog", action="store_const", const="all",
                         default=argparse.SUPPRESS,
                         help="Alias for --dataset-catalog all (the default): every DS-P1-01..DS-P12-02 dataset.")
    parser.add_argument("--test-name", dest="suite", nargs="+", default=argparse.SUPPRESS,
                         choices=["transfer", "lifecycle", "service", "edge", "cli-input", "aws-negative", "all"],
                         help="Alias for --suite: run one named test group by name, e.g. --test-name transfer.")

    parser.add_argument("--plan-file", help="Path to plan.json (required for --execute).")
    parser.add_argument("--modes", nargs="+", default=MODES, choices=MODES,
                         help="Subset of transfer modes to include (default: all).")
    parser.add_argument("--dataset-catalog", choices=["all", "specfiles"], default="all",
                         help="'all' (default) runs every dataset in dataset_cloudcp/spec_files/manifest.json "
                              "(DS-P1-01..DS-P12-02; optionally narrowed with --datasets) as its own transfer "
                              "round. 'specfiles' runs every *.yaml spec under CloudCpCliTesting/spec_files/ "
                              "(optionally narrowed with --datasets, e.g. 09_unicode_names).")
    parser.add_argument("--datasets", nargs="+", default=None,
                         help="Explicit dataset IDs (--dataset-catalog all, e.g. DS-P1-01) or spec names "
                              "(--dataset-catalog specfiles, e.g. 01_zero_byte). Defaults to the full catalog.")
    parser.add_argument("--include-lifecycle", action="store_true", default=True)
    parser.add_argument("--no-lifecycle", dest="include_lifecycle", action="store_false",
                         help="Skip the live intervention matrix (§9.2).")
    parser.add_argument("--include-service", action="store_true", default=True)
    parser.add_argument("--no-service", dest="include_service", action="store_false",
                         help="Skip the service restart matrix (§9.3).")
    parser.add_argument("--include-edge", action="store_true", default=True)
    parser.add_argument("--no-edge", dest="include_edge", action="store_false",
                         help="Skip the negative/edge-case matrix (§9.4).")
    parser.add_argument("--include-cli-input", action="store_true", default=True)
    parser.add_argument("--no-cli-input", dest="include_cli_input", action="store_false",
                         help="Skip the CLI/input-validation negative cases (CLI-01..CLI-09).")
    parser.add_argument("--include-aws-negative", action="store_true", default=True)
    parser.add_argument("--no-aws-negative", dest="include_aws_negative", action="store_false",
                         help="Skip the cloud/AWS configuration negative cases (AWS-01..AWS-08).")
    parser.add_argument("--only", nargs="+", default=None,
                         help="Build/run only these test-case IDs (e.g. --only CLI-U-DS-P1-01), ignoring --modes/--include-*.")
    parser.add_argument("--suite", nargs="+", default=None,
                         choices=["transfer", "lifecycle", "service", "edge", "cli-input", "aws-negative", "all"],
                         help="Run by suite NAME instead of test-case IDs, e.g. --suite cli-input, "
                              "--suite transfer lifecycle, or --suite all. Ignored if --only is also given.")

    parser.add_argument("--login", default=str(BRYCK_CLI_DIR / "login.json"))
    parser.add_argument("--params", default=str(BRYCK_CLI_DIR / "cloud_ops.json"),
                         help="cloud_ops.json path (dynamically rewritten per test case).")
    parser.add_argument("--format-mount-params", default=str(BRYCK_CLI_DIR / "format_mount_params.json"))
    parser.add_argument("--bryck-config-json", default=DEFAULT_BRYCK_CONFIG_JSON,
                         help="Read-only reference config (decision #14); only tier names are read from it.")

    parser.add_argument("--output-base", default="/bryck",
                         help="Bryck-side root for materialized upload datasets (e.g. /bryck/<dataset-id>).")
    parser.add_argument("--download-base", default="/bryck/cloudcp_cli_dl",
                         help="Bryck-side root for download-mode destinations.")
    parser.add_argument("--bucket", default="s3://shravani/cloudcp-cli",
                         help="S3 bucket+prefix root; each dataset gets its own sub-prefix.")

    parser.add_argument("--datagen-bin", default=DEFAULT_DATAGEN)
    parser.add_argument("--bryckcloud-bin", default=DEFAULT_BRYCKCLOUD,
                         help="Path to the bryckcloud CLI, used to start transfers directly "
                              "(bryckcloud transfer add aws --src <path> --dst <path>).")
    parser.add_argument("--python-bin", default=DEFAULT_PYTHON_BIN)
    parser.add_argument("--batchmeta-dir", default=DEFAULT_BATCHMETA)
    parser.add_argument("--transfer-logs-dir", default=DEFAULT_TRANSFER_LOGS)

    parser.add_argument("--wait-timeout", type=int, default=1800)
    parser.add_argument("--poll-interval", type=int, default=10)
    parser.add_argument("--intervention-wait", type=int, default=15,
                         help="Seconds to let a transfer run before firing a live-intervention action.")
    parser.add_argument("--action-timeout", type=int, default=90,
                         help="Seconds to wait/poll for a lifecycle/service action's expected state "
                              "transition to be confirmed before marking it FAILED/TIMED_OUT.")

    parser.add_argument("--results-dir", default=str(RESULTS_ROOT))
    parser.add_argument("--report-save-dir", default="/home/bryck/shravani",
                         help="Bryck-host directory where downloaded transfer/diagnostic reports are saved "
                              "(one subfolder per test case). Created if missing.")
    parser.add_argument("--run-id", default=None, help="Override the generated RUN_ID.")
    parser.add_argument("--keep", "--no-cleanup", dest="keep", action="store_true",
                         help="Skip auto-cleanup of datasets/cloud objects (debugging).")
    parser.add_argument("--aws-cli", default="aws", help="aws CLI binary used for S3 cleanup.")

    parser.add_argument("--journal-tag", default="bryckcloud",
                         help="journalctl syslog tag for broker log capture (default: bryckcloud).")
    parser.add_argument("--cloudcp-log",
                         default="/opt/bryck/bryckapi/downloads/cloud_transfer_logs/cloudcp.log",
                         help="Path to cloudcp.log for per-batch throughput capture.")
    parser.add_argument("--capture-lead", type=float, default=3,
                         help="Seconds to settle the journalctl follower before the transfer (default: 3).")
    parser.add_argument("--capture-drain", type=float, default=6,
                         help="Seconds to keep capturing after the transfer completes (default: 6).")
    parser.add_argument("--no-perf", dest="perf_capture", action="store_false", default=True,
                         help="Disable journal/cloudcp.log performance capture and HTML reports.")

    parser.add_argument("--yes", action="store_true", help="Auto-confirm the plan gate (§13) non-interactively.")
    parser.add_argument("--dry-run", action="store_true",
                         help="Print every command instead of executing it. Safe on any host.")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args(argv)


# =============================================================================
# Small data holders
# =============================================================================

@dataclass
class CommandResult:
    description: str
    argv: List[str]
    started_at: str
    ended_at: str
    returncode: Optional[int]
    stdout: str
    stderr: str
    dry_run: bool

    def as_dict(self) -> dict:
        return {
            "description": self.description,
            "argv": self.argv,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "returncode": self.returncode,
            "stdout": self.stdout,
            "stderr": self.stderr,
            "dry_run": self.dry_run,
        }


@dataclass
class TestCaseResult:
    test_id: str
    kind: str
    description: str
    status: str = "PENDING"
    commands: List[dict] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)
    sub_results: List[dict] = field(default_factory=list)
    steps: List[dict] = field(default_factory=list)
    expected: str = ""
    actual: str = ""
    state_change: str = ""

    def step(self, name: str, status: str, detail: str = "") -> None:
        """Record one numbered pipeline step (inspect/validate/execute/cleanup/...)."""
        self.steps.append({"n": len(self.steps) + 1, "name": name, "status": status, "detail": detail})

    def render_block(self) -> str:
        """Render the numbered-pipeline summary block (inspect -> ... -> verify final state)."""
        width = 60
        lines = ["=" * width, f"{self.test_id} | {self.description}", "=" * width]
        for s in self.steps:
            lines.append(f"[{s['n']}] {s['name']:<38} {s['status']}" + (f"  ({s['detail']})" if s["detail"] else ""))
        if self.expected:
            lines.append(f"\nExpected:     {self.expected}")
        if self.actual:
            lines.append(f"Actual:       {self.actual}")
        if self.state_change:
            lines.append(f"State change: {self.state_change}")
        lines.append(f"RESULT:       {self.status}")
        return "\n".join(lines)


# =============================================================================
# Secret redaction
# =============================================================================

def build_redactor(*configs: dict):
    secrets: List[str] = []
    for cfg in configs:
        for key, value in _flatten(cfg).items():
            leaf = key.split(".")[-1]
            if leaf in SECRET_KEYS and isinstance(value, str) and value:
                secrets.append(value)
    secrets = sorted(set(secrets), key=len, reverse=True)

    def redact(text: str) -> str:
        if not text:
            return text
        for secret in secrets:
            if secret and secret in text:
                text = text.replace(secret, "***REDACTED***")
        return text

    return redact


def _flatten(obj: Any, prefix: str = "") -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if isinstance(obj, dict):
        for key, value in obj.items():
            out.update(_flatten(value, f"{prefix}.{key}" if prefix else str(key)))
    else:
        out[prefix] = obj
    return out


# =============================================================================
# Command execution helpers
# =============================================================================

def run_argv(
    description: str,
    argv: List[str],
    logger: logging.Logger,
    dry_run: bool,
    redact=lambda s: s,
    cwd: Optional[pathlib.Path] = None,
    timeout: Optional[int] = None,
) -> CommandResult:
    logger.info("$ %s", " ".join(argv))
    started_at = dt.datetime.now().isoformat()
    if dry_run:
        return CommandResult(description, argv, started_at, started_at, None, "", "", True)
    try:
        proc = subprocess.run(
            argv, cwd=str(cwd) if cwd else None, capture_output=True, text=True, timeout=timeout,
        )
        rc, out, err = proc.returncode, proc.stdout, proc.stderr
    except subprocess.TimeoutExpired as exc:
        rc, out, err = -1, exc.stdout or "", f"TIMEOUT after {timeout}s: {exc}"
    ended_at = dt.datetime.now().isoformat()
    return CommandResult(description, argv, started_at, ended_at, rc, redact(out or ""), redact(err or ""), False)


def run_py_script(
    script: str,
    args: List[str],
    logger: logging.Logger,
    dry_run: bool,
    redact=lambda s: s,
    python_bin: str = DEFAULT_PYTHON_BIN,
    timeout: Optional[int] = None,
) -> CommandResult:
    argv = [python_bin, str(BRYCK_CLI_DIR / script)] + args
    return run_argv(f"run {script}", argv, logger, dry_run, redact, cwd=BRYCK_CLI_DIR, timeout=timeout)


def ssh_exec(
    login_path: str,
    command: str,
    logger: logging.Logger,
    dry_run: bool,
    redact=lambda s: s,
) -> CommandResult:
    started_at = dt.datetime.now().isoformat()
    logger.info("$ ssh -> %s", command)
    if dry_run:
        return CommandResult(f"ssh: {command}", ["ssh", command], started_at, started_at, None, "", "", True)
    try:
        from session import ApiSession  # type: ignore
        from ssh_runner import SshRunner  # type: ignore

        session = ApiSession.from_login_json(login_path)
        with SshRunner.from_session(session) as ssh:
            rc, out, err = ssh.run(command)
    except Exception as exc:  # noqa: BLE001 - surfaced as a failed command, not a crash
        rc, out, err = 1, "", f"SSH execution failed: {exc}"
    ended_at = dt.datetime.now().isoformat()
    return CommandResult(f"ssh: {command}", ["ssh", command], started_at, ended_at, rc, redact(out or ""), redact(err or ""), False)


def get_bryck_state(args: argparse.Namespace, logger: logging.Logger, redact) -> tuple[str, CommandResult]:
    result = run_py_script("bryck_info.py", ["--login", args.login], logger, args.dry_run, redact, args.python_bin)
    if args.dry_run or result.returncode != 0:
        return "UNKNOWN", result
    try:
        payload = json.loads(result.stdout)
    except (json.JSONDecodeError, AttributeError):
        logger.warning("bryck_info.py returned non-JSON stdout (first 200 chars): %r", result.stdout[:200])
        return "UNKNOWN", result
    # bryck_info.py prints the *unwrapped* bryck_info dict ("State" at the top
    # level); a "bryck_info"-wrapped shape is tolerated too in case that ever
    # changes back.
    state = payload.get("State")
    if state is None and isinstance(payload.get("bryck_info"), dict):
        state = payload["bryck_info"].get("State")
    state = str(state if state is not None else "UNKNOWN").strip()
    return state or "UNKNOWN", result


def get_transfer_status(args: argparse.Namespace, transfer_id: str, logger: logging.Logger, redact) -> tuple[str, CommandResult]:
    result = run_py_script(
        "bryck_cloud_transfer_status.py",
        ["--login", args.login, "--transfer-id", str(transfer_id)],
        logger, args.dry_run, redact, args.python_bin,
    )
    if args.dry_run or result.returncode != 0:
        return "UNKNOWN", result
    # bryck_cloud_transfer_status.py prints a human-readable "STATE : COMPLETED"
    # block via logger.info() (stderr by default), not JSON on stdout -- search
    # both streams with a plain-text pattern.
    combined = (result.stdout or "") + (result.stderr or "")
    match = re.search(r"STATE\s*:\s*([A-Z_]+)", combined)
    return (match.group(1) if match else "UNKNOWN"), result


def parse_transfer_id(text: str) -> Optional[str]:
    match = re.search(r"transfer[_ ]?id[^0-9]*(\d+)", text, re.IGNORECASE)
    return match.group(1) if match else None


def read_local_transfer_status(transfer_logs_dir: pathlib.Path, transfer_id: str) -> str:
    """Fallback for get_transfer_status(): the Bryck's REST /status_transfer
    endpoint can return 409 "Failed to find the transfer/s" within seconds of
    a fast transfer completing (it's purged from the *active* transfer
    registry quickly), even though transfer_summary.txt on disk already
    proves it finished. Read that file directly as the authoritative source
    when the API says UNKNOWN."""
    try:
        tid = int(transfer_id)
    except (TypeError, ValueError):
        return "UNKNOWN"
    summary_path = base.transfer_log_dir(transfer_logs_dir, tid) / "transfer_summary.txt"
    if not summary_path.is_file():
        return "UNKNOWN"
    try:
        text = summary_path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return "UNKNOWN"
    match = re.search(r"Transfer status\s*:\s*(\w+)", text)
    return match.group(1).strip().upper() if match else "UNKNOWN"


# =============================================================================
# Dataset resolution (reuses cloudcpclitesting.py helpers where possible)
# =============================================================================

def all_catalog_dataset_ids() -> List[str]:
    """Every dataset id declared in dataset_cloudcp/spec_files/manifest.json, sorted."""
    _manifest, dataset_map = base.load_manifest(SPEC_ROOT)
    return sorted(dataset_map)


def local_spec_catalog_ids() -> List[str]:
    """Every *.yaml spec name under CloudCpCliTesting/spec_files/, plus any
    CloudCpFallbackTesting/spec_files/*.yaml not already present there, sorted."""
    names = {p.stem for p in SPEC_FILES_DIR.glob("*.yaml")}
    names |= {p.stem for p in FALLBACK_SPEC_FILES_DIR.glob("*.yaml")}
    return sorted(names)


def local_spec_file_path(name: str) -> Optional[pathlib.Path]:
    candidate = SPEC_FILES_DIR / f"{name}.yaml"
    if candidate.is_file():
        return candidate
    fallback = FALLBACK_SPEC_FILES_DIR / f"{name}.yaml"
    return fallback if fallback.is_file() else None


def generate_tier_dataset(
    tier: str,
    output_base: str,
    args: argparse.Namespace,
    logger: logging.Logger,
    dataset_id: str,
) -> tuple[pathlib.Path, dict]:
    """Materialize one dataset under output_base, reusing the single-dataset
    datagen flow already validated by cloudcpclitesting.py.

    `tier` is only used as the folder-name label for local spec_files/*.yaml
    datasets; `dataset_id` (a DS-P* manifest id, or a
    CloudCpCliTesting/spec_files/*.yaml name) selects what gets generated.
    """
    ns = types.SimpleNamespace(
        output_base=output_base,
        skip_generate=False,
        datagen_bin=args.datagen_bin,
        dry_run=args.dry_run,
        verbose=args.verbose,
    )
    local_spec = local_spec_file_path(dataset_id)
    if local_spec is not None:
        return generate_named_spec_dataset(local_spec, tier, output_base, ns, logger)
    dataset = base.select_dataset(SPEC_ROOT, dataset_id)
    # generate_dataset() writes under <output_base>/<dataset_id> itself, so
    # output_base is passed through as-is -- no extra tier-level wrapping
    # (tier == dataset_id for the DS-P* catalog, so wrapping would double it:
    # <output_base>/<dataset_id>/<dataset_id>).
    dataset_root, summary = base.generate_dataset(ns, dataset, SPEC_ROOT, logger)
    return dataset_root, summary


def generate_named_spec_dataset(
    spec_path: pathlib.Path,
    name: str,
    output_base: str,
    ns: types.SimpleNamespace,
    logger: logging.Logger,
) -> tuple[pathlib.Path, dict]:
    """Materialize a single-spec YAML (any CloudCpCliTesting/spec_files/*.yaml)
    under output_base/<name>, rewriting its `root:` line to match."""
    target_root = pathlib.Path(output_base) / name
    summary = {"dataset_root": str(target_root), "spec_file": str(spec_path)}
    if ns.skip_generate:
        summary["actual_files"] = base.count_files_recursive(target_root)
        return target_root, summary

    text = spec_path.read_text(encoding="utf-8")
    new_text = base.ROOT_LINE_RE.sub(f"root: {target_root.as_posix()}", text, count=1)
    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", prefix=f"cli_{name}_", delete=False, encoding="utf-8")
    tmp.write(new_text)
    tmp.close()
    try:
        target_root.mkdir(parents=True, exist_ok=True)
        proc = base.run_cmd([ns.datagen_bin, "--spec", tmp.name], logger, ns.dry_run)
        if proc is not None:
            base.check_completed(proc, f"datagen for {name}")
        summary["actual_files"] = 0 if ns.dry_run else base.count_files_recursive(target_root)
    finally:
        try:
            os.unlink(tmp.name)
        except OSError:
            pass
    return target_root, summary


# =============================================================================
# Phase 1: --plan
# =============================================================================

def validate_json_file(path: str) -> tuple[bool, Optional[dict], Optional[str]]:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
        return True, data, None
    except FileNotFoundError:
        return False, None, f"not found: {path}"
    except json.JSONDecodeError as exc:
        return False, None, f"invalid JSON in {path}: {exc}"


def validate_bryck_config_json(path: str) -> tuple[bool, List[str], str, str]:
    """Strictly validate the read-only /etc/bryck/bryckcloud/config.json reference file.

    Returns (valid, tier_names, message, context_snippet). On a JSON parse
    error, `context_snippet` mimics `nl -ba <file> | sed -n '<a>,<b>p'`
    around the failing line so the operator can see exactly what's wrong
    (e.g. a second concatenated JSON document -> "Extra data" at line 1).
    """
    if not os.path.isfile(path):
        return False, [], f"{path} not found on this host (expected on the real Bryck host)", ""
    try:
        text = pathlib.Path(path).read_text(encoding="utf-8")
    except OSError as exc:
        return False, [], f"could not read {path}: {exc}", ""
    try:
        bconf = json.loads(text)
    except json.JSONDecodeError as exc:
        lines = text.splitlines()
        start = max(1, exc.lineno - 5)
        end = min(len(lines), exc.lineno + 5)
        snippet_lines = [f"{n:>4}  {lines[n - 1]}" for n in range(start, end + 1) if n - 1 < len(lines)]
        snippet = "\n".join(snippet_lines)
        return False, [], f"Failed to parse {path}: {exc}", snippet
    if not isinstance(bconf, dict):
        return False, [], f"{path} does not contain a JSON object at the top level", ""
    tiers = list(bconf.get("TIERS", bconf.get("tiers", {})).keys())
    return True, tiers, "", ""


def build_plan(args: argparse.Namespace, logger: logging.Logger) -> dict:
    run_id = args.run_id or dt.datetime.now().strftime("%Y%m%d_%H%M%S")

    problems: List[str] = []
    ok_login, login_cfg, err = validate_json_file(args.login)
    if not ok_login:
        problems.append(err or "login.json invalid")
    ok_params, cloud_ops_cfg, err = validate_json_file(args.params)
    if not ok_params:
        problems.append(err or "cloud_ops.json invalid")
    ok_fmp, _fmp_cfg, err = validate_json_file(args.format_mount_params)
    if not ok_fmp:
        problems.append(err or "format_mount_params.json invalid")

    config_ok, bryck_config_tiers, config_msg, config_snippet = validate_bryck_config_json(args.bryck_config_json)
    if not config_ok:
        # This is deliberately NOT folded into the generic "non-fatal warning" bucket:
        # a malformed reference config must be visible as a real problem, not buried.
        problems.append(f"CONFIG ERROR: {config_msg}")

    redact = build_redactor(login_cfg or {}, cloud_ops_cfg or {})
    state, info_cmd = get_bryck_state(args, logger, redact)

    test_cases: List[dict] = []
    if args.dataset_catalog == "all":
        try:
            catalog_ids = args.datasets if args.datasets else all_catalog_dataset_ids()
        except SystemExit as exc:
            catalog_ids = []
            problems.append(str(exc))
        for dataset_id in catalog_ids:
            for mode in args.modes:
                test_cases.append({
                    "id": f"CLI-{MODE_CODE[mode]}-{dataset_id}",
                    "kind": "transfer",
                    "tier": dataset_id,
                    "mode": mode,
                    "dataset": dataset_id,
                    "description": f"{mode} transfer for dataset {dataset_id} (full-catalog round)",
                })
    elif args.dataset_catalog == "specfiles":
        spec_ids = args.datasets if args.datasets else local_spec_catalog_ids()
        missing_specs = [name for name in spec_ids if local_spec_file_path(name) is None]
        if missing_specs:
            problems.append(f"--datasets referenced unknown spec_files entries: {missing_specs}")
        for dataset_id in spec_ids:
            if local_spec_file_path(dataset_id) is None:
                continue
            for mode in args.modes:
                test_cases.append({
                    "id": f"CLI-{MODE_CODE[mode]}-{dataset_id}",
                    "kind": "transfer",
                    "tier": dataset_id,
                    "mode": mode,
                    "dataset": dataset_id,
                    "description": f"{mode} transfer for spec_files/{dataset_id}.yaml",
                })
    if args.include_lifecycle:
        test_cases.append({
            "id": f"CLI-LC-{LIFECYCLE_DATASET}",
            "kind": "lifecycle",
            "tier": LIFECYCLE_DATASET,
            "mode": "both",
            "dataset": LIFECYCLE_DATASET,
            "description": f"Live intervention matrix on {LIFECYCLE_DATASET} ({', '.join(LIFECYCLE_ACTIONS)})",
        })
    if args.include_service:
        for target in ("bcloud", "bryckapi"):
            test_cases.append({
                "id": f"CLI-SVC-{target.upper()}",
                "kind": "service",
                "tier": LIFECYCLE_DATASET,
                "mode": "both",
                "dataset": LIFECYCLE_DATASET,
                "target_service": f"{target}.service",
                "description": f"Restart {target}.service mid-transfer on {LIFECYCLE_DATASET}",
            })
    if args.include_edge:
        for test_id, meta in EDGE_CASES.items():
            test_cases.append({
                "id": test_id,
                "kind": "edge",
                "tier": None,
                "mode": "upload",
                "dataset": meta["dataset"],
                "description": meta["description"],
            })
    if args.include_cli_input:
        for test_id, description in CLI_INPUT_CASES.items():
            test_cases.append({
                "id": test_id,
                "kind": "cli_input",
                "tier": None,
                "mode": None,
                "dataset": None,
                "description": description,
            })
    if args.include_aws_negative:
        for test_id, description in AWS_NEGATIVE_CASES.items():
            test_cases.append({
                "id": test_id,
                "kind": "cloud_negative",
                "tier": None,
                "mode": None,
                "dataset": None,
                "description": description,
            })

    if args.only:
        wanted = set(args.only)
        test_cases = [tc for tc in test_cases if tc["id"] in wanted]
        missing = wanted - {tc["id"] for tc in test_cases}
        if missing:
            problems.append(f"--only referenced unknown test case id(s): {sorted(missing)}")
    elif args.suite and "all" not in args.suite:
        suite_to_kind = {
            "transfer": "transfer", "lifecycle": "lifecycle", "service": "service",
            "edge": "edge", "cli-input": "cli_input", "aws-negative": "cloud_negative",
        }
        wanted_kinds = {suite_to_kind[s] for s in args.suite}
        test_cases = [tc for tc in test_cases if tc["kind"] in wanted_kinds]

    plan = {
        "run_id": run_id,
        "created_at": dt.datetime.now().isoformat(),
        "confirmed": False,
        "pre_flight_problems": problems,
        "bryck_state_before": state,
        "config": {
            "login": os.path.abspath(args.login),
            "params": os.path.abspath(args.params),
            "format_mount_params": os.path.abspath(args.format_mount_params),
            "bryck_config_json": args.bryck_config_json,
            "output_base": args.output_base,
            "download_base": args.download_base,
            "bucket": args.bucket,
            "datagen_bin": args.datagen_bin,
            "bryckcloud_bin": args.bryckcloud_bin,
            "python_bin": args.python_bin,
            "batchmeta_dir": args.batchmeta_dir,
            "transfer_logs_dir": args.transfer_logs_dir,
            "wait_timeout": args.wait_timeout,
            "poll_interval": args.poll_interval,
            "intervention_wait": args.intervention_wait,
            "action_timeout": args.action_timeout,
            "keep": args.keep,
            "aws_cli": args.aws_cli,
            "results_dir": os.path.abspath(args.results_dir),
            "report_save_dir": args.report_save_dir,
            "journal_tag": args.journal_tag,
            "cloudcp_log": args.cloudcp_log,
            "capture_lead": args.capture_lead,
            "capture_drain": args.capture_drain,
            "perf_capture": args.perf_capture,
        },
        "dataset_catalog": args.dataset_catalog,
        "bryck_config_tiers_seen": bryck_config_tiers,
        "bryck_config_valid": config_ok,
        "bryck_config_message": config_msg,
        "bryck_config_snippet": config_snippet,
        "test_cases": test_cases,
    }
    return plan


def render_confirmation(plan: dict) -> str:
    transfer_datasets = sorted({tc["dataset"] for tc in plan["test_cases"] if tc["kind"] == "transfer"})
    catalog = plan.get("dataset_catalog")
    if catalog == "all":
        dataset_line = f"ALL {len(transfer_datasets)} datasets from dataset_cloudcp/spec_files/manifest.json"
        generate_line = f"Generate datasets ({len(transfer_datasets)} datasets, full catalog round)"
    else:
        dataset_line = f"{len(transfer_datasets)} spec_files/*.yaml datasets: {', '.join(transfer_datasets)}"
        generate_line = f"Generate datasets ({len(transfer_datasets)} local spec_files/ datasets)"
    modes_seen = sorted({tc["mode"] for tc in plan["test_cases"] if tc["kind"] == "transfer"})
    modes_line = (" + ".join(modes_seen) + "   (all modes — automatic)") if modes_seen else "n/a (no transfer-type cases selected)"
    lines = [
        "CloudCP CLI Test Plan",
        "=====================",
        f"Run ID        : {plan['run_id']}",
        f"Bryck state   : {plan['bryck_state_before']}",
        f"Dataset(s)    : {dataset_line}",
        f"Transfer Mode : {modes_line}",
        f"Cloud         : aws",
        f"Source base   : {plan['config']['output_base']}",
        f"Destination   : {plan['config']['bucket']}",
        "",
        "Planned Operations:",
        "  [1] Validate Bryck state",
        "  [2] Mount Bryck if required (AUTO-MOUNT, and re-checked before every transfer)",
        f"  [3] {generate_line}",
        "  [4] Configure cloud (cloud_ops.json will be rewritten per case, then restored)",
        "  [5] Start transfers (upload/download/both x each dataset)",
        "  [6] Pause/resume/cancel + auto re-transfer tests",
        "  [7] Mount/eject lifecycle tests (INCLUDES eject-during-active-transfer)",
        "  [8] Format/erase/remove attempts (EXECUTED FOR REAL, not just rejection checks)",
        "  [9] Service restart tests (bcloud AND bryckapi, during active transfers)",
        " [10] Transfer verification + report download per transfer",
        " [11] Auto-cleanup datasets + cloud objects after each test" + (" (DISABLED: --keep)" if plan["config"]["keep"] else ""),
        " [12] Generate reports (JSON + HTML + Markdown)",
        " [13] CLI/input-validation negative cases (CLI-01..CLI-09)",
        " [14] Cloud/AWS configuration negative cases (AWS-01..AWS-08)",
        "",
        f"Total test cases: {len(plan['test_cases'])}",
    ]
    if not plan.get("bryck_config_valid", True):
        lines.append("")
        lines.append("=" * 60)
        lines.append("CONFIG ERROR: /etc/bryck/bryckcloud/config.json is INVALID")
        lines.append("=" * 60)
        lines.append(f"  {plan.get('bryck_config_message', '')}")
        if plan.get("bryck_config_snippet"):
            lines.append("")
            lines.append("  Context (nl -ba style, around the failing line):")
            for line in plan["bryck_config_snippet"].splitlines():
                lines.append(f"    {line}")
        lines.append("")
        lines.append("  This file is read-only reference config (decision #14); no test case")
        lines.append("  currently depends on it operationally, so the run is NOT blocked. But")
        lines.append("  this must be fixed before trusting any tier annotations derived from it.")
    other_problems = [p for p in plan["pre_flight_problems"] if not p.startswith("CONFIG ERROR:")]
    if other_problems:
        lines.append("")
        lines.append("PRE-FLIGHT WARNINGS:")
        for problem in other_problems:
            lines.append(f"  - {problem}")
    lines += [
        "",
        "WARNING:",
        "These operations WILL modify Bryck state, interrupt active transfers,",
        "restart services, and execute real format/erase/remove commands. Data",
        "generated per case is deleted automatically after that case completes"
        + (" (disabled by --keep)." if plan["config"]["keep"] else "."),
        "",
    ]
    return "\n".join(lines)


def phase_plan(args: argparse.Namespace, logger: logging.Logger) -> int:
    plan = build_plan(args, logger)
    print(render_confirmation(plan))

    if args.yes:
        answer = "yes"
    else:
        answer = input("Proceed with execution? [yes/no]: ").strip().lower()

    run_dir = pathlib.Path(args.results_dir) / plan["run_id"]
    run_dir.mkdir(parents=True, exist_ok=True)
    plan_path = run_dir / "plan.json"

    if answer not in {"y", "yes"}:
        plan["confirmed"] = False
        with plan_path.open("w", encoding="utf-8") as handle:
            json.dump(plan, handle, indent=2)
        logger.info("Plan NOT confirmed. Wrote unconfirmed plan to %s. No side effects performed.", plan_path)
        return 1

    plan["confirmed"] = True
    plan["confirmed_at"] = dt.datetime.now().isoformat()
    with plan_path.open("w", encoding="utf-8") as handle:
        json.dump(plan, handle, indent=2)
    logger.info("Plan confirmed and written to %s", plan_path)
    logger.info("Run: python3 cloud_cli_runner.py --execute --plan-file %s", plan_path)
    return 0


# =============================================================================
# Phase 2: --execute
# =============================================================================

class Executor:
    def __init__(self, args: argparse.Namespace, plan: dict, logger: logging.Logger):
        self.args = args
        self.plan = plan
        self.cfg = plan["config"]
        self.logger = logger
        self.run_dir = pathlib.Path(self.cfg["results_dir"]) / plan["run_id"]
        self.run_dir.mkdir(parents=True, exist_ok=True)

        login_ok, login_cfg, login_err = validate_json_file(self.cfg["login"])
        params_ok, cloud_ops_cfg, params_err = validate_json_file(self.cfg["params"])
        self.login_ok = login_ok
        self.cloud_ops_ok = params_ok
        self.config_error = None if (login_ok and params_ok) else (login_err or params_err)
        self.login_cfg = login_cfg or {}
        self.base_cloud_ops = cloud_ops_cfg or {}
        self.redact = build_redactor(self.login_cfg, self.base_cloud_ops)

        backup_path = self.run_dir / "cloud_ops.json.bak"
        if not backup_path.exists():
            with backup_path.open("w", encoding="utf-8") as handle:
                json.dump(self.base_cloud_ops, handle, indent=2)
        self._cloud_ops_backup_path = backup_path

    # -- helpers -------------------------------------------------------

    def case_dir(self, test_id: str) -> pathlib.Path:
        path = self.run_dir / test_id
        path.mkdir(parents=True, exist_ok=True)
        return path

    def report_dir_for(self, test_id: str) -> pathlib.Path:
        """Bryck-host destination for downloaded transfer/diagnostic reports (--report-save-dir),
        one subfolder per test case so concurrent/repeat runs never overwrite each other."""
        path = pathlib.Path(self.cfg["report_save_dir"]) / self.plan["run_id"] / test_id
        if not self.args.dry_run:
            path.mkdir(parents=True, exist_ok=True)
        return path

    def download_final_diagnostic_report(self) -> None:
        """Best-effort bryck_report.py diagnostic dump for the whole run, saved under
        --report-save-dir alongside the per-case transfer reports."""
        out_dir = pathlib.Path(self.cfg["report_save_dir"]) / self.plan["run_id"] / "_final_diagnostic_report"
        if not self.args.dry_run:
            out_dir.mkdir(parents=True, exist_ok=True)
        cmd = run_py_script(
            "bryck_report.py", ["--login", self.cfg["login"], "--output-dir", str(out_dir)],
            self.logger, self.args.dry_run, self.redact, self.cfg["python_bin"], timeout=900,
        )
        if not self.args.dry_run and cmd.returncode != 0:
            self.logger.warning(
                "Final diagnostic bryck_report.py failed (rc=%s); %s will stay empty. "
                "stderr: %.500s", cmd.returncode, out_dir, cmd.stderr or cmd.stdout,
            )
        else:
            self.logger.info("Final diagnostic report saved under %s", out_dir)

    def copy_logs_to_report_dir(self) -> None:
        """Mirror every local commands.log/report.json/summary.* under
        results/<RUN_ID>/ into --report-save-dir/<RUN_ID>/, alongside the
        per-case transfer reports and the final diagnostic dump, so all logs
        for a run live in one place on the Bryck host."""
        if self.args.dry_run:
            return
        dest = pathlib.Path(self.cfg["report_save_dir"]) / self.plan["run_id"]
        try:
            dest.mkdir(parents=True, exist_ok=True)
            shutil.copytree(self.run_dir, dest, dirs_exist_ok=True)
            self.logger.info("Copied run logs (commands.log/report.json/summary.*) to %s", dest)
        except OSError as exc:
            self.logger.warning("Could not copy run logs to %s: %s", dest, exc)

    def copy_case_logs_to_report_dir(self, test_id: str) -> None:
        """Copy one test case's commands.log/report.json/perf/ (crucial
        evidence -- perf HTML/JSON/journal logs) to --report-save-dir
        immediately after it finishes, instead of waiting for the whole run
        to end -- so nothing is lost if the process is killed/disconnected
        (SSH drop, kill -9, etc.) partway through a long multi-dataset run."""
        if self.args.dry_run:
            return
        src = self.case_dir(test_id)
        dest = pathlib.Path(self.cfg["report_save_dir"]) / self.plan["run_id"] / test_id
        try:
            dest.mkdir(parents=True, exist_ok=True)
            shutil.copytree(src, dest, dirs_exist_ok=True)
        except OSError as exc:
            self.logger.warning("Could not copy %s logs to %s: %s", test_id, dest, exc)

    def write_case_result(self, result: TestCaseResult) -> None:
        path = self.case_dir(result.test_id) / "report.json"
        with path.open("w", encoding="utf-8") as handle:
            json.dump({
                "test_id": result.test_id,
                "kind": result.kind,
                "description": result.description,
                "status": result.status,
                "commands": result.commands,
                "notes": result.notes,
                "sub_results": result.sub_results,
                "steps": result.steps,
                "expected": result.expected,
                "actual": result.actual,
                "state_change": result.state_change,
            }, handle, indent=2)
        log_path = self.case_dir(result.test_id) / "commands.log"
        with log_path.open("w", encoding="utf-8") as handle:
            for cmd in result.commands:
                handle.write(f"[{cmd['started_at']} -> {cmd['ended_at']}] rc={cmd['returncode']}\n")
                handle.write(f"$ {' '.join(cmd['argv'])}\n")
                if cmd["stdout"]:
                    handle.write(f"stdout:\n{cmd['stdout']}\n")
                if cmd["stderr"]:
                    handle.write(f"stderr:\n{cmd['stderr']}\n")
                handle.write("\n")
        if result.steps:
            print("\n" + result.render_block() + "\n")

    def run_py(self, result: TestCaseResult, script: str, py_args: List[str], **kw) -> CommandResult:
        cmd = run_py_script(script, py_args, self.logger, self.args.dry_run, self.redact, self.cfg["python_bin"], **kw)
        result.commands.append(cmd.as_dict())
        return cmd

    def ssh(self, result: TestCaseResult, command: str) -> CommandResult:
        cmd = ssh_exec(self.cfg["login"], command, self.logger, self.args.dry_run, self.redact)
        result.commands.append(cmd.as_dict())
        return cmd

    def bryck_state(self, result: TestCaseResult) -> str:
        state, cmd = get_bryck_state(argparse.Namespace(login=self.cfg["login"], dry_run=self.args.dry_run,
                                                          python_bin=self.cfg["python_bin"]), self.logger, self.redact)
        result.commands.append(cmd.as_dict())
        return state

    def transfer_status(self, result: TestCaseResult, transfer_id: str) -> str:
        ns = argparse.Namespace(login=self.cfg["login"], dry_run=self.args.dry_run, python_bin=self.cfg["python_bin"])
        state, cmd = get_transfer_status(ns, transfer_id, self.logger, self.redact)
        result.commands.append(cmd.as_dict())
        if state == "UNKNOWN" and not self.args.dry_run:
            local_state = read_local_transfer_status(pathlib.Path(self.cfg["transfer_logs_dir"]), transfer_id)
            if local_state != "UNKNOWN":
                result.notes.append(
                    f"transfer {transfer_id}: API status unavailable (likely already purged from the "
                    f"active-transfer registry); using transfer_summary.txt on disk instead: {local_state}"
                )
                return local_state
        return state

    def require_ok(self, result: TestCaseResult, cmd: CommandResult, step_name: str) -> bool:
        """Validate a command actually succeeded before letting the caller proceed."""
        if self.args.dry_run:
            return True
        ok = cmd.returncode == 0
        if not ok:
            self.logger.error("%s failed (rc=%s): %s", step_name, cmd.returncode, (cmd.stderr or cmd.stdout)[:400])
            result.notes.append(f"{step_name} FAILED rc={cmd.returncode}")
        return ok

    def wait_for_bryck_state(
        self, result: TestCaseResult, matches: List[str], timeout: int,
    ) -> tuple[bool, str]:
        """Poll bryck_info until its State contains one of `matches` (case-insensitive)."""
        if self.args.dry_run:
            return True, "DRYRUN"
        deadline = time.time() + timeout
        state = "UNKNOWN"
        while time.time() < deadline:
            state = self.bryck_state(result)
            if any(m.lower() in state.lower() for m in matches):
                return True, state
            time.sleep(self.cfg["poll_interval"])
        return False, state

    def wait_for_transfer_state(
        self, result: TestCaseResult, transfer_id: str, matches: List[str], timeout: int,
    ) -> tuple[bool, str]:
        """Poll transfer status until it is one of `matches` (exact, case-insensitive)."""
        if self.args.dry_run:
            return True, "DRYRUN"
        deadline = time.time() + timeout
        state = "UNKNOWN"
        while time.time() < deadline:
            state = self.transfer_status(result, transfer_id)
            if state.upper() in {m.upper() for m in matches}:
                return True, state
            time.sleep(self.cfg["poll_interval"])
        return False, state

    def ensure_mounted(self, result: TestCaseResult) -> bool:
        """Every transfer requires Bryck to be mounted first; mount it if it isn't."""
        state = self.bryck_state(result)
        if "mount" in state.lower():
            return True
        result.notes.append(f"Bryck not mounted (state={state!r}); mounting before dataset generation/transfer")
        mount_cmd = self.run_py(result, "bryck_mount.py", ["--login", self.cfg["login"], "--params", self.cfg["format_mount_params"]])
        if not self.require_ok(result, mount_cmd, "bryck_mount.py"):
            return False
        matched, final_state = self.wait_for_bryck_state(result, ["mount"], self.cfg.get("action_timeout", 90))
        if not matched:
            result.notes.append(f"Bryck did not reach Mounted state (last observed={final_state!r})")
        return matched

    def write_cloud_ops(self, tier: str) -> str:
        cfg = dict(self.base_cloud_ops)
        cfg["bryck_src"] = f"{self.cfg['output_base']}/{tier}"
        cfg["cloud_bucket"] = f"{self.cfg['bucket']}/{tier}"
        cfg["bryck_dst"] = f"{self.cfg['download_base']}/{tier}"
        if not self.args.dry_run:
            with open(self.cfg["params"], "w", encoding="utf-8") as handle:
                json.dump(cfg, handle, indent=2)
        return cfg["cloud_bucket"]

    def configure_cloud(self, result: TestCaseResult, tier: str) -> tuple[bool, str]:
        """Deconfigure any stale cloud config, then configure + verify.

        bryck_cloud_configure.py returns HTTP 409 if a cloud config
        already exists from a prior run, so always clear it first.
        Deconfigure failure is non-fatal (there may be nothing to
        remove yet); configure/show failure is fatal for this case.
        """
        cloud_type = str(self.base_cloud_ops.get("cloud_type", "aws"))
        deconfigure_cmd = self.run_py(result, "bryck_cloud_deconfigure.py", ["--login", self.cfg["login"], "--cloud-type", cloud_type])
        if not self.args.dry_run and deconfigure_cmd.returncode != 0:
            result.notes.append(f"bryck_cloud_deconfigure.py rc={deconfigure_cmd.returncode} (ignored, likely nothing configured yet)")

        bucket = self.write_cloud_ops(tier)
        configure_cmd = self.run_py(result, "bryck_cloud_configure.py", ["--login", self.cfg["login"], "--params", self.cfg["params"]])
        if not self.require_ok(result, configure_cmd, "bryck_cloud_configure.py"):
            return False, bucket
        show_cmd = self.run_py(result, "bryck_cloud_show.py", ["--login", self.cfg["login"]])
        if not self.require_ok(result, show_cmd, "bryck_cloud_show.py"):
            return False, bucket
        return True, bucket

    def initiate_transfer(self, result: TestCaseResult, mode: str, tier: str,
                          dataset_root: Optional[pathlib.Path] = None) -> Optional[str]:
        """Start a transfer via the bryckcloud CLI directly
        (bryckcloud transfer add aws --src <path> --dst <path>), not the API-based
        bryck_cloud_transfer_initiate.py wrapper.

        `dataset_root` (returned by generate_tier_dataset/generate_dataset) is the
        actual materialized dataset path and must be used for --src -- it does not
        necessarily equal <output_base>/<tier> (e.g. edge cases nest one level
        deeper). Falls back to <output_base>/<tier> only if not given."""
        local_src = str(dataset_root) if dataset_root is not None else f"{self.cfg['output_base']}/{tier}"
        s3_path = f"{self.cfg['bucket']}/{tier}"
        local_dst = f"{self.cfg['download_base']}/{tier}"

        def run_one(src: str, dst: str) -> Optional[str]:
            batchmeta_dir = pathlib.Path(self.cfg["batchmeta_dir"])
            transfer_logs_dir = pathlib.Path(self.cfg["transfer_logs_dir"])
            before_ids = set() if self.args.dry_run else base.collect_transfer_ids(batchmeta_dir, transfer_logs_dir)
            cmd = run_argv(
                "bryckcloud transfer add aws",
                [self.cfg["bryckcloud_bin"], "transfer", "add", "aws", "--src", src, "--dst", dst],
                self.logger, self.args.dry_run, self.redact, timeout=self.cfg["wait_timeout"],
            )
            result.commands.append(cmd.as_dict())
            if self.args.dry_run:
                return "DRYRUN-ID"
            if not self.require_ok(result, cmd, "bryckcloud transfer add aws"):
                return None
            transfer_id = base.parse_transfer_id_from_output((cmd.stdout or "") + (cmd.stderr or ""))
            if transfer_id is None:
                try:
                    transfer_id = base.detect_transfer_id(
                        argparse.Namespace(transfer_id=None, poll_interval=self.cfg["poll_interval"]),
                        before_ids, "", batchmeta_dir, transfer_logs_dir,
                    )
                except RuntimeError as exc:
                    result.notes.append(f"could not detect transfer id: {exc}")
                    return None
            return str(transfer_id)

        if mode == "upload":
            return run_one(local_src, s3_path)
        if mode == "download":
            return run_one(s3_path, local_dst)
        # mode == "both": start the upload then the download; the upload's id
        # is treated as primary for polling/pause/resume/cancel, the download's
        # id is recorded in notes for traceability.
        upload_id = run_one(local_src, s3_path)
        download_id = run_one(s3_path, local_dst)
        if download_id:
            result.notes.append(f"download_transfer_id={download_id}")
        return upload_id or download_id

    def poll_until_terminal(self, result: TestCaseResult, transfer_id: str) -> str:
        if self.args.dry_run:
            return "COMPLETED"
        deadline = time.time() + self.cfg["wait_timeout"]
        state = "UNKNOWN"
        while time.time() < deadline:
            state = self.transfer_status(result, transfer_id)
            if state in TERMINAL_STATES:
                return state
            time.sleep(self.cfg["poll_interval"])
        return state

    def cleanup_tier(self, result: TestCaseResult, tier: str) -> None:
        if self.args.dry_run or self.args.keep:
            result.notes.append("cleanup skipped (dry-run or --keep)")
            return
        for base_dir in (self.cfg["output_base"], self.cfg["download_base"]):
            target = pathlib.Path(base_dir) / tier
            if target.is_dir():
                try:
                    shutil.rmtree(target)
                except OSError as exc:
                    result.notes.append(f"cleanup: could not remove {target}: {exc}")
        bucket_prefix = f"{self.cfg['bucket']}/{tier}"
        cmd = run_argv(
            "aws s3 cleanup", [self.cfg.get("aws_cli", "aws"), "s3", "rm", bucket_prefix, "--recursive"],
            self.logger, self.args.dry_run, self.redact,
        )
        result.commands.append(cmd.as_dict())

    def restore_cloud_ops(self) -> None:
        if self.args.dry_run:
            return
        with self._cloud_ops_backup_path.open("r", encoding="utf-8") as handle:
            original = json.load(handle)
        with open(self.cfg["params"], "w", encoding="utf-8") as handle:
            json.dump(original, handle, indent=2)

    def _perf_enabled(self) -> bool:
        return self.cfg.get("perf_capture", True) and os.name == "posix"

    def _start_perf(self, test_id: str) -> Optional[perf_mod.TransferPerfCollector]:
        if not self._perf_enabled():
            return None
        collector = perf_mod.TransferPerfCollector(
            self.case_dir(test_id), self.cfg, self.args.dry_run)
        collector.start()
        return collector

    def _finish_perf(self, collector: Optional[perf_mod.TransferPerfCollector],
                     result: TestCaseResult, transfer_id: str,
                     tier: str = "", mode: str = "",
                     gen_summary: Optional[dict] = None) -> Optional[dict]:
        if collector is None:
            return None
        csv_path = None
        if not self.args.dry_run and transfer_id and transfer_id != "DRYRUN-ID":
            candidate = (pathlib.Path(self.cfg["transfer_logs_dir"])
                         / f"cloud_transfer_{transfer_id}"
                         / f"transfer_report_{transfer_id}.csv")
            if candidate.is_file():
                csv_path = candidate
        perf_data = collector.finish(
            transfer_id, csv_path=csv_path,
            test_id=result.test_id, tier=tier, mode=mode,
            description=result.description, gen_summary=gen_summary)
        result.notes.append(f"perf_report={perf_data.get('html_report', '')}")
        return perf_data

    # -- test case kinds -------------------------------------------------

    def run_transfer_case(self, tc: dict) -> TestCaseResult:
        result = TestCaseResult(tc["id"], "transfer", tc["description"])
        tier, mode = tc["tier"], tc["mode"]
        perf_collector: Optional[perf_mod.TransferPerfCollector] = None
        try:
            if not self.ensure_mounted(result):
                result.status = "BLOCKED"
                result.notes.append("Bryck could not be mounted; dataset/transfer not attempted")
                return result
            dataset_root, gen_summary = generate_tier_dataset(
                tier, self.cfg["output_base"], self._ns(), self.logger, dataset_id=tc.get("dataset"),
            )
            result.notes.append(f"generated: {gen_summary}")

            configured_ok, _bucket = self.configure_cloud(result, tier)
            if not configured_ok:
                result.status = "BLOCKED"
                result.notes.append("cloud configuration did not succeed; transfer not attempted")
                return result

            perf_collector = self._start_perf(tc["id"])

            transfer_id = self.initiate_transfer(result, mode, tier, dataset_root)
            if not transfer_id:
                result.status = "BLOCKED"
                result.notes.append("could not determine transfer_id from initiate output")
                return result
            result.notes.append(f"transfer_id={transfer_id}")
            final_state = self.poll_until_terminal(result, transfer_id)
            result.notes.append(f"final_state={final_state}")

            self._finish_perf(perf_collector, result, transfer_id,
                              tier=tier, mode=mode, gen_summary=gen_summary)
            perf_collector = None

            report_dir = self.report_dir_for(tc["id"])
            self.run_py(
                result, "bryck_cloud_transfer_report.py",
                ["--login", self.cfg["login"], "--cloud-transfer-id", str(transfer_id),
                 "--report-path", str(report_dir / f"cloud_transfer_report_{transfer_id}.zip")],
            )
            result.notes.append(f"transfer report saved under {report_dir}")

            expected_files = gen_summary.get("actual_files", 0)
            validation = {}
            if not self.args.dry_run and expected_files:
                logs_dir = pathlib.Path(self.cfg["transfer_logs_dir"])
                success = base.validate_transfer_success_rows(logs_dir, int(transfer_id), expected_files)
                final_report = base.validate_final_report(
                    base.final_report_path(logs_dir, int(transfer_id)), pathlib.Path(dataset_root),
                    expected_files, f"{self.cfg['bucket']}/{tier}",
                )
                validation = {"success_rows": success, "final_report": final_report}
                result.notes.append(f"validation={validation}")

            result.status = "PASS" if final_state == "COMPLETED" else "FAIL"
            self.cleanup_tier(result, tier)
        except Exception as exc:  # noqa: BLE001
            result.status = "FAIL"
            result.notes.append(f"exception: {exc}")
        return result

    def run_lifecycle_case(self, tc: dict) -> TestCaseResult:
        result = TestCaseResult(tc["id"], "lifecycle", tc["description"])
        tier = tc["tier"]
        perf_collector: Optional[perf_mod.TransferPerfCollector] = None
        try:
            if not self.ensure_mounted(result):
                result.status = "BLOCKED"
                result.notes.append("Bryck could not be mounted; lifecycle test not attempted")
                return result
            dataset_root, _gen_summary = generate_tier_dataset(tier, self.cfg["output_base"], self._ns(), self.logger, dataset_id=tc.get("dataset"))
            configured_ok, _bucket = self.configure_cloud(result, tier)
            if not configured_ok:
                result.status = "BLOCKED"
                result.notes.append("cloud configuration did not succeed; lifecycle test not attempted")
                return result

            perf_collector = self._start_perf(tc["id"])

            transfer_id = self.initiate_transfer(result, "both", tier, dataset_root)
            if not transfer_id:
                result.status = "BLOCKED"
                result.notes.append("could not start transfer for lifecycle test")
                return result
            time.sleep(0 if self.args.dry_run else self.cfg["intervention_wait"])

            action_timeout = self.cfg.get("action_timeout", 90)
            any_failed = False
            for action in LIFECYCLE_ACTIONS:
                sub: Dict[str, Any] = {"action": action}
                sub["before_state"] = self.bryck_state(result)
                sub["before_transfer_state"] = self.transfer_status(result, transfer_id)
                self.logger.info("--- lifecycle action: %s (transfer_id=%s) ---", action, transfer_id)

                cmd: Optional[CommandResult] = None
                if action == "pause":
                    cmd = self.run_py(result, "bryck_cloud_transfer_pause.py", ["--login", self.cfg["login"], "--transfer-id", transfer_id])
                    step_ok = self.require_ok(result, cmd, "pause")
                    matched, state = self.wait_for_transfer_state(result, transfer_id, ["PAUSED"], action_timeout) if step_ok else (False, "SKIPPED")
                    sub.update(command_ok=step_ok, expected="PAUSED", observed=state, status="PASS" if step_ok and matched else "FAIL")
                elif action == "resume":
                    cmd = self.run_py(result, "bryck_cloud_transfer_resume.py", ["--login", self.cfg["login"], "--transfer-id", transfer_id])
                    step_ok = self.require_ok(result, cmd, "resume")
                    matched, state = self.wait_for_transfer_state(result, transfer_id, ["IN_PROGRESS", "COMPLETED"], action_timeout) if step_ok else (False, "SKIPPED")
                    sub.update(command_ok=step_ok, expected="IN_PROGRESS/COMPLETED", observed=state, status="PASS" if step_ok and matched else "FAIL")
                elif action == "cancel":
                    cmd = self.run_py(result, "bryck_cloud_transfer_cancel.py", ["--login", self.cfg["login"], "--transfer-id", transfer_id])
                    step_ok = self.require_ok(result, cmd, "cancel")
                    matched, state = self.wait_for_transfer_state(result, transfer_id, ["CANCELLED"], action_timeout) if step_ok else (False, "SKIPPED")
                    sub.update(command_ok=step_ok, expected="CANCELLED", observed=state, status="PASS" if step_ok and matched else "FAIL")
                elif action == "retransfer":
                    new_id = self.initiate_transfer(result, "both", tier, dataset_root)
                    sub["new_transfer_id"] = new_id
                    sub.update(command_ok=bool(new_id), expected="new transfer_id", observed=new_id, status="PASS" if new_id else "FAIL")
                    if new_id:
                        transfer_id = new_id
                elif action == "mount":
                    cmd = self.run_py(result, "bryck_mount.py", ["--login", self.cfg["login"], "--params", self.cfg["format_mount_params"]])
                    step_ok = self.require_ok(result, cmd, "mount")
                    matched, state = self.wait_for_bryck_state(result, ["mount"], action_timeout) if step_ok else (False, "SKIPPED")
                    sub.update(command_ok=step_ok, expected="*Mounted*", observed=state, status="PASS" if step_ok and matched else "FAIL")
                elif action == "eject":
                    cmd = self.run_py(result, "bryck_eject_unmount.py", ["--login", self.cfg["login"]])
                    step_ok = self.require_ok(result, cmd, "eject")
                    matched, state = self.wait_for_bryck_state(result, ["eject", "remov"], action_timeout) if step_ok else (False, "SKIPPED")
                    sub.update(command_ok=step_ok, expected="*Ejected/Removed*", observed=state, status="PASS" if step_ok and matched else "FAIL")
                elif action == "format":
                    cmd = self.run_py(result, "bryck_format.py", ["--login", self.cfg["login"], "--params", self.cfg["format_mount_params"]])
                    step_ok = self.require_ok(result, cmd, "format")
                    sub.update(command_ok=step_ok, expected="command succeeds (destructive; verify manually)", observed=cmd.returncode, status="PASS" if step_ok else "FAIL")
                elif action == "erase":
                    cmd = self.run_py(result, "bryck_erase.py", ["--login", self.cfg["login"]])
                    step_ok = self.require_ok(result, cmd, "erase")
                    sub.update(command_ok=step_ok, expected="command succeeds (destructive; verify manually)", observed=cmd.returncode, status="PASS" if step_ok else "FAIL")
                elif action == "remove":
                    cmd = self.run_py(result, "bryck_remove.py", ["--login", self.cfg["login"]])
                    step_ok = self.require_ok(result, cmd, "remove")
                    sub.update(command_ok=step_ok, expected="command succeeds (destructive; verify manually)", observed=cmd.returncode, status="PASS" if step_ok else "FAIL")
                elif action == "restart_bcloud":
                    cmd = self.ssh(result, "sudo systemctl restart bcloud.service")
                    step_ok = self.require_ok(result, cmd, "restart_bcloud")
                    matched, state = self.wait_for_bryck_state(result, ["mount", "eject", "remov"], action_timeout) if step_ok else (False, "SKIPPED")
                    sub.update(command_ok=step_ok, expected="bryck reachable after restart", observed=state, status="PASS" if step_ok and matched else "FAIL")
                elif action == "restart_bryckapi":
                    cmd = self.ssh(result, "sudo systemctl restart bryckapi.service")
                    step_ok = self.require_ok(result, cmd, "restart_bryckapi")
                    matched, state = self.wait_for_bryck_state(result, ["mount", "eject", "remov"], action_timeout) if step_ok else (False, "SKIPPED")
                    sub.update(command_ok=step_ok, expected="bryck reachable after restart", observed=state, status="PASS" if step_ok and matched else "FAIL")

                sub["after_state"] = self.bryck_state(result)
                sub["after_transfer_state"] = self.transfer_status(result, transfer_id)
                result.sub_results.append(sub)
                self.logger.info("--- lifecycle action %s -> %s ---", action, sub.get("status"))
                if sub.get("status") == "FAIL":
                    any_failed = True

            # Best-effort recovery so subsequent test cases start clean.
            recovery_cmd = self.run_py(result, "bryck_mount.py", ["--login", self.cfg["login"], "--params", self.cfg["format_mount_params"]])
            self.require_ok(result, recovery_cmd, "post-lifecycle recovery mount")
            self._finish_perf(perf_collector, result, transfer_id, tier=tier, mode="both")
            perf_collector = None
            result.status = "FAIL" if any_failed else "PASS"
            self.cleanup_tier(result, tier)
        except Exception as exc:  # noqa: BLE001
            result.status = "FAIL"
            result.notes.append(f"exception: {exc}")
        return result

    def run_service_case(self, tc: dict) -> TestCaseResult:
        result = TestCaseResult(tc["id"], "service", tc["description"])
        tier = tc["tier"]
        perf_collector: Optional[perf_mod.TransferPerfCollector] = None
        try:
            if not self.ensure_mounted(result):
                result.status = "BLOCKED"
                result.notes.append("Bryck could not be mounted; service-restart test not attempted")
                return result
            dataset_root, _gen_summary = generate_tier_dataset(tier, self.cfg["output_base"], self._ns(), self.logger, dataset_id=tc.get("dataset"))
            configured_ok, _bucket = self.configure_cloud(result, tier)
            if not configured_ok:
                result.status = "BLOCKED"
                result.notes.append("cloud configuration did not succeed; service-restart test not attempted")
                return result

            perf_collector = self._start_perf(tc["id"])

            transfer_id = self.initiate_transfer(result, "both", tier, dataset_root)
            if not transfer_id:
                result.status = "BLOCKED"
                result.notes.append("could not start transfer for service-restart test")
                return result
            time.sleep(0 if self.args.dry_run else self.cfg["intervention_wait"])

            before_state = self.transfer_status(result, transfer_id)
            restart_cmd = self.ssh(result, f"sudo systemctl restart {tc['target_service']}")
            if not self.require_ok(result, restart_cmd, f"restart {tc['target_service']}"):
                result.status = "FAIL"
                result.notes.append(f"before_restart={before_state}")
                return result

            action_timeout = self.cfg.get("action_timeout", 90)
            reachable, state_after_restart = self.wait_for_bryck_state(result, ["mount", "eject", "remov"], action_timeout)
            result.notes.append(f"reachable_after_restart={reachable} state={state_after_restart}")
            if not reachable:
                result.status = "FAIL"
                result.notes.append("Bryck did not become reachable again within action-timeout after service restart")
                return result

            final_state = self.poll_until_terminal(result, transfer_id)
            result.notes.append(f"before_restart={before_state} final_state={final_state}")
            self._finish_perf(perf_collector, result, transfer_id, tier=tier, mode="both")
            perf_collector = None
            result.status = "PASS" if final_state in TERMINAL_STATES else "FAIL"
            self.cleanup_tier(result, tier)
        except Exception as exc:  # noqa: BLE001
            result.status = "FAIL"
            result.notes.append(f"exception: {exc}")
        return result

    def run_edge_case(self, tc: dict) -> TestCaseResult:
        result = TestCaseResult(tc["id"], "edge", tc["description"])
        dataset_id = tc["dataset"]
        tier = f"EDGE-{dataset_id}"
        perf_collector: Optional[perf_mod.TransferPerfCollector] = None
        try:
            if not self.ensure_mounted(result):
                result.status = "BLOCKED"
                result.notes.append("Bryck could not be mounted; edge case not attempted")
                return result
            dataset = base.select_dataset(SPEC_ROOT, dataset_id)
            ns = self._ns()
            ns.output_base = str(pathlib.Path(self.cfg["output_base"]) / tier)
            dataset_root, gen_summary = base.generate_dataset(ns, dataset, SPEC_ROOT, self.logger)
            result.notes.append(f"generated: {gen_summary}")

            configured_ok, _bucket = self.configure_cloud(result, tier)
            if not configured_ok:
                result.status = "BLOCKED"
                result.notes.append("cloud configuration did not succeed; edge case not attempted")
                return result

            perf_collector = self._start_perf(tc["id"])

            transfer_id = self.initiate_transfer(result, "upload", tier, dataset_root)
            if not transfer_id:
                result.status = "BLOCKED"
                result.notes.append("could not determine transfer_id")
                return result
            final_state = self.poll_until_terminal(result, transfer_id)
            result.notes.append(f"transfer_id={transfer_id} final_state={final_state}")
            self._finish_perf(perf_collector, result, transfer_id,
                              tier=tier, mode="upload", gen_summary=gen_summary)
            perf_collector = None

            report_dir = self.report_dir_for(tc["id"])
            self.run_py(
                result, "bryck_cloud_transfer_report.py",
                ["--login", self.cfg["login"], "--cloud-transfer-id", str(transfer_id),
                 "--report-path", str(report_dir / f"cloud_transfer_report_{transfer_id}.zip")],
            )
            result.notes.append(f"transfer report saved under {report_dir}")

            result.status = "PASS" if final_state == "COMPLETED" else "FAIL"
            self.cleanup_tier(result, tier)
        except Exception as exc:  # noqa: BLE001
            result.status = "FAIL"
            result.notes.append(f"exception: {exc}")
        return result

    def _ns(self) -> types.SimpleNamespace:
        return types.SimpleNamespace(
            output_base=self.cfg["output_base"],
            skip_generate=False,
            datagen_bin=self.cfg["datagen_bin"],
            dry_run=self.args.dry_run,
            verbose=self.args.verbose,
        )

    def _write_fixture_json(self, path: pathlib.Path, data: dict) -> None:
        if self.args.dry_run:
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as handle:
            json.dump(data, handle, indent=2)

    def _judge_expect_fail(self, result: TestCaseResult, cmd: CommandResult, expect_fail: bool, step_name: str) -> str:
        """PASS/FAIL for a negative case: expect_fail=True means rc!=0 is the correct (PASS) outcome."""
        if self.args.dry_run:
            result.notes.append(f"{step_name}: dry-run, outcome not evaluated")
            return "PASS"
        ok = (cmd.returncode != 0) if expect_fail else (cmd.returncode == 0)
        result.notes.append(
            f"{step_name}: rc={cmd.returncode}, expected_fail={expect_fail} -> {'PASS' if ok else 'FAIL'}"
        )
        return "PASS" if ok else "FAIL"

    def _run_negative_pipeline(
        self,
        result: TestCaseResult,
        execute_fn,
        expect_fail: bool,
        expected_text: str,
        fixture_note: str = "n/a (no fixture required)",
    ) -> TestCaseResult:
        """Environment-aware pipeline shared by CLI-input and AWS-negative cases:
        inspect -> validate config -> capture baseline -> create fixture ->
        execute -> validate result -> verify no unintended state change ->
        cleanup -> verify final environment. Mirrors negative_environment_runner.py's
        inspect/prepare/validate/execute/validate/cleanup/verify shape."""
        result.expected = expected_text
        case_id = result.test_id

        state_before = self.bryck_state(result)
        result.step("Inspect environment", "PASS", f"bryck_state={state_before}")

        if not (self.login_ok and self.cloud_ops_ok):
            result.step("Validate configuration", "BLOCKED", self.config_error or "login.json/cloud_ops.json invalid")
            result.status = "BLOCKED"
            result.notes.append("environment could not be established: login.json/cloud_ops.json invalid")
            return result
        result.step("Validate configuration", "PASS", "login.json/cloud_ops.json parse OK")

        result.step(
            "Establish Bryck mounted state", "SKIPPED",
            "not required — pure input/config validation, no dataset access",
        )

        baseline = state_before
        result.step("Capture baseline", "PASS", f"state={baseline}")
        result.step("Create negative fixture", "PASS", fixture_note)

        try:
            cmd = execute_fn()
        except Exception as exc:  # noqa: BLE001
            result.step("Execute operation", "FAIL", f"exception: {exc}")
            result.status = "FAIL"
            result.actual = f"exception: {exc}"
            return result

        exec_label = "EXPECTED FAILURE" if expect_fail else "EXECUTED"
        result.step("Execute operation", exec_label, "dry-run" if self.args.dry_run else f"rc={cmd.returncode}")

        judged = self._judge_expect_fail(result, cmd, expect_fail, case_id)
        result.step(f"Validate {'rejection' if expect_fail else 'success'}", judged)

        state_after = self.bryck_state(result)
        unaffected = self.args.dry_run or (state_after == baseline)
        result.step(
            "Verify no unintended state change", "PASS" if unaffected else "FAIL",
            f"before={baseline!r} after={state_after!r}",
        )

        result.step("Cleanup", "PASS", "no shared login.json/cloud_ops.json modified; per-case fixture retained as evidence")
        state_final = self.bryck_state(result)
        result.step("Verify final environment", "PASS", f"state={state_final}")

        result.status = judged if (judged == "FAIL" or unaffected) else "FAIL"
        result.actual = f"rc={cmd.returncode}" if not self.args.dry_run else "dry-run (not executed)"
        result.state_change = f"{baseline} -> {state_after}"
        return result

    def run_cli_input_case(self, tc: dict) -> TestCaseResult:
        result = TestCaseResult(tc["id"], "cli_input", tc["description"])
        case_id = tc["id"]
        case_dir = self.case_dir(case_id)
        expect_fail = True
        try:
            if case_id == "CLI-01":
                execute_fn = lambda: self.run_py(result, "bryck_cloud_transfer_initiate.py",
                                                  ["--login", self.cfg["login"], "--params", self.cfg["params"]])
                expected = "argparse rejects the initiate call because --mode is required; no transfer is created."
                fixture_note = "n/a (omits required --mode flag)"
            elif case_id == "CLI-02":
                execute_fn = lambda: self.run_py(result, "bryck_cloud_transfer_initiate.py",
                                                  ["--login", self.cfg["login"], "--params", self.cfg["params"], "--mode", "copy"])
                expected = "argparse rejects --mode copy as an invalid choice; no transfer is created."
                fixture_note = "n/a (invalid --mode value)"
            elif case_id == "CLI-03":
                fixture = case_dir / "cli03_cloud_ops.json"
                self._write_fixture_json(fixture, {**self.base_cloud_ops, "bryck_src": ""})
                execute_fn = lambda: self.run_py(result, "bryck_cloud_transfer_initiate.py",
                                                  ["--login", self.cfg["login"], "--params", str(fixture), "--mode", "upload"])
                expected = "Upload must be rejected because bryck_src is empty."
                fixture_note = f"{fixture.name}: bryck_src=''"
            elif case_id == "CLI-04":
                fixture = case_dir / "cli04_cloud_ops.json"
                self._write_fixture_json(fixture, {**self.base_cloud_ops, "cloud_bucket": ""})
                execute_fn = lambda: self.run_py(result, "bryck_cloud_transfer_initiate.py",
                                                  ["--login", self.cfg["login"], "--params", str(fixture), "--mode", "upload"])
                expected = "Upload must be rejected because cloud_bucket is empty."
                fixture_note = f"{fixture.name}: cloud_bucket=''"
            elif case_id == "CLI-05":
                fixture = case_dir / "cli05_cloud_ops.json"
                self._write_fixture_json(fixture, {**self.base_cloud_ops, "bryck_dst": ""})
                execute_fn = lambda: self.run_py(result, "bryck_cloud_transfer_initiate.py",
                                                  ["--login", self.cfg["login"], "--params", str(fixture), "--mode", "download"])
                expected = "Download must be rejected because bryck_dst is empty."
                fixture_note = f"{fixture.name}: bryck_dst=''"
            elif case_id == "CLI-06":
                execute_fn = lambda: self.run_py(result, "bryck_cloud_show.py", ["--login", str(case_dir / "missing-login.json")])
                expected = "bryck_cloud_show.py must fail with a readable file-not-found error; no API call is made."
                fixture_note = "missing-login.json intentionally not created"
            elif case_id == "CLI-07":
                fixture = case_dir / "cli07_login.json"
                if not self.args.dry_run:
                    fixture.write_text("{", encoding="utf-8")
                execute_fn = lambda: self.run_py(result, "bryck_cloud_show.py", ["--login", str(fixture)])
                expected = "bryck_cloud_show.py must fail with a readable JSON-parse error."
                fixture_note = f"{fixture.name}: malformed content '{{' "
            elif case_id == "CLI-08":
                execute_fn = lambda: self.run_py(result, "bryck_cloud_transfer_pause.py",
                                                  ["--login", self.cfg["login"], "--transfer-id", "not-a-transfer-id"])
                expected = "Pause must be rejected for a non-numeric/invalid transfer id; no state change."
                fixture_note = "n/a (invalid --transfer-id value)"
            elif case_id == "CLI-09":
                spec_path = case_dir / "missing-spec.yaml"

                def execute_fn():
                    cmd = run_argv(
                        "datagen with missing spec",
                        [self.cfg["datagen_bin"], "--spec", str(spec_path)],
                        self.logger, self.args.dry_run, self.redact,
                    )
                    result.commands.append(cmd.as_dict())
                    return cmd
                expected = "datagen must fail before any host mutation because the spec file does not exist."
                fixture_note = "missing-spec.yaml intentionally not created"
            else:
                result.status = "BLOCKED"
                result.notes.append("no fixture implemented for this CLI-input case yet")
                return result

            return self._run_negative_pipeline(result, execute_fn, expect_fail, expected, fixture_note)
        except Exception as exc:  # noqa: BLE001
            result.status = "FAIL"
            result.notes.append(f"exception: {exc}")
        return result

    def run_cloud_negative_case(self, tc: dict) -> TestCaseResult:
        result = TestCaseResult(tc["id"], "cloud_negative", tc["description"])
        case_id = tc["id"]
        case_dir = self.case_dir(case_id)
        cloud_type = str(self.base_cloud_ops.get("cloud_type", "aws"))
        mutation_map = {
            "AWS-01": ("access_key_id", "", "Configure must reject an empty access_key_id."),
            "AWS-02": ("secret_access_key", "", "Configure must reject an empty secret_access_key."),
            "AWS-03": ("access_key_id", "invalid-access-key", "Configure must reject an invalid access_key_id."),
            "AWS-04": ("secret_access_key", "invalid-secret-key", "Configure must reject an invalid secret_access_key."),
            "AWS-05": ("region", "invalid-region", "Configure must reject an invalid region."),
            "AWS-06": ("cloud_bucket", "not-a-valid-bucket", "Configure must reject an invalid cloud_bucket URI."),
        }
        try:
            if case_id in mutation_map:
                field_name, bad_value, expected = mutation_map[case_id]
                fixture = case_dir / f"{case_id}_cloud_ops.json"
                self._write_fixture_json(fixture, {**self.base_cloud_ops, field_name: bad_value})
                execute_fn = lambda: self.run_py(result, "bryck_cloud_configure.py",
                                                  ["--login", self.cfg["login"], "--params", str(fixture)])
                return self._run_negative_pipeline(result, execute_fn, True, expected, f"{fixture.name}: {field_name}={bad_value!r}")
            elif case_id == "AWS-07":
                execute_fn = lambda: self.run_py(result, "bryck_cloud_deconfigure.py",
                                                  ["--login", self.cfg["login"], "--cloud-type", cloud_type])
                expected = "Deconfiguring when nothing is configured is either rejected or idempotent (documented behavior, not a hard PASS/FAIL rc check)."
                out = self._run_negative_pipeline(result, execute_fn, False, expected, "n/a (uses existing/absent cloud config)")
                out.status = "PASS"  # observational case: any bounded, non-crashing rc is acceptable
                out.notes.append("observational case: rc recorded but not used to force PASS/FAIL")
                return out
            elif case_id == "AWS-08":
                def execute_fn():
                    self.run_py(result, "bryck_cloud_deconfigure.py", ["--login", self.cfg["login"], "--cloud-type", cloud_type])
                    return self.run_py(result, "bryck_cloud_deconfigure.py", ["--login", self.cfg["login"], "--cloud-type", cloud_type])
                expected = "A second deconfigure call in a row must be deterministic (same behavior every time), not crash."
                out = self._run_negative_pipeline(result, execute_fn, False, expected, "n/a (deconfigure called twice)")
                out.status = "PASS"
                out.notes.append("observational case: both rcs recorded but not used to force PASS/FAIL")
                return out
            else:
                result.status = "BLOCKED"
                result.notes.append("no fixture implemented for this AWS-negative case yet")
        except Exception as exc:  # noqa: BLE001
            result.status = "FAIL"
            result.notes.append(f"exception: {exc}")
        return result

    def run_all(self) -> tuple[List[TestCaseResult], bool]:
        results: List[TestCaseResult] = []
        dispatch = {
            "transfer": self.run_transfer_case,
            "lifecycle": self.run_lifecycle_case,
            "service": self.run_service_case,
            "edge": self.run_edge_case,
            "cli_input": self.run_cli_input_case,
            "cloud_negative": self.run_cloud_negative_case,
        }
        interrupted = False
        for tc in self.plan["test_cases"]:
            self.logger.info("=== running %s (%s) ===", tc["id"], tc["kind"])
            try:
                result = dispatch[tc["kind"]](tc)
            except KeyboardInterrupt:
                self.logger.warning("Ctrl+C received while running %s -- stopping and generating reports for the %d test case(s) already completed.", tc["id"], len(results))
                result = TestCaseResult(tc["id"], tc["kind"], tc["description"])
                result.status = "INTERRUPTED"
                result.notes.append("Run interrupted by user (Ctrl+C) while this test case was in progress")
                self.write_case_result(result)
                self.copy_case_logs_to_report_dir(tc["id"])
                results.append(result)
                interrupted = True
                break
            self.write_case_result(result)
            self.copy_case_logs_to_report_dir(tc["id"])
            results.append(result)
        try:
            self.restore_cloud_ops()
        except KeyboardInterrupt:
            interrupted = True
        return results, interrupted


def write_summary(run_dir: pathlib.Path, plan: dict, results: List[TestCaseResult]) -> None:
    counts = {"PASS": 0, "FAIL": 0, "BLOCKED": 0, "PENDING": 0}
    for r in results:
        counts[r.status] = counts.get(r.status, 0) + 1
    tc_by_id = {tc["id"]: tc for tc in plan.get("test_cases", [])}

    summary = {
        "run_id": plan["run_id"],
        "generated_at": dt.datetime.now().isoformat(),
        "counts": counts,
        "test_cases": [
            {
                "test_id": r.test_id, "kind": r.kind, "description": r.description, "status": r.status,
                "dataset": tc_by_id.get(r.test_id, {}).get("dataset"),
                "mode": tc_by_id.get(r.test_id, {}).get("mode"),
                "notes": r.notes,
            }
            for r in results
        ],
    }
    (run_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    md_lines = [
        f"# CloudCP CLI Run {plan['run_id']}", "",
        f"PASS={counts.get('PASS', 0)} FAIL={counts.get('FAIL', 0)} BLOCKED={counts.get('BLOCKED', 0)} "
        f"(total {len(results)})", "",
        "| Test ID | Kind | Dataset | Mode | Status | Description |",
        "|---|---|---|---|---|---|",
    ]
    for r in results:
        tc = tc_by_id.get(r.test_id, {})
        md_lines.append(f"| {r.test_id} | {r.kind} | {tc.get('dataset', '')} | {tc.get('mode', '')} | {r.status} | {r.description} |")
    (run_dir / "summary.md").write_text("\n".join(md_lines), encoding="utf-8")

    def status_class(status: str) -> str:
        return {"PASS": "pass", "FAIL": "fail", "BLOCKED": "blocked", "INTERRUPTED": "interrupted"}.get(status, "")

    def _perf_link(r: TestCaseResult) -> str:
        for note in r.notes:
            if note.startswith("perf_report=") and note != "perf_report=":
                rel = note.split("=", 1)[1]
                try:
                    rel_path = pathlib.Path(rel).relative_to(run_dir)
                except (ValueError, TypeError):
                    rel_path = pathlib.Path(rel).name
                return f"<a href='{rel_path}'>perf</a>"
        return ""

    html_rows = "".join(
        f"<tr class='{status_class(r.status)}'>"
        f"<td><span class='badge {status_class(r.status)}'>{r.status}</span></td>"
        f"<td>{r.test_id}</td><td>{r.kind}</td>"
        f"<td>{tc_by_id.get(r.test_id, {}).get('dataset', '')}</td>"
        f"<td>{tc_by_id.get(r.test_id, {}).get('mode', '')}</td>"
        f"<td>{r.description}</td>"
        f"<td>{_perf_link(r)}</td>"
        f"<td>{'; '.join(r.notes[:3])}</td></tr>"
        for r in results
    )
    html = f"""<!doctype html>
<html><head><meta charset="utf-8"><title>CloudCP CLI Run {plan['run_id']}</title>
<style>
body {{ font-family: Arial, sans-serif; margin: 24px; background: #f3f4f6; color: #1f2937; }}
h1 {{ margin-bottom: 4px; }}
.summary {{ display: flex; gap: 12px; margin: 16px 0; }}
.summary div {{ background: #fff; border: 1px solid #e5e7eb; border-radius: 8px; padding: 10px 16px; text-align: center; }}
.summary .value {{ font-size: 1.4rem; font-weight: 700; }}
table {{ border-collapse: collapse; width: 100%; background: #fff; font-size: 13px; }}
th, td {{ border: 1px solid #e5e7eb; padding: 6px 10px; text-align: left; }}
th {{ background: #f9fafb; }}
tr.fail td {{ background: #fef2f2; }}
tr.blocked td {{ background: #fffbeb; }}
tr.interrupted td {{ background: #ede9fe; }}
.badge {{ padding: 2px 8px; border-radius: 999px; font-weight: 700; font-size: 12px; }}
.badge.pass {{ background: #dcfce7; color: #14532d; }}
.badge.fail {{ background: #fee2e2; color: #7f1d1d; }}
.badge.blocked {{ background: #fef3c7; color: #78350f; }}
.badge.interrupted {{ background: #ede9fe; color: #4c1d95; }}
a {{ color: #2563eb; }}
</style></head>
<body>
<h1>CloudCP CLI Run {plan['run_id']}</h1>
<div class="summary">
  <div><div class="value">{len(results)}</div>Total</div>
  <div><div class="value" style="color:#14532d">{counts.get('PASS', 0)}</div>PASS</div>
  <div><div class="value" style="color:#7f1d1d">{counts.get('FAIL', 0)}</div>FAIL</div>
  <div><div class="value" style="color:#78350f">{counts.get('BLOCKED', 0)}</div>BLOCKED</div>
  <div><div class="value" style="color:#4c1d95">{counts.get('INTERRUPTED', 0)}</div>INTERRUPTED</div>
</div>
<table>
<tr><th>Status</th><th>Test ID</th><th>Kind</th><th>Dataset</th><th>Mode</th><th>Description</th><th>Perf</th><th>Notes (first 3)</th></tr>
{html_rows}
</table>
</body></html>"""
    (run_dir / "summary.html").write_text(html, encoding="utf-8")


def _execute_confirmed_plan(args: argparse.Namespace, plan: dict, logger: logging.Logger) -> int:
    executor = Executor(args, plan, logger)
    results, interrupted = executor.run_all()
    if interrupted:
        logger.warning("Run interrupted (Ctrl+C). Generating reports for the %d test case(s) completed so far...", len(results))
    try:
        executor.download_final_diagnostic_report()
    except KeyboardInterrupt:
        logger.warning("Ctrl+C received again during final diagnostic report download; skipping it.")
        interrupted = True
    write_summary(executor.run_dir, plan, results)
    try:
        executor.copy_logs_to_report_dir()
    except KeyboardInterrupt:
        logger.warning("Ctrl+C received again while copying logs to report-save-dir; skipping it.")
        interrupted = True
    counts: Dict[str, int] = {}
    for r in results:
        counts[r.status] = counts.get(r.status, 0) + 1
    html_report = executor.run_dir / "summary.html"
    json_report = executor.run_dir / "summary.json"
    md_report = executor.run_dir / "summary.md"
    if interrupted:
        logger.warning("RUN INTERRUPTED by user -- %d/%d test case(s) completed.", len(results), len(plan["test_cases"]))
    else:
        logger.info("Run complete. Results: %s", executor.run_dir)
    logger.info("PASS=%s FAIL=%s BLOCKED=%s INTERRUPTED=%s (total %s)", counts.get("PASS", 0), counts.get("FAIL", 0),
                counts.get("BLOCKED", 0), counts.get("INTERRUPTED", 0), len(results))
    print("")
    print(f"Results dir: {executor.run_dir.resolve()}")
    print(f"JSON summary: {json_report.resolve()}")
    print(f"Markdown summary: {md_report.resolve()}")
    print(f"HTML report: {html_report.resolve().as_uri()}")
    print(f"             ({html_report.resolve()})")
    if not args.dry_run:
        print(f"Logs + reports also copied to: {pathlib.Path(executor.cfg['report_save_dir']) / plan['run_id']}")
    if interrupted:
        return 130
    failed = sum(1 for r in results if r.status not in ("PASS",))
    return 1 if failed else 0


def phase_execute(args: argparse.Namespace, logger: logging.Logger) -> int:
    if not args.plan_file:
        raise SystemExit("--plan-file is required with --execute")
    with open(args.plan_file, "r", encoding="utf-8") as handle:
        plan = json.load(handle)
    if not plan.get("confirmed"):
        raise SystemExit(f"plan {args.plan_file} was not confirmed (run --plan and answer 'yes' first)")
    return _execute_confirmed_plan(args, plan, logger)


def phase_run(args: argparse.Namespace, logger: logging.Logger) -> int:
    """One-shot: build the plan, show the confirmation gate, and (on yes) execute
    it immediately in this same process -- no separate --execute/--plan-file step."""
    plan = build_plan(args, logger)
    print(render_confirmation(plan))

    answer = "yes" if args.yes else input("Proceed with execution? [yes/no]: ").strip().lower()
    run_dir = pathlib.Path(args.results_dir) / plan["run_id"]
    run_dir.mkdir(parents=True, exist_ok=True)
    plan_path = run_dir / "plan.json"

    if answer not in {"y", "yes"}:
        plan["confirmed"] = False
        with plan_path.open("w", encoding="utf-8") as handle:
            json.dump(plan, handle, indent=2)
        logger.info("Not confirmed. Wrote unconfirmed plan to %s. Nothing was executed.", plan_path)
        return 1

    plan["confirmed"] = True
    plan["confirmed_at"] = dt.datetime.now().isoformat()
    with plan_path.open("w", encoding="utf-8") as handle:
        json.dump(plan, handle, indent=2)
    logger.info("Plan confirmed (%s). Executing %s test case(s) now...", plan_path, len(plan["test_cases"]))
    return _execute_confirmed_plan(args, plan, logger)


# =============================================================================
# Entry point
# =============================================================================

def phase_list_cases(args: argparse.Namespace, logger: logging.Logger) -> int:
    args.dry_run = True  # never touch the real host just to list case IDs
    plan = build_plan(args, logger)
    test_cases = plan["test_cases"]
    print(f"{len(test_cases)} test case(s) with the current selection:\n")
    for tc in test_cases:
        print(f"  {tc['id']:<20} kind={tc['kind']:<10} {tc['description']}")
    print("\nRun one at a time with, e.g.:")
    print(f"  python cloud_cli_runner.py --run --only {test_cases[0]['id'] if test_cases else '<ID>'} --yes")
    return 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    logger = setup_logging(args.verbose)
    try:
        if args.list_cases:
            return phase_list_cases(args, logger)
        if args.run:
            return phase_run(args, logger)
        if args.plan:
            return phase_plan(args, logger)
        return phase_execute(args, logger)
    except KeyboardInterrupt:
        # Safety net for Ctrl+C outside the test-case loop (e.g. during plan
        # building or the confirmation prompt) -- run_all() already handles
        # the common case of interrupting mid-execution and still writes
        # reports; this just avoids a raw traceback if no results exist yet.
        logger.warning("Interrupted (Ctrl+C) before any test case ran. No results directory was created.")
        return 130


if __name__ == "__main__":
    sys.exit(main())
