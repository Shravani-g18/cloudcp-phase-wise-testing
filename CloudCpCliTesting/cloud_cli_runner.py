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
FALLBACK_SPEC_ROOT = REPO_ROOT / "CloudCpFallbackTesting" / "spec_files"
RESULTS_ROOT = HERE / "results"

sys.path.insert(0, str(HERE))
sys.path.insert(0, str(BRYCK_CLI_DIR))

import cloudcpclitesting as base  # noqa: E402  (dataset/report helpers reuse)

DEFAULT_DATAGEN = "/home/bryck/rperiyas/datagen"
DEFAULT_PYTHON_BIN = "python3"
DEFAULT_BATCHMETA = "/opt/bryck/bryckapi/downloads/bcloud_batchmeta"
DEFAULT_TRANSFER_LOGS = "/opt/bryck/bryckapi/downloads/cloud_transfer_logs"
DEFAULT_BRYCK_CONFIG_JSON = "/etc/bryck/bryckcloud/config.json"

TIER_DATASET_MAP = {
    "ZERO": "DS-P1-01",
    "TINY": "DS-P1-02",
    "SMALL": "DS-P1-03",
    "MEDIUM": "DS-P1-04",
    "LARGE": "DS-P1-05",
}
ALL_TIERS = ["ZERO", "TINY", "SMALL", "MEDIUM", "LARGE", "SPARSE"]
SPARSE_SPEC_FILE = FALLBACK_SPEC_ROOT / "06_sparse_files.yaml"
SPEC_FILES_DIR = HERE / "spec_files"

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
    mode.add_argument("--plan", action="store_true", help="Phase 1: build + confirm the plan. No side effects.")
    mode.add_argument("--execute", action="store_true", help="Phase 2: run the confirmed plan.")

    parser.add_argument("--plan-file", help="Path to plan.json (required for --execute).")
    parser.add_argument("--tiers", nargs="+", default=ALL_TIERS, choices=ALL_TIERS,
                         help="Subset of tiers to include (default: all). Ignored when --dataset-catalog all is used.")
    parser.add_argument("--modes", nargs="+", default=MODES, choices=MODES,
                         help="Subset of transfer modes to include (default: all).")
    parser.add_argument("--dataset-catalog", choices=["tiers", "all", "specfiles"], default="tiers",
                         help="'tiers' (default) runs one representative dataset per size tier. "
                              "'all' runs every dataset in dataset_cloudcp/spec_files/manifest.json "
                              "(optionally narrowed with --datasets) as its own transfer round. "
                              "'specfiles' runs every *.yaml spec under CloudCpCliTesting/spec_files/ "
                              "(optionally narrowed with --datasets, e.g. 01_zero_byte 12_tiny_2million).")
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
                         help="Build/run only these test-case IDs (e.g. --only CLI-U-ZERO), ignoring --tiers/--modes/--include-*.")

    parser.add_argument("--login", default=str(BRYCK_CLI_DIR / "login.json"))
    parser.add_argument("--params", default=str(BRYCK_CLI_DIR / "cloud_ops.json"),
                         help="cloud_ops.json path (dynamically rewritten per test case).")
    parser.add_argument("--format-mount-params", default=str(BRYCK_CLI_DIR / "format_mount_params.json"))
    parser.add_argument("--bryck-config-json", default=DEFAULT_BRYCK_CONFIG_JSON,
                         help="Read-only reference config (decision #14); only tier names are read from it.")

    parser.add_argument("--output-base", default="/bryck/cloudcp_cli",
                         help="Bryck-side root for materialized upload datasets.")
    parser.add_argument("--download-base", default="/bryck/cloudcp_cli_dl",
                         help="Bryck-side root for download-mode destinations.")
    parser.add_argument("--bucket", default="s3://aditya/cloudcp-cli",
                         help="S3 bucket+prefix root; each tier gets its own sub-prefix.")

    parser.add_argument("--datagen-bin", default=DEFAULT_DATAGEN)
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
    parser.add_argument("--run-id", default=None, help="Override the generated RUN_ID.")
    parser.add_argument("--keep", "--no-cleanup", dest="keep", action="store_true",
                         help="Skip auto-cleanup of datasets/cloud objects (debugging).")
    parser.add_argument("--aws-cli", default="aws", help="aws CLI binary used for S3 cleanup.")

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
        state = str(payload.get("bryck_info", {}).get("State", "UNKNOWN")).strip()
        return state or "UNKNOWN", result
    except (json.JSONDecodeError, AttributeError):
        return "UNKNOWN", result


def get_transfer_status(args: argparse.Namespace, transfer_id: str, logger: logging.Logger, redact) -> tuple[str, CommandResult]:
    result = run_py_script(
        "bryck_cloud_transfer_status.py",
        ["--login", args.login, "--transfer-id", str(transfer_id)],
        logger, args.dry_run, redact, args.python_bin,
    )
    if args.dry_run or result.returncode != 0:
        return "UNKNOWN", result
    match = re.search(r'"state"\s*:\s*"([A-Z_]+)"', result.stdout)
    return (match.group(1) if match else "UNKNOWN"), result


def parse_transfer_id(text: str) -> Optional[str]:
    match = re.search(r"transfer[_ ]?id[^0-9]*(\d+)", text, re.IGNORECASE)
    return match.group(1) if match else None


# =============================================================================
# Dataset resolution (reuses cloudcpclitesting.py helpers where possible)
# =============================================================================

def resolve_tier_dataset(tier: str) -> "base.DatasetSelection":
    dataset_id = TIER_DATASET_MAP[tier]
    return base.select_dataset(SPEC_ROOT, dataset_id)


def all_catalog_dataset_ids() -> List[str]:
    """Every dataset id declared in dataset_cloudcp/spec_files/manifest.json, sorted."""
    _manifest, dataset_map = base.load_manifest(SPEC_ROOT)
    return sorted(dataset_map)


def local_spec_catalog_ids() -> List[str]:
    """Every *.yaml spec name under CloudCpCliTesting/spec_files/, sorted."""
    return sorted(p.stem for p in SPEC_FILES_DIR.glob("*.yaml"))


def local_spec_file_path(name: str) -> Optional[pathlib.Path]:
    candidate = SPEC_FILES_DIR / f"{name}.yaml"
    return candidate if candidate.is_file() else None


def generate_tier_dataset(
    tier: str,
    output_base: str,
    args: argparse.Namespace,
    logger: logging.Logger,
    dataset_id: Optional[str] = None,
) -> tuple[pathlib.Path, dict]:
    """Materialize one dataset under output_base/<TIER>, reusing the
    single-dataset datagen flow already validated by cloudcpclitesting.py.

    `dataset_id` overrides the tier->dataset lookup so any dataset in the
    manifest catalog, or any single-spec YAML under CloudCpCliTesting/spec_files/,
    can be driven through the same tier-shaped folder layout (used by
    --dataset-catalog all / --dataset-catalog specfiles).
    """
    ns = types.SimpleNamespace(
        output_base=str(pathlib.Path(output_base) / tier),
        skip_generate=False,
        datagen_bin=args.datagen_bin,
        dry_run=args.dry_run,
        verbose=args.verbose,
    )
    local_spec = local_spec_file_path(dataset_id) if dataset_id else None
    if local_spec is not None:
        return generate_named_spec_dataset(local_spec, tier, output_base, ns, logger)
    if tier == "SPARSE" and dataset_id is None:
        return generate_named_spec_dataset(SPARSE_SPEC_FILE, "SPARSE", output_base, ns, logger)
    dataset = base.select_dataset(SPEC_ROOT, dataset_id) if dataset_id else resolve_tier_dataset(tier)
    # generate_dataset() writes under <output_base>/<dataset_id>; point
    # output_base one level up so files land at <output_base>/<TIER>/<dataset_id>.
    ns.output_base = str(pathlib.Path(output_base) / tier)
    dataset_root, summary = base.generate_dataset(ns, dataset, SPEC_ROOT, logger)
    return dataset_root, summary


def generate_named_spec_dataset(
    spec_path: pathlib.Path,
    name: str,
    output_base: str,
    ns: types.SimpleNamespace,
    logger: logging.Logger,
) -> tuple[pathlib.Path, dict]:
    """Materialize a single-spec YAML (SPARSE, or any CloudCpCliTesting/spec_files/*.yaml)
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

    bryck_config_tiers: List[str] = []
    if os.path.isfile(args.bryck_config_json):
        try:
            with open(args.bryck_config_json, "r", encoding="utf-8") as handle:
                bconf = json.load(handle)
            bryck_config_tiers = list(bconf.get("TIERS", bconf.get("tiers", {})).keys())
        except (OSError, json.JSONDecodeError) as exc:
            problems.append(f"could not parse {args.bryck_config_json} (read-only, non-fatal): {exc}")
    else:
        problems.append(f"{args.bryck_config_json} not found on this host (non-fatal outside the Bryck host)")

    redact = build_redactor(login_cfg or {}, cloud_ops_cfg or {})
    state, info_cmd = get_bryck_state(args, logger, redact)

    tiers = [t for t in ALL_TIERS if t in args.tiers]
    dataset_specs: Dict[str, dict] = {}
    for tier in tiers:
        if tier == "SPARSE":
            dataset_specs[tier] = {"dataset_id": None, "spec": str(SPARSE_SPEC_FILE)}
            continue
        try:
            dataset = resolve_tier_dataset(tier)
            dataset_specs[tier] = {
                "dataset_id": dataset.dataset_id,
                "name": dataset.name,
                "expected_files": dataset.expected_files,
                "spec_dir": str(SPEC_ROOT / dataset.dataset_id),
            }
        except SystemExit as exc:
            problems.append(str(exc))

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
    else:
        for tier in tiers:
            for mode in args.modes:
                test_cases.append({
                    "id": f"CLI-{MODE_CODE[mode]}-{tier}",
                    "kind": "transfer",
                    "tier": tier,
                    "mode": mode,
                    "dataset": dataset_specs.get(tier, {}).get("dataset_id") or "06_sparse_files.yaml",
                    "description": f"{mode} transfer for {tier} tier",
                })
    if args.include_lifecycle:
        for tier in tiers:
            test_cases.append({
                "id": f"CLI-LC-{tier}",
                "kind": "lifecycle",
                "tier": tier,
                "mode": "both",
                "dataset": dataset_specs.get(tier, {}).get("dataset_id") or "06_sparse_files.yaml",
                "description": f"Live intervention matrix on {tier} ({', '.join(LIFECYCLE_ACTIONS)})",
            })
    if args.include_service:
        for target in ("bcloud", "bryckapi"):
            test_cases.append({
                "id": f"CLI-SVC-{target.upper()}",
                "kind": "service",
                "tier": "MEDIUM",
                "mode": "both",
                "dataset": dataset_specs.get("MEDIUM", {}).get("dataset_id", "DS-P1-04"),
                "target_service": f"{target}.service",
                "description": f"Restart {target}.service mid-transfer on MEDIUM",
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
        },
        "dataset_catalog": args.dataset_catalog,
        "tiers": tiers,
        "datasets": dataset_specs,
        "bryck_config_tiers_seen": bryck_config_tiers,
        "test_cases": test_cases,
    }
    return plan


def render_confirmation(plan: dict) -> str:
    transfer_datasets = sorted({tc["dataset"] for tc in plan["test_cases"] if tc["kind"] == "transfer"})
    catalog = plan.get("dataset_catalog")
    if catalog == "all":
        dataset_line = f"ALL {len(transfer_datasets)} datasets from dataset_cloudcp/spec_files/manifest.json"
        generate_line = f"Generate datasets ({len(transfer_datasets)} datasets, full catalog round)"
    elif catalog == "specfiles":
        dataset_line = f"{len(transfer_datasets)} spec_files/*.yaml datasets: {', '.join(transfer_datasets)}"
        generate_line = f"Generate datasets ({len(transfer_datasets)} local spec_files/ datasets)"
    else:
        dataset_line = ", ".join(plan["tiers"]) + "   (all sizes — automatic)"
        generate_line = f"Generate datasets ({', '.join(plan['tiers'])})"
    modes_seen = sorted({tc["mode"] for tc in plan["test_cases"] if tc["kind"] == "transfer"})
    lines = [
        "CloudCP CLI Test Plan",
        "=====================",
        f"Run ID        : {plan['run_id']}",
        f"Bryck state   : {plan['bryck_state_before']}",
        f"Dataset(s)    : {dataset_line}",
        f"Transfer Mode : {' + '.join(modes_seen)}   (all modes — automatic)",
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
    if plan["pre_flight_problems"]:
        lines.append("")
        lines.append("PRE-FLIGHT WARNINGS:")
        for problem in plan["pre_flight_problems"]:
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

        login_ok, login_cfg, _ = validate_json_file(self.cfg["login"])
        params_ok, cloud_ops_cfg, _ = validate_json_file(self.cfg["params"])
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

    def initiate_transfer(self, result: TestCaseResult, mode: str) -> Optional[str]:
        cmd = self.run_py(
            result, "bryck_cloud_transfer_initiate.py",
            ["--login", self.cfg["login"], "--params", self.cfg["params"], "--mode", mode],
            timeout=self.cfg["wait_timeout"],
        )
        if self.args.dry_run:
            return "DRYRUN-ID"
        if not self.require_ok(result, cmd, "bryck_cloud_transfer_initiate.py"):
            return None
        transfer_id = parse_transfer_id(cmd.stdout + cmd.stderr)
        if not transfer_id:
            result.notes.append("initiate succeeded (rc=0) but no transfer_id could be parsed from its output")
        return transfer_id

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

    # -- test case kinds -------------------------------------------------

    def run_transfer_case(self, tc: dict) -> TestCaseResult:
        result = TestCaseResult(tc["id"], "transfer", tc["description"])
        tier, mode = tc["tier"], tc["mode"]
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

            transfer_id = self.initiate_transfer(result, mode)
            if not transfer_id:
                result.status = "BLOCKED"
                result.notes.append("could not determine transfer_id from initiate output")
                return result
            result.notes.append(f"transfer_id={transfer_id}")
            final_state = self.poll_until_terminal(result, transfer_id)
            result.notes.append(f"final_state={final_state}")

            report_dir = self.case_dir(tc["id"]) / "cloud_transfer_logs"
            report_dir.mkdir(parents=True, exist_ok=True)
            self.run_py(
                result, "bryck_cloud_transfer_report.py",
                ["--login", self.cfg["login"], "--cloud-transfer-id", str(transfer_id),
                 "--report-path", str(report_dir / f"cloud_transfer_report_{transfer_id}.zip")],
            )

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
        try:
            if not self.ensure_mounted(result):
                result.status = "BLOCKED"
                result.notes.append("Bryck could not be mounted; lifecycle test not attempted")
                return result
            generate_tier_dataset(tier, self.cfg["output_base"], self._ns(), self.logger, dataset_id=tc.get("dataset"))
            configured_ok, _bucket = self.configure_cloud(result, tier)
            if not configured_ok:
                result.status = "BLOCKED"
                result.notes.append("cloud configuration did not succeed; lifecycle test not attempted")
                return result

            transfer_id = self.initiate_transfer(result, "both")
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
                    new_id = self.initiate_transfer(result, "both")
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
            result.status = "FAIL" if any_failed else "PASS"
            self.cleanup_tier(result, tier)
        except Exception as exc:  # noqa: BLE001
            result.status = "FAIL"
            result.notes.append(f"exception: {exc}")
        return result

    def run_service_case(self, tc: dict) -> TestCaseResult:
        result = TestCaseResult(tc["id"], "service", tc["description"])
        tier = tc["tier"]
        try:
            if not self.ensure_mounted(result):
                result.status = "BLOCKED"
                result.notes.append("Bryck could not be mounted; service-restart test not attempted")
                return result
            generate_tier_dataset(tier, self.cfg["output_base"], self._ns(), self.logger, dataset_id=tc.get("dataset"))
            configured_ok, _bucket = self.configure_cloud(result, tier)
            if not configured_ok:
                result.status = "BLOCKED"
                result.notes.append("cloud configuration did not succeed; service-restart test not attempted")
                return result

            transfer_id = self.initiate_transfer(result, "both")
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

            transfer_id = self.initiate_transfer(result, "upload")
            if not transfer_id:
                result.status = "BLOCKED"
                result.notes.append("could not determine transfer_id")
                return result
            final_state = self.poll_until_terminal(result, transfer_id)
            result.notes.append(f"transfer_id={transfer_id} final_state={final_state}")
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

    def run_cli_input_case(self, tc: dict) -> TestCaseResult:
        result = TestCaseResult(tc["id"], "cli_input", tc["description"])
        case_id = tc["id"]
        case_dir = self.case_dir(case_id)
        try:
            expect_fail = True
            if case_id == "CLI-01":
                cmd = self.run_py(result, "bryck_cloud_transfer_initiate.py",
                                  ["--login", self.cfg["login"], "--params", self.cfg["params"]])
            elif case_id == "CLI-02":
                cmd = self.run_py(result, "bryck_cloud_transfer_initiate.py",
                                  ["--login", self.cfg["login"], "--params", self.cfg["params"], "--mode", "copy"])
            elif case_id == "CLI-03":
                fixture = case_dir / "cli03_cloud_ops.json"
                self._write_fixture_json(fixture, {**self.base_cloud_ops, "bryck_src": ""})
                cmd = self.run_py(result, "bryck_cloud_transfer_initiate.py",
                                  ["--login", self.cfg["login"], "--params", str(fixture), "--mode", "upload"])
            elif case_id == "CLI-04":
                fixture = case_dir / "cli04_cloud_ops.json"
                self._write_fixture_json(fixture, {**self.base_cloud_ops, "cloud_bucket": ""})
                cmd = self.run_py(result, "bryck_cloud_transfer_initiate.py",
                                  ["--login", self.cfg["login"], "--params", str(fixture), "--mode", "upload"])
            elif case_id == "CLI-05":
                fixture = case_dir / "cli05_cloud_ops.json"
                self._write_fixture_json(fixture, {**self.base_cloud_ops, "bryck_dst": ""})
                cmd = self.run_py(result, "bryck_cloud_transfer_initiate.py",
                                  ["--login", self.cfg["login"], "--params", str(fixture), "--mode", "download"])
            elif case_id == "CLI-06":
                cmd = self.run_py(result, "bryck_cloud_show.py", ["--login", str(case_dir / "missing-login.json")])
            elif case_id == "CLI-07":
                fixture = case_dir / "cli07_login.json"
                if not self.args.dry_run:
                    fixture.write_text("{", encoding="utf-8")
                cmd = self.run_py(result, "bryck_cloud_show.py", ["--login", str(fixture)])
            elif case_id == "CLI-08":
                cmd = self.run_py(result, "bryck_cloud_transfer_pause.py",
                                  ["--login", self.cfg["login"], "--transfer-id", "not-a-transfer-id"])
            elif case_id == "CLI-09":
                cmd = run_argv(
                    "datagen with missing spec",
                    [self.cfg["datagen_bin"], "--spec", str(case_dir / "missing-spec.yaml")],
                    self.logger, self.args.dry_run, self.redact,
                )
                result.commands.append(cmd.as_dict())
            else:
                result.status = "BLOCKED"
                result.notes.append("no fixture implemented for this CLI-input case yet")
                return result

            result.status = self._judge_expect_fail(result, cmd, expect_fail, case_id)
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
            "AWS-01": {"access_key_id": ""},
            "AWS-02": {"secret_access_key": ""},
            "AWS-03": {"access_key_id": "invalid-access-key"},
            "AWS-04": {"secret_access_key": "invalid-secret-key"},
            "AWS-05": {"region": "invalid-region"},
            "AWS-06": {"cloud_bucket": "not-a-valid-bucket"},
        }
        try:
            if case_id in mutation_map:
                fixture = case_dir / f"{case_id}_cloud_ops.json"
                self._write_fixture_json(fixture, {**self.base_cloud_ops, **mutation_map[case_id]})
                cmd = self.run_py(result, "bryck_cloud_configure.py", ["--login", self.cfg["login"], "--params", str(fixture)])
                result.status = self._judge_expect_fail(result, cmd, True, case_id)
            elif case_id == "AWS-07":
                cmd = self.run_py(result, "bryck_cloud_deconfigure.py", ["--login", self.cfg["login"], "--cloud-type", cloud_type])
                result.notes.append(f"observational: deconfigure-when-not-configured returned rc={cmd.returncode}")
                result.status = "PASS"
            elif case_id == "AWS-08":
                cmd1 = self.run_py(result, "bryck_cloud_deconfigure.py", ["--login", self.cfg["login"], "--cloud-type", cloud_type])
                cmd2 = self.run_py(result, "bryck_cloud_deconfigure.py", ["--login", self.cfg["login"], "--cloud-type", cloud_type])
                result.notes.append(f"observational: first rc={cmd1.returncode}, second rc={cmd2.returncode} (should be deterministic)")
                result.status = "PASS"
            else:
                result.status = "BLOCKED"
                result.notes.append("no fixture implemented for this AWS-negative case yet")
        except Exception as exc:  # noqa: BLE001
            result.status = "FAIL"
            result.notes.append(f"exception: {exc}")
        return result

    def run_all(self) -> List[TestCaseResult]:
        results: List[TestCaseResult] = []
        dispatch = {
            "transfer": self.run_transfer_case,
            "lifecycle": self.run_lifecycle_case,
            "service": self.run_service_case,
            "edge": self.run_edge_case,
            "cli_input": self.run_cli_input_case,
            "cloud_negative": self.run_cloud_negative_case,
        }
        for tc in self.plan["test_cases"]:
            self.logger.info("=== running %s (%s) ===", tc["id"], tc["kind"])
            result = dispatch[tc["kind"]](tc)
            self.write_case_result(result)
            results.append(result)
        self.restore_cloud_ops()
        return results


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
        return {"PASS": "pass", "FAIL": "fail", "BLOCKED": "blocked"}.get(status, "")

    html_rows = "".join(
        f"<tr class='{status_class(r.status)}'>"
        f"<td><span class='badge {status_class(r.status)}'>{r.status}</span></td>"
        f"<td>{r.test_id}</td><td>{r.kind}</td>"
        f"<td>{tc_by_id.get(r.test_id, {}).get('dataset', '')}</td>"
        f"<td>{tc_by_id.get(r.test_id, {}).get('mode', '')}</td>"
        f"<td>{r.description}</td>"
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
.badge {{ padding: 2px 8px; border-radius: 999px; font-weight: 700; font-size: 12px; }}
.badge.pass {{ background: #dcfce7; color: #14532d; }}
.badge.fail {{ background: #fee2e2; color: #7f1d1d; }}
.badge.blocked {{ background: #fef3c7; color: #78350f; }}
</style></head>
<body>
<h1>CloudCP CLI Run {plan['run_id']}</h1>
<div class="summary">
  <div><div class="value">{len(results)}</div>Total</div>
  <div><div class="value" style="color:#14532d">{counts.get('PASS', 0)}</div>PASS</div>
  <div><div class="value" style="color:#7f1d1d">{counts.get('FAIL', 0)}</div>FAIL</div>
  <div><div class="value" style="color:#78350f">{counts.get('BLOCKED', 0)}</div>BLOCKED</div>
</div>
<table>
<tr><th>Status</th><th>Test ID</th><th>Kind</th><th>Dataset</th><th>Mode</th><th>Description</th><th>Notes (first 3)</th></tr>
{html_rows}
</table>
</body></html>"""
    (run_dir / "summary.html").write_text(html, encoding="utf-8")


def phase_execute(args: argparse.Namespace, logger: logging.Logger) -> int:
    if not args.plan_file:
        raise SystemExit("--plan-file is required with --execute")
    with open(args.plan_file, "r", encoding="utf-8") as handle:
        plan = json.load(handle)
    if not plan.get("confirmed"):
        raise SystemExit(f"plan {args.plan_file} was not confirmed (run --plan and answer 'yes' first)")

    executor = Executor(args, plan, logger)
    results = executor.run_all()
    write_summary(executor.run_dir, plan, results)
    counts: Dict[str, int] = {}
    for r in results:
        counts[r.status] = counts.get(r.status, 0) + 1
    html_report = executor.run_dir / "summary.html"
    logger.info("Run complete. Results: %s", executor.run_dir)
    logger.info("PASS=%s FAIL=%s BLOCKED=%s (total %s)", counts.get("PASS", 0), counts.get("FAIL", 0),
                counts.get("BLOCKED", 0), len(results))
    print("")
    print(f"HTML report: {html_report.resolve().as_uri()}")
    print(f"             ({html_report.resolve()})")
    failed = sum(1 for r in results if r.status not in ("PASS",))
    return 1 if failed else 0


# =============================================================================
# Entry point
# =============================================================================

def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    logger = setup_logging(args.verbose)
    if args.plan:
        return phase_plan(args, logger)
    return phase_execute(args, logger)


if __name__ == "__main__":
    sys.exit(main())
