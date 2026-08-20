#!/usr/bin/env python3
"""
Cloud Transfer Unified Runner
=============================

One executable entry point for the complete CloudCP QE flow.

This runner combines the existing project pieces without duplicating their
operation logic:

1. NEGATIVE catalog
   - Uses negative_environment_runner.dispatch()/EnvironmentManager.
   - Uses the existing NEGATIVE_TEST_PLAN.md catalog.
   - Includes CLI/AUTH/TID/AWS/PATH/LIFE/DATA/XFER/DOWNLOAD/STATE/RACE/DUP/
     REPORT/FAULT/REC/VERIFY/INT/CLEAN/MGMT/SVC/SM/F cases.
   - Includes the P0 MASTER upload/download/both flows.

2. POSITIVE Cloud Transfer matrix
   - Uses cloud_transfer_only.py's proven dataset generation, direct
     `bryckcloud transfer add aws` initiation, polling, cleanup and
     performance capture.
   - Uses the authoritative dataset_cloudcp/spec_files/manifest.json catalog.
   - Default: every DS-P* dataset x upload/download/both.

3. Environment
   - Uses EnvironmentManager from negative_environment_runner for the
     inspect/prepare/validate/recover model.
   - A single run has one confirmation gate for destructive execution.

4. Reporting
   - Per-test evidence is kept below results/<RUN_ID>/<TEST_ID>/.
   - Transfer legs use the cloud_transfer_only performance collector.
   - Final unified JSON, Markdown and HTML summaries are generated.

The runner is an orchestrator. Existing cloud-transfer operation scripts remain
the source of truth for actual device operations.

Typical usage
-------------
    # Read-only plan; no device mutation
    python3 cloud_transfer_unified_runner.py --plan

    # Everything: all negative cases + 54 datasets x upload/download/both
    python3 cloud_transfer_unified_runner.py --execute

    # Faster validation
    python3 cloud_transfer_unified_runner.py --execute --datasets DS-P1-01 DS-P1-02

    # Negative suite only
    python3 cloud_transfer_unified_runner.py --execute --negative-only

    # Positive transfer matrix only
    python3 cloud_transfer_unified_runner.py --execute --transfer-only

    # Skip destructive negative cases
    python3 cloud_transfer_unified_runner.py --execute \
        --skip-destructive-negative

Important:
    --execute is intentionally destructive-capable. It requires typing YES.
    Use only on a dedicated test Bryck and isolated cloud bucket/prefix.
"""

from __future__ import annotations

import argparse
import datetime as dt
import html
import json
import os
import re
import shutil
import sys
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any


HERE = Path(__file__).resolve().parent
RESULTS_ROOT = HERE / "results"

# Existing project modules. They remain the operation implementations.
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE / "bryckclient-cli"))

import cloud_transfer_test_runner as ctr  # noqa: E402
import negative_environment_runner as ner  # noqa: E402
import cloud_transfer_negative_test_runner as neg_master  # noqa: E402
import cloud_transfer_only as transfer_only  # noqa: E402


TERMINAL_SUCCESS = "COMPLETED"

NEGATIVE_SECTIONS = [
    "CLI", "AUTH", "TID", "AWS", "PATH", "LIFE", "DATA",
    "XFER", "DOWNLOAD", "STATE", "RACE", "DUP", "REPORT",
    "FAULT", "REC", "VERIFY", "INT", "CLEAN", "MGMT", "SVC",
    "SM", "F",
]

DEFAULT_BRYCK_CONFIG = "/etc/bryck/bryckcloud/config.json"


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------

def now_id(prefix: str = "cloud_transfer") -> str:
    return f"{prefix}_{dt.datetime.now():%Y%m%d_%H%M%S}"


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(data, indent=2, default=str),
        encoding="utf-8",
    )


def redact_text(text: str) -> str:
    if not text:
        return ""
    patterns = [
        (r"(?i)(secret[_ -]?access[_ -]?key\s*[:=]\s*)\S+", r"\1<REDACTED>"),
        (r"(?i)(access[_ -]?key(?:_id)?\s*[:=]\s*)\S+", r"\1<REDACTED>"),
        (r"(?i)(bryckapi_password\s*[:=]\s*)\S+", r"\1<REDACTED>"),
        (r"(?i)(password\s*[:=]\s*)\S+", r"\1<REDACTED>"),
        (r"(?i)(token\s*[:=]\s*)\S+", r"\1<REDACTED>"),
    ]
    for pattern, replacement in patterns:
        text = re.sub(pattern, replacement, text)
    return text


def safe_test_dir(run_dir: Path, test_id: str) -> Path:
    clean = re.sub(r"[^A-Za-z0-9_.-]+", "_", test_id)
    path = run_dir / clean
    path.mkdir(parents=True, exist_ok=True)
    return path


# ---------------------------------------------------------------------------
# Arguments
# ---------------------------------------------------------------------------

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Unified Cloud Transfer + Negative Test Runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    execution = p.add_argument_group("execution")
    mode = execution.add_mutually_exclusive_group()
    mode.add_argument("--plan", action="store_true",
                      help="build and validate the plan without executing device changes")
    mode.add_argument("--execute", action="store_true",
                      help="execute the confirmed plan against the Bryck device")
    execution.add_argument("--negative-only", action="store_true",
                           help="run the complete negative catalog/master flows only")
    execution.add_argument("--transfer-only", action="store_true",
                           help="run the positive dataset transfer matrix only")
    execution.add_argument("--skip-negative", action="store_true")
    execution.add_argument("--skip-transfers", action="store_true")
    execution.add_argument("--skip-destructive-negative", action="store_true",
                           help="do not approve destructive negative cases")
    execution.add_argument("--confirm-destructive", action="store_true",
                           help="allow destructive negative cases after the main YES gate")
    execution.add_argument("--yes", action="store_true",
                           help="skip the interactive 'Type YES to execute' prompt (still prints "
                                "the full plan/warning first; use for unattended/scripted runs)")
    execution.add_argument("--allow-ip-change", action="store_true")
    execution.add_argument("--allow-service-faults", action="store_true")
    execution.add_argument("--allow-network-faults", action="store_true")
    execution.add_argument("--allow-reboot", action="store_true")

    selection = p.add_argument_group("negative test selection")
    selection.add_argument("--test", default="",
                           help="negative test ID(s), comma separated")
    selection.add_argument("--section", default="",
                           help="negative section(s), comma separated")
    selection.add_argument("--range", dest="negative_range", default="",
                           help="negative catalog position range, e.g. 1-50")
    selection.add_argument("--override", action="append", default=[],
                           help="negative fixture override KEY=VALUE")

    datasets = p.add_argument_group("dataset selection")
    datasets.add_argument("--dataset-catalog", choices=["all", "specfiles"],
                          default="all",
                          help="all = authoritative DS-P* manifest; specfiles = local legacy specs")
    datasets.add_argument("--datasets", nargs="*", default=None,
                           help="explicit DS-P* IDs; overrides --dataset-catalog all")
    datasets.add_argument("--dataset", default=None,
                          help="single dataset shorthand/ID; compatibility alias")
    datasets.add_argument("--spec-file", default=None,
                          help="explicit YAML file or directory for the transfer matrix")
    datasets.add_argument("--repeat", type=int, default=1,
                          help="repeat the selected transfer matrix this many times")

    paths = p.add_argument_group("configuration / paths")
    paths.add_argument("--login", default=str(ctr.DEFAULT_LOGIN_JSON))
    paths.add_argument("--cloud-ops", default=str(ctr.DEFAULT_CLOUD_OPS_JSON))
    paths.add_argument("--format-mount-params", default=str(ctr.DEFAULT_FORMAT_MOUNT_PARAMS_JSON))
    paths.add_argument("--report-dir", default=str(ctr.DEFAULT_REPORT_DIR))
    paths.add_argument("--spec-dir", default=str(ctr.SPEC_DIR))
    paths.add_argument("--results-dir", default=str(RESULTS_ROOT / "unified"))
    paths.add_argument("--ssh-user", default=None)
    paths.add_argument("--ssh-host", default=None)
    paths.add_argument("--datagen-bin", default=ctr.DATAGEN_BIN)
    paths.add_argument("--bryck-config-json", default=DEFAULT_BRYCK_CONFIG)

    transfer = p.add_argument_group("transfer-only / performance")
    transfer.add_argument("--output-base", default="/bryck")
    transfer.add_argument("--download-base", default="/bryck/cloudcp_cli_dl")
    transfer.add_argument("--bucket", default="s3://shravani/cloudcp-cli")
    transfer.add_argument("--bryckcloud-bin",
                           default="/opt/bryck/.venv/bryck/bin/bryckcloud")
    transfer.add_argument("--batchmeta-dir",
                           default="/opt/bryck/bryckapi/downloads/bcloud_batchmeta")
    transfer.add_argument("--transfer-logs-dir",
                           default="/opt/bryck/bryckapi/downloads/cloud_transfer_logs")
    transfer.add_argument("--cloudcp-log",
                           default="/opt/bryck/bryckapi/downloads/cloud_transfer_logs/cloudcp.log")
    transfer.add_argument("--journal-tag", nargs="+",
                           default=["bcloud", "bryckcloud"])
    transfer.add_argument("--poll-interval", type=int, default=10)
    transfer.add_argument("--wait-timeout", type=int, default=7200)
    transfer.add_argument("--action-timeout", type=int, default=120)
    transfer.add_argument("--log-wait-timeout", type=float, default=30)
    transfer.add_argument("--log-wait-interval", type=float, default=3)
    transfer.add_argument("--capture-lead", type=float, default=3)
    transfer.add_argument("--capture-drain", type=float, default=6)
    transfer.add_argument("--perf", dest="perf_capture", action="store_true", default=True)
    transfer.add_argument("--no-perf", dest="perf_capture", action="store_false")
    transfer.add_argument("--aws-cli", default="aws")
    transfer.add_argument("--aws-endpoint-url",
                          default="https://10.10.10.103:9000")
    transfer.add_argument("--aws-no-verify-ssl",
                          dest="aws_verify_ssl", action="store_false", default=False)
    transfer.add_argument("--cleanup", action="store_true",
                          help="cleanup generated dataset/cloud objects after each transfer case")
    transfer.add_argument("--keep", action="store_true",
                          help="keep generated dataset/cloud objects for debugging")
    transfer.add_argument("--skip-datagen", action="store_true")
    transfer.add_argument("--skip-mount-check", action="store_true")
    transfer.add_argument("--python-bin",
                          default=getattr(transfer_only.ccr, "DEFAULT_PYTHON_BIN", "python3"))
    transfer.add_argument("--run-id", default=None)
    transfer.add_argument("--verbose", action="store_true")

    args = p.parse_args(argv)

    if not any([args.plan, args.execute]):
        args.plan = True

    if args.negative_only and args.transfer_only:
        p.error("--negative-only and --transfer-only cannot be used together")

    if args.negative_only:
        args.skip_transfers = True
    if args.transfer_only:
        args.skip_negative = True

    if args.repeat < 1:
        p.error("--repeat must be >= 1")

    return args


# ---------------------------------------------------------------------------
# Negative runner adapter
# ---------------------------------------------------------------------------

def build_negative_args(args: argparse.Namespace, live: bool) -> argparse.Namespace:
    """
    Build the argument namespace expected by the existing negative runner.
    This keeps its handlers unchanged.
    """
    return SimpleNamespace(
        dry_run=not live,
        live=live,
        confirm_destructive=(
            args.confirm_destructive and not args.skip_destructive_negative
        ),
        allow_ip_change=args.allow_ip_change,
        allow_service_faults=args.allow_service_faults,
        allow_network_faults=args.allow_network_faults,
        allow_reboot=args.allow_reboot,
        sections=args.section,
        test_id=args.test,
        range=args.negative_range,
        override=args.override,
        login=args.login,
        cloud_ops=args.cloud_ops,
        format_mount_params=args.format_mount_params,
        report_dir=args.report_dir,
        results_dir=args.results_dir,
        datagen_bin=args.datagen_bin,
        spec_dir=args.spec_dir,
        ssh_user=args.ssh_user,
        ssh_host=args.ssh_host,
        upload_to_server=False,
        remote_report_dir="/opt/bryck/bryckapi/downloads/reports",
        # Fields used by the master runner/registry but not required by
        # EnvironmentManager.dispatch().
        upload=False,
        download=False,
        both=False,
        static=False,
        concurrency=False,
        recovery=False,
        all=True,
        test="",
        tests="",
        range_from="",
        range_to="",
        module="",
        modules="",
        list=False,
        search="",
        scenario_ids=[],
    )


def build_negative_context(args: argparse.Namespace, live: bool):
    n_args = build_negative_args(args, live)
    ctx = ner.build_context(n_args)
    mgr = ner.EnvironmentManager(ctx)
    return n_args, ctx, mgr


def negative_catalog_entries(args: argparse.Namespace) -> list[tuple[str, str, str]]:
    entries = ctr._negative_plan_entries(ner.PLAN_PATH)

    if args.section:
        wanted = {x.strip().upper() for x in args.section.split(",") if x.strip()}
        entries = [
            e for e in entries
            if re.match(r"[A-Z]+", e[0]).group(0) in wanted
        ]

    if args.test:
        wanted = {x.strip() for x in args.test.split(",") if x.strip()}
        entries = [e for e in entries if e[0] in wanted]

    if args.negative_range:
        m = re.fullmatch(r"\s*(\d+)\s*-\s*(\d+)\s*", args.negative_range)
        if not m:
            raise ValueError("--range must be START-END")
        start, end = int(m.group(1)), int(m.group(2))
        entries = entries[start - 1:end]

    return entries


def run_negative_suite(
    args: argparse.Namespace,
    run_dir: Path,
    live: bool,
) -> list[Any]:
    n_args, ctx, mgr = build_negative_context(args, live)
    results: list[Any] = []
    work = run_dir / "_negative_work"
    work.mkdir(parents=True, exist_ok=True)

    entries = negative_catalog_entries(args)

    print(f"\n[NEGATIVE] Selected catalog cases: {len(entries)}")

    for case_id, _heading, desc in entries:
        mgr.commands = []
        try:
            result = ner.dispatch(case_id, desc, mgr, n_args, work, {})
            result.narrative = ner.build_narrative(result)
        except Exception as exc:
            result = ner.TestResult(
                test_id=case_id,
                section=(re.match(r"[A-Z]+", case_id) or ["UNKNOWN"])[0],
                name=desc,
                status="FAIL",
                expected="Negative test executes without runner exception.",
                actual=f"{type(exc).__name__}: {exc}",
                reason="Runner exception; not a product verdict.",
                baseline={},
                env_before=None,
                env_after=None,
                commands=list(mgr.commands),
            )
        results.append(result)
        print(f"    [{case_id}] {desc} -> {result.status}")

    # P0 master flows are kept as separate named test cases.
    # They use the same EnvironmentManager and the same negative-test logic.
    master_requested = not args.test and not args.section and not args.negative_range

    if master_requested:
        if live:
            print("\n[NEGATIVE] P0 MASTER-UPLOAD")
            results.extend(
                neg_master.run_master_flow("upload", mgr, n_args, run_dir / "MASTER-UPLOAD")
            )
            print("\n[NEGATIVE] P0 MASTER-DOWNLOAD")
            results.extend(
                neg_master.run_master_flow("download", mgr, n_args, run_dir / "MASTER-DOWNLOAD")
            )
            print("\n[NEGATIVE] P0 MASTER-BOTH")
            results.extend(
                neg_master.run_master_flow_both(mgr, n_args, run_dir / "MASTER-BOTH")
            )
        else:
            for case_id, desc in [
                ("MASTER-UPLOAD", "P0 end-to-end upload master flow"),
                ("MASTER-DOWNLOAD", "P0 end-to-end download master flow"),
                ("MASTER-BOTH", "P0 end-to-end upload + download master flow"),
            ]:
                results.append(
                    ner.blocked(
                        case_id,
                        "MASTER",
                        desc,
                        {"flow": "P0"},
                        "live execution required",
                        mgr=mgr,
                    )
                )

    return results


# ---------------------------------------------------------------------------
# Dataset manager
# ---------------------------------------------------------------------------

def discover_datasets(args: argparse.Namespace) -> list[str]:
    """
    Resolve the authoritative dataset catalog.

    Latest plan behavior is authoritative:
      dataset_cloudcp/spec_files/manifest.json -> DS-P1-01..DS-P12-02.
    The directory scan is only a defensive fallback when the manifest is
    unavailable.
    """
    if args.datasets:
        return list(dict.fromkeys(args.datasets))

    if args.dataset:
        return [args.dataset]

    if args.spec_file:
        return [Path(args.spec_file).stem]

    spec_dir = Path(args.spec_dir)

    # Manifest can be next to the spec directory or under its parent.
    candidates = [
        spec_dir / "manifest.json",
        spec_dir.parent / "manifest.json",
        HERE / "dataset_cloudcp" / "spec_files" / "manifest.json",
    ]

    manifest = next((p for p in candidates if p.is_file()), None)

    if manifest:
        try:
            data = json.loads(manifest.read_text(encoding="utf-8"))

            ids: list[str] = []

            def walk(obj: Any) -> None:
                if isinstance(obj, dict):
                    for key, value in obj.items():
                        if isinstance(key, str) and re.fullmatch(r"DS-P\d+-\d+", key):
                            ids.append(key)
                        walk(value)
                elif isinstance(obj, list):
                    for value in obj:
                        walk(value)
                elif isinstance(obj, str) and re.fullmatch(r"DS-P\d+-\d+", obj):
                    ids.append(obj)

            walk(data)

            # Preserve manifest order and remove duplicates.
            ids = list(dict.fromkeys(ids))
            if ids:
                return ids
        except Exception as exc:
            print(f"WARNING: manifest parse failed: {exc}")

    # Last-resort filesystem fallback.
    roots = [
        spec_dir,
        HERE / "dataset_cloudcp" / "spec_files",
    ]

    found: set[str] = set()
    for root in roots:
        if root.is_dir():
            for child in root.iterdir():
                if child.is_dir() and re.fullmatch(r"DS-P\d+-\d+", child.name):
                    found.add(child.name)

    ids = sorted(
        found,
        key=lambda x: tuple(int(v) for v in re.findall(r"\d+", x)),
    )

    if ids:
        return ids

    raise FileNotFoundError(
        "No dataset catalog found. Expected "
        "dataset_cloudcp/spec_files/manifest.json or DS-P* directories."
    )


def validate_dataset_catalog(args: argparse.Namespace, datasets: list[str]) -> list[str]:
    """
    Validate that each selected dataset can be resolved before execution.
    """
    if args.spec_file:
        p = Path(args.spec_file)
        if not p.exists():
            return [f"spec-file missing: {p}"]
        return []

    missing: list[str] = []
    spec_dir = Path(args.spec_dir)

    for dataset_id in datasets:
        candidates = [
            spec_dir / dataset_id,
            HERE / "dataset_cloudcp" / "spec_files" / dataset_id,
        ]
        if not any(p.exists() for p in candidates):
            # Some older cloud_cli_runner implementations resolve datasets
            # through their own manifest helper, so don't hard-fail here if
            # the runner itself can resolve the ID.
            if not hasattr(transfer_only.ccr, "generate_tier_dataset"):
                missing.append(dataset_id)

    return missing


# ---------------------------------------------------------------------------
# Transfer matrix
# ---------------------------------------------------------------------------

def transfer_args_namespace(args: argparse.Namespace, run_id: str) -> argparse.Namespace:
    """
    Build the namespace expected by cloud_transfer_only.py functions.
    """
    return SimpleNamespace(
        login=args.login,
        params=args.cloud_ops,
        format_mount_params=args.format_mount_params,
        output_base=args.output_base,
        download_base=args.download_base,
        bucket=args.bucket,
        datagen_bin=args.datagen_bin,
        python_bin=args.python_bin,
        bryckcloud_bin=args.bryckcloud_bin,
        batchmeta_dir=args.batchmeta_dir,
        transfer_logs_dir=args.transfer_logs_dir,
        dry_run=False,
        skip_datagen=args.skip_datagen,
        skip_mount_check=args.skip_mount_check,
        poll_interval=args.poll_interval,
        wait_timeout=args.wait_timeout,
        action_timeout=args.action_timeout,
        results_dir=args.results_dir,
        run_id=run_id,
        perf_capture=args.perf_capture,
        journal_tag=args.journal_tag,
        cloudcp_log=args.cloudcp_log,
        capture_lead=args.capture_lead,
        capture_drain=args.capture_drain,
        bryck_config_json=args.bryck_config_json,
        cleanup=args.cleanup,
        keep=args.keep,
        force_cleanup=False,
        log_wait_timeout=args.log_wait_timeout,
        log_wait_interval=args.log_wait_interval,
        aws_cli=args.aws_cli,
        aws_endpoint_url=args.aws_endpoint_url,
        aws_verify_ssl=args.aws_verify_ssl,
        background_cleanup=False,
        verbose=args.verbose,
    )


def validate_common_configs(args: argparse.Namespace, live: bool) -> tuple[bool, dict, dict]:
    login_ok, login_cfg, login_err = transfer_only.ccr.validate_json_file(args.login)
    cloud_ok, cloud_cfg, cloud_err = transfer_only.ccr.validate_json_file(args.cloud_ops)

    if not login_ok:
        print(f"CONFIG ERROR: login.json: {login_err}")
    if not cloud_ok:
        print(f"CONFIG ERROR: cloud_ops.json: {cloud_err}")

    if live:
        config_ok, _tiers, config_msg, config_snippet = (
            transfer_only.ccr.validate_bryck_config_json(args.bryck_config_json)
        )
        if not config_ok:
            print(f"CONFIG ERROR: {args.bryck_config_json}: {config_msg}")
            if config_snippet:
                print(config_snippet)
            return False, login_cfg or {}, cloud_cfg or {}

    return bool(login_ok and cloud_ok), login_cfg or {}, cloud_cfg or {}


def transfer_case(
    args: argparse.Namespace,
    run_dir: Path,
    dataset_id: str,
    mode: str,
    iteration: int,
    base_cloud_ops: dict,
    live: bool,
) -> list[dict]:
    """
    Run one dataset x mode transfer case.

    upload:
        generated dataset -> S3

    download:
        seed upload -> S3, then S3 -> local

    both:
        upload -> S3, then S3 -> local in one reported test group.
    """
    case_id = f"TRANSFER-{dataset_id}-{mode.upper()}-R{iteration:02d}"
    case_dir = safe_test_dir(run_dir, case_id)

    targs = transfer_args_namespace(args, case_id)
    targs.dry_run = not live

    # Make a private cloud_ops copy for this case. The transfer-only helper
    # expects the path in args.params, so restore the original after the case.
    case_cloud_ops = case_dir / "cloud_ops.json"
    write_json(case_cloud_ops, base_cloud_ops)
    targs.params = str(case_cloud_ops)

    redact = transfer_only.ccr.build_redactor(
        base_cloud_ops,
        base_cloud_ops,
    )

    results: list[dict] = []

    # Setup / mount.
    setup_tcr = transfer_only.ccr.TestCaseResult(
        test_id=f"{case_id}-SETUP",
        kind="setup",
        description=f"mount/configure for {dataset_id} {mode}",
    )

    if live:
        state = transfer_only.ensure_mounted(targs, redact, setup_tcr)
        if not args.skip_mount_check and state.strip().lower() != "mounted":
            setup_tcr.status = "BLOCKED"
            setup_tcr.notes.append(f"Bryck not mounted: {state!r}")
            results.append({
                "case_id": case_id,
                "dataset": dataset_id,
                "mode": mode,
                "status": "BLOCKED",
                "setup": setup_tcr,
                "legs": [],
            })
            return results
    else:
        state = "DRYRUN"

    # Generate the dataset using the same helper used by cloud_transfer_only.
    ns = SimpleNamespace(
        output_base=args.output_base,
        skip_generate=args.skip_datagen,
        datagen_bin=args.datagen_bin,
        dry_run=not live,
        verbose=args.verbose,
    )

    dataset_label = dataset_id
    tier = dataset_id

    try:
        spec_obj = Path(args.spec_file) if args.spec_file else None

        if spec_obj is not None and spec_obj.is_dir():
            dataset_root, gen_summary = transfer_only.generate_spec_dir_dataset(
                spec_obj, tier, args.output_base, case_dir, ns, transfer_only.LOG,
            )
        elif spec_obj is not None:
            dataset_root, gen_summary = transfer_only.generate_named_spec_dataset_local(
                spec_obj, tier, args.output_base, case_dir, ns, transfer_only.LOG,
            )
        else:
            dataset_root, gen_summary = transfer_only.ccr.generate_tier_dataset(
                tier, args.output_base, ns, transfer_only.LOG, dataset_id,
            )

        bucket = transfer_only.configure_cloud(
            targs, base_cloud_ops, tier, redact, setup_tcr,
        )

        setup_tcr.status = "PASS" if live or not live else "PASS"
        setup_tcr.notes.append(f"dataset_root={dataset_root}")
        setup_tcr.notes.append(f"dataset_summary={gen_summary}")
        setup_tcr.notes.append(f"cloud_bucket={bucket}")
    except Exception as exc:
        setup_tcr.status = "BLOCKED" if live else "PASS"
        setup_tcr.notes.append(f"setup error: {type(exc).__name__}: {exc}")
        results.append({
            "case_id": case_id,
            "dataset": dataset_id,
            "mode": mode,
            "status": setup_tcr.status,
            "setup": setup_tcr,
            "legs": [],
        })
        return results

    local_src = str(dataset_root)
    s3_path = f"{args.bucket}/{tier}"
    local_dst = f"{args.download_base}/{tier}"

    # Direct upload leg.
    if mode == "upload":
        leg = transfer_only.run_leg(
            targs, "upload", case_dir / "upload", case_id,
            local_src, s3_path, redact, tier, gen_summary, dataset_label,
        )
        results.append({
            "case_id": case_id,
            "dataset": dataset_id,
            "mode": mode,
            "status": leg["tcr"].status,
            "setup": setup_tcr,
            "legs": [leg],
        })

    # Download requires a completed seed upload.
    elif mode == "download":
        seed_tcr = transfer_only.ccr.TestCaseResult(
            test_id=f"{case_id}-SEED",
            kind="seed",
            description="untracked upload used to seed remote data for download",
        )
        seed_id = transfer_only.initiate_transfer_cli(
            targs, local_src, s3_path, redact, seed_tcr,
        )
        seed_state = transfer_only.poll_until_terminal(
            targs, seed_id or "unknown", None, redact, seed_tcr,
        ) if seed_id else "UNKNOWN"

        if seed_state != TERMINAL_SUCCESS and live:
            seed_tcr.status = "FAIL"
            results.append({
                "case_id": case_id,
                "dataset": dataset_id,
                "mode": mode,
                "status": "FAIL",
                "setup": setup_tcr,
                "legs": [],
                "seed": seed_tcr,
            })
            return results

        leg = transfer_only.run_leg(
            targs, "download", case_dir / "download", case_id,
            s3_path, local_dst, redact, tier, gen_summary, dataset_label,
        )
        results.append({
            "case_id": case_id,
            "dataset": dataset_id,
            "mode": mode,
            "status": leg["tcr"].status,
            "setup": setup_tcr,
            "legs": [leg],
            "seed": seed_tcr,
        })

    # Both = upload then download with independent performance capture.
    else:
        up = transfer_only.run_leg(
            targs, "upload", case_dir / "upload", case_id,
            local_src, s3_path, redact, tier, gen_summary, dataset_label,
        )

        down: dict | None = None
        if up["final_state"] == TERMINAL_SUCCESS or not live:
            down = transfer_only.run_leg(
                targs, "download", case_dir / "download", case_id,
                s3_path, local_dst, redact, tier, gen_summary, dataset_label,
            )

        legs = [up] + ([down] if down is not None else [])
        overall = "PASS" if all(
            x["error"] is None and x["final_state"] == TERMINAL_SUCCESS
            for x in legs
        ) else "FAIL"

        results.append({
            "case_id": case_id,
            "dataset": dataset_id,
            "mode": mode,
            "status": overall,
            "setup": setup_tcr,
            "legs": legs,
        })

    # Per-case cleanup. Keep is deliberately honored.
    if args.cleanup and not args.keep:
        try:
            # Use the transfer-only cleanup implementation against this case.
            transfer_only.cleanup(
                targs,
                tier,
                redact,
                setup_tcr,
                log_dir=case_dir,
            )
        except Exception as exc:
            setup_tcr.notes.append(f"cleanup error: {type(exc).__name__}: {exc}")
    else:
        setup_tcr.notes.append("cleanup skipped (use --cleanup to enable)")

    return results


def run_transfer_matrix(
    args: argparse.Namespace,
    run_dir: Path,
    datasets: list[str],
    live: bool,
) -> list[dict]:
    ok, _login_cfg, base_cloud_ops = validate_common_configs(args, live)
    if not ok:
        return [{
            "case_id": "TRANSFER-SETUP",
            "dataset": "",
            "mode": "",
            "status": "BLOCKED",
            "error": "configuration validation failed",
            "legs": [],
        }]

    all_results: list[dict] = []

    print(
        f"\n[TRANSFER MATRIX] datasets={len(datasets)} "
        f"modes=upload,download,both repeats={args.repeat}"
    )

    for iteration in range(1, args.repeat + 1):
        for dataset_id in datasets:
            for mode in ("upload", "download", "both"):
                print(
                    f"\n[TRANSFER] iteration={iteration} "
                    f"dataset={dataset_id} mode={mode}"
                )
                try:
                    case_results = transfer_case(
                        args, run_dir, dataset_id, mode,
                        iteration, base_cloud_ops, live,
                    )
                    all_results.extend(case_results)
                except KeyboardInterrupt:
                    raise
                except Exception as exc:
                    all_results.append({
                        "case_id": f"TRANSFER-{dataset_id}-{mode.upper()}-R{iteration:02d}",
                        "dataset": dataset_id,
                        "mode": mode,
                        "status": "FAIL",
                        "error": f"{type(exc).__name__}: {exc}",
                        "legs": [],
                    })
                    print(f"    ERROR: {type(exc).__name__}: {exc}")

    return all_results


# ---------------------------------------------------------------------------
# Unified report
# ---------------------------------------------------------------------------

def result_to_dict(result: Any) -> dict:
    if hasattr(result, "__dataclass_fields__"):
        return {
            k: result_to_dict(v)
            for k, v in vars(result).items()
        }
    if isinstance(result, dict):
        return {str(k): result_to_dict(v) for k, v in result.items()}
    if isinstance(result, (list, tuple)):
        return [result_to_dict(v) for v in result]
    if isinstance(result, Path):
        return str(result)
    if hasattr(result, "__dict__"):
        return {
            k: result_to_dict(v)
            for k, v in vars(result).items()
            if not k.startswith("_")
        }
    return result


def summarize_status(statuses: list[str]) -> dict[str, int]:
    counts = {"PASS": 0, "FAIL": 0, "BLOCKED": 0, "SKIP": 0}
    for status in statuses:
        counts[status] = counts.get(status, 0) + 1
    return counts


def collect_transfer_statuses(transfer_results: list[dict]) -> list[str]:
    statuses: list[str] = []
    for item in transfer_results:
        statuses.append(item.get("status", "BLOCKED"))
    return statuses


def build_unified_html(summary: dict, output: Path) -> None:
    neg = summary["negative"]["counts"]
    tr = summary["transfers"]["counts"]

    rows: list[str] = []

    for item in summary["negative"]["cases"]:
        rows.append(
            "<tr>"
            f"<td>{html.escape(str(item.get('test_id', '')))}</td>"
            f"<td>{html.escape(str(item.get('section', '')))}</td>"
            f"<td>{html.escape(str(item.get('name', '')))}</td>"
            f"<td class='{item.get('status','').lower()}'>"
            f"{html.escape(str(item.get('status', '')))}</td>"
            f"<td>{html.escape(str(item.get('reason', '')))}</td>"
            "</tr>"
        )

    for item in summary["transfers"]["cases"]:
        rows.append(
            "<tr>"
            f"<td>{html.escape(str(item.get('case_id', '')))}</td>"
            "<td>TRANSFER</td>"
            f"<td>{html.escape(str(item.get('dataset', '')))} / "
            f"{html.escape(str(item.get('mode', '')))}</td>"
            f"<td class='{item.get('status','').lower()}'>"
            f"{html.escape(str(item.get('status', '')))}</td>"
            f"<td>{html.escape(str(item.get('error', '')))}</td>"
            "</tr>"
        )

    document = f"""<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>Cloud Transfer Unified Report - {html.escape(summary['run_id'])}</title>
<style>
body {{ font-family: Arial, sans-serif; margin: 24px; }}
h1 {{ margin-bottom: 4px; }}
.small {{ color: #666; }}
.cards {{ display:flex; gap:12px; flex-wrap:wrap; margin:18px 0; }}
.card {{ border:1px solid #ddd; border-radius:8px; padding:12px 18px; min-width:120px; }}
table {{ border-collapse:collapse; width:100%; margin-top:18px; }}
th,td {{ border:1px solid #ddd; padding:7px; text-align:left; vertical-align:top; }}
th {{ background:#f4f4f4; }}
.pass {{ color:#16803c; font-weight:bold; }}
.fail {{ color:#b42318; font-weight:bold; }}
.blocked {{ color:#8a6100; font-weight:bold; }}
.skip {{ color:#666; }}
code {{ white-space:pre-wrap; }}
</style>
</head>
<body>
<h1>Cloud Transfer Unified Report</h1>
<div class="small">Run ID: {html.escape(summary['run_id'])}</div>
<div class="small">Started: {html.escape(summary['started'])}</div>
<div class="small">Finished: {html.escape(summary['finished'])}</div>

<div class="cards">
<div class="card"><b>Negative PASS</b><br>{neg.get('PASS',0)}</div>
<div class="card"><b>Negative FAIL</b><br>{neg.get('FAIL',0)}</div>
<div class="card"><b>Negative BLOCKED</b><br>{neg.get('BLOCKED',0)}</div>
<div class="card"><b>Transfer PASS</b><br>{tr.get('PASS',0)}</div>
<div class="card"><b>Transfer FAIL</b><br>{tr.get('FAIL',0)}</div>
<div class="card"><b>Transfer BLOCKED</b><br>{tr.get('BLOCKED',0)}</div>
</div>

<h2>Execution Scope</h2>
<ul>
<li>Negative catalog + P0 master flows</li>
<li>Authoritative dataset catalog: {summary['dataset_count']} dataset(s)</li>
<li>Transfer modes: upload + download + both</li>
<li>Single Bryck execution target</li>
</ul>

<h2>All Cases</h2>
<table>
<thead>
<tr><th>ID</th><th>Section</th><th>Name</th><th>Status</th><th>Details</th></tr>
</thead>
<tbody>
{''.join(rows)}
</tbody>
</table>
</body>
</html>
"""
    output.write_text(document, encoding="utf-8")


def build_unified_reports(
    run_dir: Path,
    run_id: str,
    started: str,
    finished: str,
    negative_results: list[Any],
    transfer_results: list[dict],
    datasets: list[str],
) -> tuple[Path, Path, Path]:
    neg_dicts = [result_to_dict(x) for x in negative_results]
    tr_dicts = [result_to_dict(x) for x in transfer_results]

    neg_counts = summarize_status(
        [x.get("status", "BLOCKED") for x in neg_dicts]
    )
    tr_counts = summarize_status(
        [x.get("status", "BLOCKED") for x in tr_dicts]
    )

    summary = {
        "run_id": run_id,
        "started": started,
        "finished": finished,
        "dataset_count": len(datasets),
        "datasets": datasets,
        "modes": ["upload", "download", "both"],
        "negative": {
            "counts": neg_counts,
            "cases": neg_dicts,
        },
        "transfers": {
            "counts": tr_counts,
            "cases": tr_dicts,
        },
        "totals": {
            "negative_cases": len(neg_dicts),
            "transfer_cases": len(tr_dicts),
            "pass": neg_counts.get("PASS", 0) + tr_counts.get("PASS", 0),
            "fail": neg_counts.get("FAIL", 0) + tr_counts.get("FAIL", 0),
            "blocked": neg_counts.get("BLOCKED", 0) + tr_counts.get("BLOCKED", 0),
        },
    }

    json_path = run_dir / "summary.json"
    md_path = run_dir / "summary.md"
    html_path = run_dir / "summary.html"

    write_json(json_path, summary)

    md_lines = [
        f"# Cloud Transfer Unified Report",
        "",
        f"- **Run ID:** `{run_id}`",
        f"- **Started:** `{started}`",
        f"- **Finished:** `{finished}`",
        f"- **Datasets:** {len(datasets)}",
        f"- **Transfer modes:** upload, download, both",
        "",
        "## Negative Tests",
        "",
        f"- PASS: {neg_counts.get('PASS',0)}",
        f"- FAIL: {neg_counts.get('FAIL',0)}",
        f"- BLOCKED: {neg_counts.get('BLOCKED',0)}",
        "",
        "## Transfer Matrix",
        "",
        f"- PASS: {tr_counts.get('PASS',0)}",
        f"- FAIL: {tr_counts.get('FAIL',0)}",
        f"- BLOCKED: {tr_counts.get('BLOCKED',0)}",
        "",
        "## Datasets",
        "",
    ]
    md_lines.extend(f"- `{d}`" for d in datasets)
    md_lines += [
        "",
        "## Transfer Cases",
        "",
        "| Case | Dataset | Mode | Status |",
        "|---|---|---|---|",
    ]
    for item in tr_dicts:
        md_lines.append(
            f"| `{item.get('case_id','')}` | "
            f"`{item.get('dataset','')}` | "
            f"`{item.get('mode','')}` | "
            f"**{item.get('status','')}** |"
        )

    md_path.write_text("\n".join(md_lines) + "\n", encoding="utf-8")
    build_unified_html(summary, html_path)

    return json_path, md_path, html_path


# ---------------------------------------------------------------------------
# Plan / confirmation
# ---------------------------------------------------------------------------

def preflight(args: argparse.Namespace, datasets: list[str], live: bool) -> dict:
    errors: list[str] = []

    for path_str in [args.login, args.cloud_ops, args.format_mount_params]:
        path = Path(path_str)
        if not path.exists():
            errors.append(f"missing configuration: {path}")

    missing_datasets = validate_dataset_catalog(args, datasets)
    errors.extend(f"dataset not resolvable: {x}" for x in missing_datasets)

    if live:
        ok, _login, _cloud = validate_common_configs(args, live=True)
        if not ok:
            errors.append("configuration validation failed")

        # Read-only environment inspection. No format/eject/datagen here.
        try:
            n_args, ctx, mgr = build_negative_context(args, live=True)
            snap = mgr.snapshot("UNIFIED_PREFLIGHT")
        except Exception as exc:
            snap = {"error": f"{type(exc).__name__}: {exc}"}
            errors.append(f"environment inspection failed: {snap['error']}")
    else:
        snap = {"mode": "plan-only"}

    negative_count = len(negative_catalog_entries(args))
    master_count = 3 if not args.test and not args.section and not args.negative_range else 0
    transfer_case_count = len(datasets) * 3 * args.repeat

    return {
        "errors": errors,
        "datasets": datasets,
        "negative_cases": negative_count,
        "master_cases": master_count,
        "transfer_cases": transfer_case_count,
        "environment": snap,
    }


def print_plan(plan: dict, args: argparse.Namespace) -> None:
    print("\n" + "=" * 72)
    print("CLOUD TRANSFER UNIFIED PLAN")
    print("=" * 72)
    print(f"Datasets             : {len(plan['datasets'])}")
    print(f"Negative catalog     : {plan['negative_cases']}")
    print(f"P0 master flows      : {plan['master_cases']}")
    print(f"Transfer matrix      : {plan['transfer_cases']} cases")
    print(f"Transfer modes       : upload / download / both")
    print(f"Execution            : {'LIVE' if args.execute else 'PLAN ONLY'}")
    print(f"Environment          : {plan['environment']}")
    if plan["errors"]:
        print("\nPREFLIGHT ERRORS:")
        for error in plan["errors"]:
            print(f"  - {error}")
    else:
        print("\nPreflight: PASS")
    print("=" * 72)


def confirm_execution(plan: dict, args: argparse.Namespace) -> bool:
    print("\n" + "!" * 72)
    print("WARNING: LIVE EXECUTION WILL MODIFY THE DEDICATED BRYCK.")
    print("!" * 72)
    print(f"Negative catalog cases : {plan['negative_cases']}")
    print(f"P0 master flows        : {plan['master_cases']}")
    print(f"Transfer matrix cases  : {plan['transfer_cases']}")
    print("Transfer modes         : upload, download, both")
    print("")
    print("Potential destructive operations include:")
    print("  - format / erase / remove")
    print("  - eject / mount state changes")
    print("  - service restarts")
    print("  - transfer cancellation / interruption")
    print("  - cloud configuration changes")
    print("  - dataset and cloud-object cleanup")
    print("")
    if args.confirm_destructive and not args.skip_destructive_negative:
        print("Destructive negative cases are ENABLED.")
    else:
        print("Destructive negative cases are BLOCKED.")
    print("")
    if args.yes:
        print("--yes given: skipping interactive confirmation, proceeding with execution.")
        return True
    answer = input("Type YES to execute the complete plan: ").strip()
    return answer == "YES"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    run_id = args.run_id or now_id("cloud_transfer_unified")
    run_dir = Path(args.results_dir) / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    started = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")

    try:
        datasets = discover_datasets(args)
    except Exception as exc:
        print(f"DATASET CATALOG ERROR: {exc}")
        return 2

    plan = preflight(args, datasets, live=bool(args.execute))
    print_plan(plan, args)

    if plan["errors"]:
        print("\nRun not started because preflight failed.")
        return 3

    plan_payload = {
        "run_id": run_id,
        "started": started,
        "execute": bool(args.execute),
        "datasets": datasets,
        "negative_cases": plan["negative_cases"],
        "master_cases": plan["master_cases"],
        "transfer_cases": plan["transfer_cases"],
        "arguments": vars(args),
    }
    write_json(run_dir / "plan.json", plan_payload)

    if not args.execute:
        print(f"\nPlan saved: {run_dir / 'plan.json'}")
        print("No device changes were made.")
        return 0

    if not confirm_execution(plan, args):
        print("Execution cancelled. No live execution started.")
        return 0

    live = True
    negative_results: list[Any] = []
    transfer_results: list[dict] = []

    # Backup shared cloud_ops.json. Existing transfer-only behavior rewrites it
    # during configuration, so the original is restored at the end.
    cloud_ops_path = Path(args.cloud_ops)
    cloud_ops_backup = run_dir / "cloud_ops.json.bak"
    if cloud_ops_path.exists():
        shutil.copy2(cloud_ops_path, cloud_ops_backup)

    interrupted = False

    try:
        if not args.skip_negative:
            negative_results = run_negative_suite(args, run_dir, live=True)

        if not args.skip_transfers:
            transfer_results = run_transfer_matrix(
                args, run_dir, datasets, live=True
            )

    except KeyboardInterrupt:
        interrupted = True
        print("\nRUN INTERRUPTED: writing partial report...")
    except Exception as exc:
        interrupted = True
        print(f"\nRUN ERROR: {type(exc).__name__}: {exc}")
    finally:
        if cloud_ops_backup.exists():
            try:
                shutil.copy2(cloud_ops_backup, cloud_ops_path)
            except Exception as exc:
                print(f"WARNING: could not restore cloud_ops.json: {exc}")

    finished = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")

    json_path, md_path, html_path = build_unified_reports(
        run_dir,
        run_id,
        started,
        finished,
        negative_results,
        transfer_results,
        datasets,
    )

    # Add interruption marker after the main report is generated.
    try:
        report = json.loads(json_path.read_text(encoding="utf-8"))
        report["interrupted"] = interrupted
        write_json(json_path, report)
    except Exception:
        pass

    neg_statuses = [
        getattr(x, "status", "BLOCKED")
        for x in negative_results
    ]
    tr_statuses = [
        x.get("status", "BLOCKED")
        for x in transfer_results
    ]
    counts = summarize_status(neg_statuses + tr_statuses)

    print("\n" + "=" * 72)
    print("CLOUD TRANSFER UNIFIED EXECUTION COMPLETE")
    print("=" * 72)
    print(f"Run ID        : {run_id}")
    print(f"PASS          : {counts.get('PASS',0)}")
    print(f"FAIL          : {counts.get('FAIL',0)}")
    print(f"BLOCKED       : {counts.get('BLOCKED',0)}")
    print(f"INTERRUPTED   : {interrupted}")
    print("")
    print(f"JSON report   : {json_path}")
    print(f"Markdown      : {md_path}")
    print(f"HTML report   : {html_path}")
    print(f"Plan          : {run_dir / 'plan.json'}")
    print(f"Results dir   : {run_dir}")
    print("=" * 72)

    return 1 if counts.get("FAIL", 0) else 0


if __name__ == "__main__":
    sys.exit(main())
