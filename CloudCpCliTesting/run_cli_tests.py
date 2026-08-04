#!/usr/bin/env python3
"""run_cli_tests.py — Main orchestrator for CloudCP CLI test cases.

Pipeline per test case
----------------------
  1. Select dataset → confirm on disk (or skip with --skip-datagen)
  2. Apply per-case config.json overrides (backed up; restored after run)
  3. Allocate a new transfer_id
  4. Invoke the broker (batch_scheduler.py) — or print the command in --dry-run
  5. Validate the transfer report via scripts/report_validator.py
  6. Optionally clear the S3 bucket prefix
  7. Restore config.json to its pre-test state
  8. Emit a per-run record (JSON + Markdown summary)

Usage
-----
    # List all available CLI test cases
    python3 run_cli_tests.py --list

    # List with full detail (datasets, tags, pass criteria)
    python3 run_cli_tests.py --list --verbose

    # Dry-run a single case (no disk / config / broker changes)
    python3 run_cli_tests.py --case CLI-SMOKE-01 --dry-run

    # Run the smoke suite
    python3 run_cli_tests.py --tag smoke

    # Run all P0 cases
    python3 run_cli_tests.py --priority P0

    # Run the full suite
    python3 run_cli_tests.py --all

    # Skip S3 bucket clearing after each run
    python3 run_cli_tests.py --tag smoke --no-clear

    # Override bucket and endpoint
    python3 run_cli_tests.py --case CLI-SMOKE-01 \\
        --bucket my_bucket --endpoint https://minio.internal:9000

Notes
-----
- This script is safe to import (no side effects at import time).
- --dry-run never writes config.json, never invokes the broker or validator.
- Run on the Linux bryck host where datagen, cloudcp, and batch_scheduler.py exist.
"""

import argparse
import contextlib
import copy
import datetime as _dt
import json
import os
import pathlib
import shutil
import subprocess
import sys
import tempfile
import time

# ---------------------------------------------------------------------------
# Add parent dir to path so cli_config / cli_cases import cleanly
# ---------------------------------------------------------------------------
_HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))

import cli_config as _cfg
import cli_cases as _cases

# ---------------------------------------------------------------------------
# Output directory
# ---------------------------------------------------------------------------

_RUNS_DIR = pathlib.Path(_cfg.CLI_TEST_RUNS_DIR)


# ---------------------------------------------------------------------------
# Config.json patch / restore context manager
# ---------------------------------------------------------------------------

@contextlib.contextmanager
def _patched_config(overrides: dict, dry_run: bool):
    """Temporarily apply overrides to config.json; restore on exit.

    The original file is backed up to a sibling .bak file during the run.
    No-op when dry_run=True (overrides are printed but not applied).
    """
    if dry_run or not overrides:
        if overrides:
            print(f"  [dry-run] would apply config overrides: {json.dumps(overrides)}")
        yield
        return

    config_path = pathlib.Path(_cfg.CONFIG_FILE)
    backup_path = config_path.with_suffix(".json.cli_test_bak")

    try:
        original_text = config_path.read_text(encoding="utf-8")
        original = json.loads(original_text)
    except FileNotFoundError:
        print(
            f"  [warn] config.json not found at {config_path}; "
            "proceeding without patching."
        )
        yield
        return

    shutil.copy2(config_path, backup_path)

    patched = _deep_merge(copy.deepcopy(original), overrides)
    config_path.write_text(json.dumps(patched, indent=2), encoding="utf-8")
    print(f"  [config] applied overrides: {json.dumps(overrides)}")

    try:
        yield
    finally:
        config_path.write_text(original_text, encoding="utf-8")
        backup_path.unlink(missing_ok=True)
        print(f"  [config] restored {config_path}")


def _deep_merge(base: dict, overrides: dict) -> dict:
    """Recursively merge overrides into base (in-place on base)."""
    for key, value in overrides.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            _deep_merge(base[key], value)
        else:
            base[key] = value
    return base


# ---------------------------------------------------------------------------
# Broker invocation
# ---------------------------------------------------------------------------

def _broker_cmd(transfer_id: int, extra_args: list[str]) -> list[str]:
    """Build the broker command list for subprocess."""
    try:
        scheduler = _cfg.resolve_scheduler_script()
    except FileNotFoundError as exc:
        scheduler = "<batch_scheduler.py-not-found>"
        print(f"  [warn] {exc}")

    cmd = [
        _cfg.SCHEDULER_PYTHON,
        scheduler,
        "--config", _cfg.CONFIG_FILE,
        "--transfer-id", str(transfer_id),
    ]
    cmd.extend(extra_args)
    return cmd


def _run_broker(transfer_id: int, extra_args: list[str], dry_run: bool) -> int:
    """Invoke the broker and return its exit code (0 on dry-run)."""
    cmd = _broker_cmd(transfer_id, extra_args)
    print(f"  [broker] {' '.join(cmd)}")
    if dry_run:
        print("  [dry-run] broker not invoked.")
        return 0

    result = subprocess.run(cmd, text=True)
    return result.returncode


# ---------------------------------------------------------------------------
# Bucket clear
# ---------------------------------------------------------------------------

def _clear_bucket_prefix(bucket: str, prefix: str, endpoint: str, dry_run: bool) -> None:
    """Remove all objects under s3://<bucket>/<prefix>/ using the aws CLI."""
    s3_uri = f"s3://{bucket}/{prefix}/"
    cmd = [
        "aws", "s3", "rm", s3_uri, "--recursive",
        "--endpoint-url", endpoint,
    ]
    print(f"  [clear] {' '.join(cmd)}")
    if dry_run:
        print("  [dry-run] bucket not cleared.")
        return
    subprocess.run(cmd, check=False)


# ---------------------------------------------------------------------------
# Run a single case
# ---------------------------------------------------------------------------

def run_case(
    case: dict,
    *,
    bucket: str,
    endpoint: str,
    prefix: str,
    dry_run: bool,
    no_clear: bool,
    skip_datagen: bool,
    broker_extra_args: list[str],
) -> dict:
    """Execute one CLI test case and return a result record."""
    case_id = case["id"]
    start = _dt.datetime.now(_dt.timezone.utc)
    print(f"\n{'='*60}")
    print(f"  CASE: {case_id} — {case['title']}")
    print(f"  GROUP: {case['group']}  PRIORITY: {case['priority']}")
    print(f"  DATASETS: {', '.join(case['datasets'])}")
    print(f"  OVERRIDES: {case['config_overrides'] or '(none)'}")
    print(f"{'='*60}")

    result: dict = {
        "case_id": case_id,
        "title": case["title"],
        "group": case["group"],
        "priority": case["priority"],
        "start_utc": start.isoformat(),
        "end_utc": None,
        "transfer_id": None,
        "broker_exit_code": None,
        "validation_passed": None,
        "outcome": "UNKNOWN",
        "notes": [],
    }

    transfer_id = _cfg.next_transfer_id() if not dry_run else 9999
    result["transfer_id"] = transfer_id
    print(f"  [transfer] id={transfer_id}")

    # Datagen step (informational; actual datagen is a separate prerequisite)
    if not skip_datagen:
        for ds_id in case["datasets"]:
            spec_dir = _cfg.DATASET_SPEC_DIR / ds_id
            if spec_dir.exists():
                print(f"  [datagen] spec dir found: {spec_dir}")
            else:
                result["notes"].append(
                    f"Dataset spec dir not found: {spec_dir}. "
                    "Run datagen manually before executing this case."
                )
                print(f"  [warn] spec dir not found: {spec_dir}")

    # Run broker under patched config
    broker_rc = 0
    with _patched_config(case["config_overrides"], dry_run):
        broker_rc = _run_broker(transfer_id, broker_extra_args, dry_run)

    result["broker_exit_code"] = broker_rc

    if not dry_run and broker_rc != 0:
        result["outcome"] = "FAIL"
        result["notes"].append(f"Broker exited with rc={broker_rc}.")
    else:
        # Validate report
        report_path = _cfg.transfer_report_path(transfer_id)
        validator_script = _HERE / "scripts" / "report_validator.py"

        validator_cmd = [
            sys.executable,
            str(validator_script),
            "--csv", str(report_path),
        ]
        print(f"  [validate] {' '.join(validator_cmd)}")

        if dry_run:
            print("  [dry-run] validator not invoked.")
            result["validation_passed"] = None
            result["outcome"] = "DRY-RUN"
        else:
            vr = subprocess.run(validator_cmd, text=True, capture_output=True)
            if vr.returncode == 0:
                result["validation_passed"] = True
                result["outcome"] = "PASS"
                print("  [validate] PASSED")
            else:
                result["validation_passed"] = False
                result["outcome"] = "FAIL"
                print("  [validate] FAILED")
                print(vr.stdout[-2000:] if vr.stdout else "")
                print(vr.stderr[-2000:] if vr.stderr else "")
                result["notes"].append("Report validation failed.")

    # Clear bucket prefix
    if not no_clear:
        run_prefix = f"{prefix}/{case_id}"
        _clear_bucket_prefix(bucket, run_prefix, endpoint, dry_run)

    result["end_utc"] = _dt.datetime.now(_dt.timezone.utc).isoformat()
    elapsed = (
        _dt.datetime.fromisoformat(result["end_utc"])
        - _dt.datetime.fromisoformat(result["start_utc"])
    ).total_seconds()
    print(f"  OUTCOME: {result['outcome']}  ({elapsed:.1f}s)")
    return result


# ---------------------------------------------------------------------------
# Run suite
# ---------------------------------------------------------------------------

def run_suite(
    selected: list[dict],
    *,
    bucket: str,
    endpoint: str,
    prefix: str,
    dry_run: bool,
    no_clear: bool,
    skip_datagen: bool,
    broker_extra_args: list[str],
) -> list[dict]:
    """Run all selected cases and return a list of result records."""
    results = []
    for case in selected:
        r = run_case(
            case,
            bucket=bucket,
            endpoint=endpoint,
            prefix=prefix,
            dry_run=dry_run,
            no_clear=no_clear,
            skip_datagen=skip_datagen,
            broker_extra_args=broker_extra_args,
        )
        results.append(r)
    return results


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

def _emit_report(results: list[dict], out_dir: pathlib.Path) -> None:
    """Write run_report.json and run_report.md to out_dir."""
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = _dt.datetime.now(_dt.timezone.utc).strftime("%Y%m%d_%H%M%S")

    json_path = out_dir / f"run_report_{ts}.json"
    json_path.write_text(json.dumps(results, indent=2), encoding="utf-8")
    print(f"\n[report] JSON  → {json_path}")

    n_pass = sum(1 for r in results if r["outcome"] == "PASS")
    n_fail = sum(1 for r in results if r["outcome"] == "FAIL")
    n_dry = sum(1 for r in results if r["outcome"] == "DRY-RUN")
    n_unk = len(results) - n_pass - n_fail - n_dry

    lines = [
        "# CLI Test Run Report",
        f"**Generated:** {ts}Z",
        f"**Total cases:** {len(results)}  "
        f"PASS: {n_pass}  FAIL: {n_fail}  DRY-RUN: {n_dry}  OTHER: {n_unk}",
        "",
        "| Case ID | Group | Priority | Outcome | Transfer ID | Notes |",
        "|---|---|---|---|---|---|",
    ]
    for r in results:
        notes = "; ".join(r.get("notes", [])) or "—"
        lines.append(
            f"| {r['case_id']} | {r['group']} | {r['priority']} "
            f"| {r['outcome']} | {r['transfer_id']} | {notes} |"
        )

    md_path = out_dir / f"run_report_{ts}.md"
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"[report] MD    → {md_path}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="CloudCP CLI test runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # Selection
    sel = p.add_mutually_exclusive_group()
    sel.add_argument("--all", action="store_true", help="Run all CLI test cases")
    sel.add_argument("--case", metavar="ID", help="Run a single case by ID")
    sel.add_argument("--tag", metavar="TAG", help="Run all cases matching a tag")
    sel.add_argument("--group", metavar="GROUP", help="Run all cases in a group")
    sel.add_argument("--list", action="store_true", help="List cases and exit")

    p.add_argument(
        "--priority", choices=["P0", "P2"],
        help="Filter to only cases of this priority (applies to --all/--tag/--group)",
    )

    # Execution flags
    p.add_argument("--dry-run", action="store_true", help="Print commands; no execution")
    p.add_argument("--verbose", action="store_true", help="Show full case detail in --list")
    p.add_argument("--no-clear", action="store_true", help="Skip S3 bucket clearing after run")
    p.add_argument("--skip-datagen", action="store_true", help="Skip datagen check step")

    # Connection
    p.add_argument("--bucket", default=_cfg.DEFAULT_BUCKET, help="S3 bucket name")
    p.add_argument("--endpoint", default=_cfg.DEFAULT_ENDPOINT, help="S3 endpoint URL")
    p.add_argument("--prefix", default=_cfg.DEFAULT_PREFIX, help="S3 key prefix for test objects")

    # Output
    p.add_argument("--out-dir", default=_cfg.CLI_TEST_RUNS_DIR, help="Output directory for reports")

    # Pass-through to broker
    p.add_argument(
        "--broker-arg", action="append", dest="broker_args", default=[], metavar="ARG",
        help="Extra argument to pass to batch_scheduler.py (repeatable)",
    )

    return p


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    # --list
    if args.list:
        _cases.list_cases(verbose=args.verbose)
        return 0

    # Selection
    if args.all:
        selected = _cases.filter_cases(priority=args.priority)
    elif args.case:
        selected = _cases.filter_cases(case_id=args.case, priority=args.priority)
    elif args.tag:
        selected = _cases.filter_cases(tag=args.tag, priority=args.priority)
    elif args.group:
        selected = _cases.filter_cases(group=args.group, priority=args.priority)
    else:
        parser.print_help()
        return 1

    if not selected:
        print("[run] No cases matched the selection criteria.")
        return 0

    print(f"[run] {len(selected)} case(s) selected.")

    results = run_suite(
        selected,
        bucket=args.bucket,
        endpoint=args.endpoint,
        prefix=args.prefix,
        dry_run=args.dry_run,
        no_clear=args.no_clear,
        skip_datagen=args.skip_datagen,
        broker_extra_args=args.broker_args,
    )

    _emit_report(results, pathlib.Path(args.out_dir))

    n_fail = sum(1 for r in results if r["outcome"] == "FAIL")
    return 1 if n_fail else 0


if __name__ == "__main__":
    sys.exit(main())
