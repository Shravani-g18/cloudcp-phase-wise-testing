#!/usr/bin/env python3
"""Run one dynamic dataset BatchBuilder validation across remote hosts."""

from __future__ import annotations

import argparse
import datetime as dt
import getpass
import json
import os
import re
import shlex
import time
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

from assertions.batchbuilder_assertions import (
    TierTotals,
    build_tier_thresholds,
    classify_tier as _classify_tier,
    compare_summaries as _compare_summaries,
    iter_dataset_records,
    parse_batch_summary_csv,
    parse_batch_detail_rows,
    summarize_dataset_file as _summarize_dataset_file,
    validate_batch_parameters,
)
from functions.config_loader import load_json_config, merge_dict
from functions.dataset_discovery import list_source_datasets
from functions.remote_ops import (
    apply_remote_batch_overrides,
    connect_ssh,
    count_remote_batch_files,
    ensure_remote_dir,
    expand_remote_home,
    restore_remote_config,
    run_remote,
)
from functions.telemetry_hooks import (
    add_derived_metrics,
    build_monitored_batchbuilder_command,
    parse_elapsed_seconds,
    parse_gnu_time_metrics,
    parse_temperature_metrics,
)
from reports.report_lib import build_master_index_html, render_run_html

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_CONFIG_PATH = BASE_DIR / "config" / "framework_config.json"


def _default_tier_model() -> tuple[List[Tuple[str, int]], str]:
    cfg = load_json_config(DEFAULT_CONFIG_PATH)
    validation = cfg.get("validation", {}) if isinstance(cfg.get("validation"), dict) else {}
    return build_tier_thresholds(validation.get("tiers", [])), str(validation.get("overflow_tier", "large"))


def parse_dataset_lines(lines: Iterable[str]) -> List[Tuple[int, str]]:
    return list(iter_dataset_records(lines))


def classify_tier(size_bytes: int) -> str:
    tier_thresholds, overflow_tier = _default_tier_model()
    return _classify_tier(size_bytes, tier_thresholds, overflow_tier)


def expected_tier_summary(records: Iterable[Tuple[int, str]]) -> Dict[str, TierTotals]:
    tier_thresholds, overflow_tier = _default_tier_model()
    summary: Dict[str, TierTotals] = {name: TierTotals() for name, _ in tier_thresholds}
    summary[overflow_tier] = TierTotals()
    for size, _ in records:
        tier = _classify_tier(size, tier_thresholds, overflow_tier)
        summary[tier].file_count += 1
        summary[tier].total_bytes += size
    return summary


def summarize_dataset_file(path: Path) -> Tuple[Dict[str, TierTotals], int]:
    tier_thresholds, overflow_tier = _default_tier_model()
    return _summarize_dataset_file(path, tier_thresholds, overflow_tier)


def compare_summaries(expected: Dict[str, TierTotals], actual: Dict[str, TierTotals], total_row: TierTotals) -> List[str]:
    return _compare_summaries(expected, actual, total_row)


def shell_join(parts: List[str]) -> str:
    return " ".join(shlex.quote(p) for p in parts)


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(content)


def read_text(path: Path) -> str:
    with path.open("r", encoding="utf-8") as handle:
        return handle.read()


def append_command_log(path: Path, title: str, command: str, code: int, out: str, err: str) -> None:
    block = [
        f"\n=== {title} ===",
        f"COMMAND: {command}",
        f"EXIT: {code}",
        "STDOUT:",
        out or "",
        "STDERR:",
        err or "",
        "",
    ]
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write("\n".join(block))


def validate_requested_dataset_name(dataset: str) -> None:
    if not re.match(r"^[A-Za-z0-9_.-]+$", dataset):
        raise ValueError("Dataset filename can only contain letters, numbers, underscore, dash, and dot.")


def load_effective_config(config_path: Path) -> Dict[str, object]:
    base = load_json_config(DEFAULT_CONFIG_PATH)
    if config_path == DEFAULT_CONFIG_PATH:
        return base
    override = load_json_config(config_path)
    return merge_dict(base, override)


def resolve_value(cli_value: object, cfg_value: object, default_value: object = None) -> object:
    if cli_value not in (None, ""):
        return cli_value
    if cfg_value not in (None, ""):
        return cfg_value
    return default_value


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate one dataset run for dynamic remote BatchBuilder flow.")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH))
    parser.add_argument("--source-host", default="")
    parser.add_argument("--dest-host", default="")
    parser.add_argument("--username", default="")
    parser.add_argument("--password", default=os.environ.get("BRYCK_SSH_PASSWORD", ""))
    parser.add_argument("--source-dir", default="")
    parser.add_argument("--dataset", default="")
    parser.add_argument("--dest-dir", default="")
    parser.add_argument("--dest-csv", default="")
    parser.add_argument("--output-dir", default="")
    parser.add_argument("--batchbuilder-python", default="")
    parser.add_argument("--artifacts-base-dir", default="")
    parser.add_argument("--temp-sample-interval", type=float, default=-1)
    parser.add_argument("--cleanup-remote", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--cleanup-output", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--list-datasets", action="store_true", help="List remote source datasets and exit.")
    parser.add_argument(
        "--batch-override-file", default="",
        help="Path to batch_param_overrides.json. When set, BATCH parameters in "
             "/etc/bryck/bryckcloud/config.json on the destination host are updated "
             "before the run and restored afterwards.",
    )
    parser.add_argument(
        "--no-timeout", action="store_true",
        help="Disable all remote command timeouts. Useful for very large datasets.",
    )
    args = parser.parse_args()

    config = load_effective_config(Path(args.config).resolve())
    hosts = config.get("hosts", {}) if isinstance(config.get("hosts"), dict) else {}
    paths = config.get("paths", {}) if isinstance(config.get("paths"), dict) else {}
    runtime = config.get("runtime", {}) if isinstance(config.get("runtime"), dict) else {}
    validation = config.get("validation", {}) if isinstance(config.get("validation"), dict) else {}

    source_host = str(resolve_value(args.source_host, hosts.get("source_host"), ""))
    dest_host = str(resolve_value(args.dest_host, hosts.get("dest_host"), ""))
    username = str(resolve_value(args.username, hosts.get("username"), ""))
    password = str(resolve_value(args.password, hosts.get("password"), ""))
    source_dir = str(resolve_value(args.source_dir, paths.get("source_dir"), ""))
    dest_dir = str(resolve_value(args.dest_dir, paths.get("dest_dir"), ""))
    dest_csv = str(resolve_value(args.dest_csv, paths.get("dest_csv"), "bryck_file_list.csv"))
    output_dir = str(resolve_value(args.output_dir, paths.get("output_dir"), "output"))
    batchbuilder_python = str(resolve_value(args.batchbuilder_python, paths.get("batchbuilder_python"), "python3"))
    dataset_pattern = str(runtime.get("dataset_pattern", "*.yaml"))

    temp_sample_interval = args.temp_sample_interval if args.temp_sample_interval > 0 else float(runtime.get("temp_sample_interval", 0.5))
    remote_cmd_timeout_sec = None if args.no_timeout else float(runtime.get("remote_command_timeout_sec", 1800))

    cleanup_output = bool(runtime.get("cleanup_output", True)) if args.cleanup_output is None else bool(args.cleanup_output)
    cleanup_remote = bool(runtime.get("cleanup_remote", True)) if args.cleanup_remote is None else bool(args.cleanup_remote)

    tier_thresholds = build_tier_thresholds(validation.get("tiers", []))
    overflow_tier = str(validation.get("overflow_tier", "large"))

    if not password:
        password = getpass.getpass("SSH password for remote hosts: ")

    source_client = None
    dest_client = None

    # Batch override state — populated when --batch-override-file is provided.
    _batch_overrides: Dict[str, object] = {}
    _original_remote_config: Dict[str, object] | None = None
    _override_config_path: str = "/etc/bryck/bryckcloud/config.json"
    _use_sudo: bool = False

    if args.batch_override_file:
        _override_file = Path(args.batch_override_file).resolve()
        if not _override_file.exists():
            raise FileNotFoundError(f"Batch override file not found: {_override_file}")
        _batch_overrides = load_json_config(_override_file)
        _override_config_path = str(_batch_overrides.get("remote_config_path", _override_config_path))
        _use_sudo = bool(_batch_overrides.get("use_sudo", False))

    timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    artifacts_base_dir = Path(args.artifacts_base_dir) if args.artifacts_base_dir else BASE_DIR / "artifacts"
    artifacts_root = artifacts_base_dir / f"run_{timestamp}"
    artifacts_root.mkdir(parents=True, exist_ok=True)

    log_file = artifacts_root / "execution.log"

    report: Dict[str, object] = {
        "status": "FAILED",
        "timestamp": timestamp,
        "source_host": source_host,
        "dest_host": dest_host,
        "dataset": "",
        "dest_dir": dest_dir,
        "output_dir": output_dir,
        "dataset_size_bytes": 0,
        "steps": [],
        "issues": [],
    }

    local_report_json = artifacts_root / "validation_report.json"
    local_report_md = artifacts_root / "validation_report.md"
    local_report_html = artifacts_root / "validation_report.html"
    local_summary = artifacts_root / "batch_summary.csv"

    def persist_report_snapshot() -> None:
        write_text(local_report_json, json.dumps(report, indent=2))

    def log_step(step: str, details: str) -> None:
        report["steps"].append({"step": step, "details": details})
        persist_report_snapshot()

    try:
        dataset = args.dataset.strip() if args.dataset else ""
        if dataset:
            # Validate explicit user input before any remote connections.
            report["dataset"] = dataset
            validate_requested_dataset_name(dataset)

        source_client = connect_ssh(source_host, username, password)
        dest_client = connect_ssh(dest_host, username, password)
        log_step("connect_source", f"Connected to {source_host} as {username}")
        log_step("connect_dest", f"Connected to {dest_host} as {username}")

        # Apply batch parameter overrides to the destination host config before running.
        if _batch_overrides:
            _original_remote_config = apply_remote_batch_overrides(
                dest_client, _override_config_path, _batch_overrides, use_sudo=_use_sudo,
            )
            tiers_applied = list((_batch_overrides.get("tiers") or {}).keys())
            log_step(
                "apply_batch_overrides",
                f"Applied overrides for tiers {tiers_applied} to {_override_config_path}",
            )
            report["batch_overrides"] = {
                "applied": True,
                "file": Path(args.batch_override_file).name,
                "remote_config": _override_config_path,
                "tiers": tiers_applied,
            }

        available_datasets: List[str] = []
        if args.list_datasets:
            available_datasets = list_source_datasets(source_client, source_dir, dataset_pattern)
            for name in available_datasets:
                print(name)
            return 0

        if not dataset:
            available_datasets = list_source_datasets(source_client, source_dir, dataset_pattern)
            if not available_datasets:
                raise ValueError("No datasets found on source host.")
            dataset = available_datasets[0]

        validate_requested_dataset_name(dataset)
        report["dataset"] = dataset
        write_text(log_file, f"Run timestamp: {timestamp}\nDataset: {dataset}\n")

        preclean_cmd = f"rm -f {shlex.quote(dataset)} {shlex.quote(dest_csv)}"
        code, out, err = run_remote(dest_client, preclean_cmd, cwd=dest_dir, timeout_sec=remote_cmd_timeout_sec)
        append_command_log(log_file, "preclean_dest_dataset_csv", preclean_cmd, code, out, err)
        if code != 0:
            raise RuntimeError(f"preclean destination failed: {err.strip() or out.strip()}")
        log_step("preclean_dest_dataset_csv", "Removed any existing dataset and CSV on destination")

        source_dir_expanded = expand_remote_home(source_client, source_dir)
        remote_dataset_path = f"{source_dir_expanded.rstrip('/')}/{dataset}"
        local_dataset = artifacts_root / dataset

        log_step("download_dataset_start", f"Fetching {remote_dataset_path}")
        t0 = time.perf_counter()
        with source_client.open_sftp() as source_sftp:
            source_sftp.stat(remote_dataset_path)
            source_sftp.get(remote_dataset_path, str(local_dataset))
        download_elapsed = time.perf_counter() - t0
        report["dataset_size_bytes"] = local_dataset.stat().st_size
        log_step("download_dataset", f"Downloaded {remote_dataset_path} to {local_dataset.name} in {download_elapsed:.2f}s")

        log_step("upload_dataset_start", f"Uploading to {dest_dir.rstrip('/')}/{dataset}")
        t0 = time.perf_counter()
        with dest_client.open_sftp() as dest_sftp:
            ensure_remote_dir(dest_sftp, dest_dir)
            remote_dest_dataset_path = f"{dest_dir.rstrip('/')}/{dataset}"
            dest_sftp.put(str(local_dataset), remote_dest_dataset_path)
        upload_elapsed = time.perf_counter() - t0
        log_step("upload_dataset", f"Uploaded dataset to {remote_dest_dataset_path} in {upload_elapsed:.2f}s")

        if cleanup_output:
            cmd = f"rm -rf {shlex.quote(output_dir)}"
            code, out, err = run_remote(dest_client, cmd, cwd=dest_dir, timeout_sec=remote_cmd_timeout_sec)
            append_command_log(log_file, "cleanup_output", cmd, code, out, err)
            if code != 0:
                raise RuntimeError(f"Failed cleanup-output: {err.strip() or out.strip()}")
            log_step("cleanup_output", f"Deleted {output_dir} before test run")

        grep_cmd = f"grep -v '^#' {shlex.quote(dataset)} > {shlex.quote(dest_csv)}"
        code, out, err = run_remote(dest_client, grep_cmd, cwd=dest_dir, timeout_sec=remote_cmd_timeout_sec)
        append_command_log(log_file, "yaml_to_csv", grep_cmd, code, out, err)
        if code != 0:
            raise RuntimeError(f"grep step failed: {err.strip() or out.strip()}")
        log_step("yaml_to_csv", f"Created {dest_csv} from {dataset} using grep")

        bb_cmd = shell_join([batchbuilder_python, "BatchBuilder.py", dest_csv, "-o", output_dir])
        timed_bb_cmd = build_monitored_batchbuilder_command(bb_cmd, temp_sample_interval)
        log_step("run_batchbuilder_start", "Launching BatchBuilder.py")
        t0 = time.perf_counter()
        code, out, err = run_remote(dest_client, timed_bb_cmd, cwd=dest_dir, timeout_sec=remote_cmd_timeout_sec)
        run_elapsed = time.perf_counter() - t0
        append_command_log(log_file, "run_batchbuilder", timed_bb_cmd, code, out, err)
        perf_metrics = parse_gnu_time_metrics(err)
        perf_metrics.update(parse_temperature_metrics(f"{out}\n{err}"))
        if code != 0:
            raise RuntimeError("BatchBuilder.py failed. Inspect execution.log in artifacts directory.")
        log_step("run_batchbuilder", f"BatchBuilder.py completed successfully in {run_elapsed:.2f}s")

        summary_candidates = [
            f"{dest_dir.rstrip('/')}/{output_dir.strip('/')}/batch_summary.csv",
            f"{dest_dir.rstrip('/')}/{output_dir.strip('/')}/batch_summary.txt",
        ]
        fetched_summary: str | None = None
        with dest_client.open_sftp() as dest_sftp:
            for remote_summary in summary_candidates:
                try:
                    dest_sftp.stat(remote_summary)
                    dest_sftp.get(remote_summary, str(local_summary))
                    fetched_summary = remote_summary
                    break
                except Exception:
                    continue

        if fetched_summary is None:
            raise FileNotFoundError(
                "Could not find batch summary on destination. "
                f"Tried: {', '.join(summary_candidates)}"
            )

        log_step("download_summary", f"Fetched batch summary from {fetched_summary}")

        batch_files_count = count_remote_batch_files(dest_client, output_dir, dest_dir, timeout_sec=remote_cmd_timeout_sec)
        append_command_log(log_file, "count_batch_files", f"find {output_dir} -type f -name 'batch_[0-9]*.txt' | wc -l", 0, str(batch_files_count), "")
        log_step("count_batch_files", f"Remote batch file count: {batch_files_count}")

        expected, record_count = _summarize_dataset_file(local_dataset, tier_thresholds, overflow_tier)
        log_step("summarize_dataset", f"Parsed {record_count} records from dataset")
        actual, total_row = parse_batch_summary_csv(local_summary)
        issues = compare_summaries(expected, actual, total_row)
        if total_row.batch_count != batch_files_count:
            issues.append(
                "TOTAL batch_count mismatch against actual generated batch files "
                f"expected={batch_files_count} actual={total_row.batch_count}"
            )

        # Validate per-batch constraints against the configured BATCH parameters.
        if _batch_overrides and _batch_overrides.get("tiers"):
            batch_detail_rows = parse_batch_detail_rows(local_summary)
            if batch_detail_rows:
                param_violations = validate_batch_parameters(local_summary, _batch_overrides["tiers"])
                if param_violations:
                    issues.extend(
                        [f"BATCH_PARAM_VIOLATION: {v}" for v in param_violations]
                    )
                    log_step(
                        "validate_batch_params",
                        f"{len(param_violations)} batch parameter violation(s) found",
                    )
                else:
                    log_step("validate_batch_params", "All batch parameter constraints satisfied")
                report["batch_param_validation"] = {
                    "checked": True,
                    "violations": param_violations,
                    "batches_inspected": len(batch_detail_rows),
                }
            else:
                log_step(
                    "validate_batch_params",
                    "Skipped — batch_summary.csv is in legacy aggregated format",
                )
                report["batch_param_validation"] = {
                    "checked": False,
                    "reason": "legacy aggregated CSV format — per-batch rows unavailable",
                }

        report["status"] = "FAILED" if issues else "PASSED"
        report["issues"] = issues
        report["records"] = record_count
        report["summary_total"] = {
            "batch_count": total_row.batch_count,
            "file_count": total_row.file_count,
            "total_bytes": total_row.total_bytes,
        }

        perf_metrics = add_derived_metrics(perf_metrics, record_count, total_row.batch_count, total_row.total_bytes)
        report["performance"] = perf_metrics
        report["temperature"] = {
            "supported": perf_metrics.get("temperature_supported", False),
            "samples": perf_metrics.get("temp_samples"),
            "min_c": perf_metrics.get("temp_min_c"),
            "avg_c": perf_metrics.get("temp_avg_c"),
            "max_c": perf_metrics.get("temp_max_c"),
            "drives": perf_metrics.get("drive_temperatures", []),
        }
        report["disk_usage"] = perf_metrics.get("disk_usage", {})
        persist_report_snapshot()

        if cleanup_remote:
            cleanup_cmd = f"rm -f {shlex.quote(dataset)} {shlex.quote(dest_csv)}"
            code, out, err = run_remote(dest_client, cleanup_cmd, cwd=dest_dir, timeout_sec=remote_cmd_timeout_sec)
            append_command_log(log_file, "cleanup_remote", cleanup_cmd, code, out, err)
            if code != 0:
                report["issues"].append(f"cleanup_remote failed: {err.strip() or out.strip()}")
            else:
                log_step("cleanup_remote", "Removed copied dataset and CSV from destination")

        # Clean up local copy of the downloaded dataset YAML (no longer needed after validation).
        if local_dataset.exists():
            try:
                local_dataset.unlink()
                log_step("cleanup_local_dataset", f"Removed local dataset copy {local_dataset.name}")
            except OSError:
                pass  # Non-fatal; the JSON/HTML reports are what matter

    except Exception as exc:
        report["issues"].append(str(exc))
        report["status"] = "FAILED"
        persist_report_snapshot()
    finally:
        # Always restore the remote config before dropping the SSH connection.
        if _original_remote_config is not None and dest_client is not None:
            try:
                restore_remote_config(
                    dest_client, _override_config_path, _original_remote_config, _use_sudo
                )
                log_step("restore_batch_config", f"Restored original config at {_override_config_path}")
            except Exception as _restore_exc:
                report.setdefault("issues", []).append(
                    f"WARNING: Failed to restore remote config: {_restore_exc}"
                )
                persist_report_snapshot()

        if source_client is not None:
            source_client.close()
        if dest_client is not None:
            dest_client.close()

    write_text(local_report_json, json.dumps(report, indent=2))

    status = str(report.get("status", "FAILED"))
    summary_lines = [
        "# Remote BatchBuilder Validation Report",
        "",
        f"- Status: {status}",
        f"- Timestamp: {timestamp}",
        f"- Source host: {source_host}",
        f"- Destination host: {dest_host}",
        f"- Dataset: {report.get('dataset', '-')}",
        f"- Destination directory: {dest_dir}",
        f"- Output directory: {output_dir}",
        "",
        "## Steps",
    ]

    for step_entry in report.get("steps", []):
        summary_lines.append(f"- {step_entry['step']}: {step_entry['details']}")

    perf = report.get("performance", {}) if isinstance(report.get("performance"), dict) else {}
    summary_lines.extend(["", "## Performance"])
    for key in [
        "elapsed_sec",
        "cpu_percent",
        "max_rss_mb",
        "rows_per_sec",
        "batches_per_sec",
        "bytes_per_sec",
        "seconds_per_batch",
        "bytes_per_record",
    ]:
        summary_lines.append(f"- {key}: {perf.get(key, '-')}")

    temp = report.get("temperature", {}) if isinstance(report.get("temperature"), dict) else {}
    summary_lines.extend(["", "## Temperature"])
    for key in ["supported", "samples", "min_c", "avg_c", "max_c"]:
        summary_lines.append(f"- {key}: {temp.get(key, '-')}")

    drives = temp.get("drives", []) if isinstance(temp.get("drives"), list) else []
    summary_lines.extend(["", "## Drive Temperatures"])
    if drives:
        for drive in drives:
            summary_lines.append(
                "- {dev} (SN {sn}) samples={samples} min={min_c} avg={avg_c} max={max_c}".format(
                    dev=drive.get("dev", "-"),
                    sn=drive.get("sn", "-"),
                    samples=drive.get("samples", "-"),
                    min_c=drive.get("min_c", "-"),
                    avg_c=drive.get("avg_c", "-"),
                    max_c=drive.get("max_c", "-"),
                )
            )
    else:
        summary_lines.append("- None")

    disk_usage = report.get("disk_usage", {}) if isinstance(report.get("disk_usage"), dict) else {}
    summary_lines.extend(["", "## Disk Usage"])
    if disk_usage:
        for key in ["filesystem", "size_kb", "used_kb", "avail_kb", "used_pct", "mount"]:
            summary_lines.append(f"- {key}: {disk_usage.get(key, '-')}")
    else:
        summary_lines.append("- None")

    issues = report.get("issues", []) if isinstance(report.get("issues"), list) else []
    summary_lines.extend(["", "## Issues"])
    if issues:
        for issue in issues:
            summary_lines.append(f"- {issue}")
    else:
        summary_lines.append("- None")

    write_text(local_report_md, "\n".join(summary_lines) + "\n")
    write_text(local_report_html, render_run_html(report, read_text(log_file) if log_file.exists() else ""))

    dashboard_path = artifacts_root.parent / "index.html"
    write_text(dashboard_path, build_master_index_html(artifacts_root.parent))

    print(f"Validation status: {status}")
    print(f"Artifacts: {artifacts_root}")
    print(f"Run HTML report: {local_report_html}")
    print(f"Dashboard: {dashboard_path}")

    return 0 if status == "PASSED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
