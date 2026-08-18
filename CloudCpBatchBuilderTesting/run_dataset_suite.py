#!/usr/bin/env python3
"""Run a dynamic multi-dataset remote BatchBuilder validation suite."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Dict, List

from functions.config_loader import load_json_config, merge_dict
from functions.dataset_discovery import list_source_datasets, resolve_dataset_selection
from functions.remote_ops import connect_ssh
from reports.report_lib import (
    annotate_and_update_performance_warnings,
    compute_variation,
    extract_run_metrics,
    render_shareable_dashboard,
    write_structured_reports,
)

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_CONFIG_PATH = BASE_DIR / "config" / "framework_config.json"


def read_text(path: Path) -> str:
    with path.open("r", encoding="utf-8") as handle:
        return handle.read()


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(content)


def load_effective_config(config_path: Path) -> Dict[str, object]:
    base = load_json_config(DEFAULT_CONFIG_PATH)
    if config_path == DEFAULT_CONFIG_PATH:
        return base
    override = load_json_config(config_path)
    return merge_dict(base, override)


# Named suite presets — each entry is a dict of flag overrides applied before CLI parsing.
# "DEFAULT_OVERRIDE_FILE" is a sentinel replaced with the actual default path at runtime.
SUITE_PRESETS: Dict[str, Dict[str, object]] = {
    "default":      {},
    "override":     {"batch_override_file": "DEFAULT_OVERRIDE_FILE", "no_timeout": True},
    "all":          {"dataset_mode": "all",  "no_timeout": True},
    "override-all": {"batch_override_file": "DEFAULT_OVERRIDE_FILE", "dataset_mode": "all", "no_timeout": True},
}


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run dynamic remote validation suite for datasets discovered from source host.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH))
    parser.add_argument("--source-host", default="")
    parser.add_argument("--dest-host", default="")
    parser.add_argument("--username", default="")
    parser.add_argument("--password", default=os.environ.get("BRYCK_SSH_PASSWORD", ""))
    parser.add_argument("--source-dir", default="")
    parser.add_argument("--dest-dir", default="")
    parser.add_argument("--dataset-pattern", default="")
    parser.add_argument("--dataset-mode", choices=["all", "random", "explicit"], default="")
    parser.add_argument("--datasets", nargs="*", default=[])
    parser.add_argument("--random-count", type=int, default=-1)
    parser.add_argument("--max-datasets", type=int, default=-1)
    parser.add_argument("--temp-sample-interval", type=float, default=-1)
    parser.add_argument("--cleanup-output", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--cleanup-remote", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument(
        "--allow-duplicate-datasets",
        action="store_true",
        help="Keep duplicate names in --datasets list. Useful for A/B micro-benchmark repetitions.",
    )
    parser.add_argument("--list-only", action="store_true", help="Only list selected datasets and exit.")
    parser.add_argument(
        "--override", action="store_true",
        help="Apply batch parameter overrides from config/batch_param_overrides.json. "
             "The BATCH section of /etc/bryck/bryckcloud/config.json on the destination "
             "host is updated before each dataset run and the original is restored afterwards.",
    )
    parser.add_argument(
        "--suite",
        choices=list(SUITE_PRESETS.keys()),
        default="",
        help=argparse.SUPPRESS,  # Advanced: use --override instead
    )
    parser.add_argument(
        "--batch-override-file", default="",
        help=argparse.SUPPRESS,  # Advanced: use --override instead
    )
    parser.add_argument(
        "--no-timeout", action="store_true",
        help=argparse.SUPPRESS,  # Set automatically by --override
    )
    args = parser.parse_args()

    # --override is the simple flag; apply it before the advanced preset logic.
    default_override_path = BASE_DIR / "config" / "batch_param_overrides.json"
    if args.override and not args.batch_override_file:
        args.batch_override_file = str(default_override_path)
        args.no_timeout = True

    # Advanced: --suite preset (still supported, hidden from --help)
    if args.suite and args.suite in SUITE_PRESETS:
        preset = SUITE_PRESETS[args.suite]
        if preset.get("batch_override_file") == "DEFAULT_OVERRIDE_FILE" and not args.batch_override_file:
            args.batch_override_file = str(default_override_path)
        if preset.get("no_timeout") and not args.no_timeout:
            args.no_timeout = True
        if preset.get("dataset_mode") and not args.dataset_mode:
            args.dataset_mode = str(preset["dataset_mode"])

    config = load_effective_config(Path(args.config).resolve())
    hosts = config.get("hosts", {}) if isinstance(config.get("hosts"), dict) else {}
    paths = config.get("paths", {}) if isinstance(config.get("paths"), dict) else {}
    runtime = config.get("runtime", {}) if isinstance(config.get("runtime"), dict) else {}

    source_host = args.source_host or str(hosts.get("source_host", ""))
    dest_host = args.dest_host or str(hosts.get("dest_host", ""))
    username = args.username or str(hosts.get("username", ""))
    password = args.password or str(hosts.get("password", ""))
    source_dir = args.source_dir or str(paths.get("source_dir", ""))
    dest_dir = args.dest_dir or str(paths.get("dest_dir", ""))

    dataset_pattern = args.dataset_pattern or str(runtime.get("dataset_pattern", "*.yaml"))
    dataset_mode = args.dataset_mode or str(runtime.get("dataset_mode", "all"))
    random_count = args.random_count if args.random_count > 0 else int(runtime.get("random_count", 10))
    max_datasets = args.max_datasets if args.max_datasets >= 0 else int(runtime.get("max_datasets", 0))
    temp_sample_interval = args.temp_sample_interval if args.temp_sample_interval > 0 else float(runtime.get("temp_sample_interval", 0.5))
    per_dataset_timeout_sec: int | None = (
        None if args.no_timeout
        else int(runtime.get("per_dataset_timeout_sec", 7200))
    )

    cleanup_output = bool(runtime.get("cleanup_output", True)) if args.cleanup_output is None else bool(args.cleanup_output)
    cleanup_remote = bool(runtime.get("cleanup_remote", True)) if args.cleanup_remote is None else bool(args.cleanup_remote)

    if not password:
        raise ValueError("Password is required. Set BRYCK_SSH_PASSWORD or provide --password.")

    explicit = list(args.datasets)
    available: List[str] = []
    if explicit:
        dataset_mode = "explicit"
        selected = explicit
    else:
        if not args.dataset_mode:
            # No CLI selection flags means run the full discovered dataset inventory.
            dataset_mode = "all"
        source_client = None
        try:
            source_client = connect_ssh(source_host, username, password)
            available = list_source_datasets(source_client, source_dir, dataset_pattern)
        finally:
            if source_client is not None:
                source_client.close()

        use_random = dataset_mode == "random"
        selected = resolve_dataset_selection(
            available=available,
            explicit=explicit,
            use_random=use_random,
            random_count=random_count,
            max_datasets=max_datasets,
        )

    if args.allow_duplicate_datasets:
        duplicate_count = len(selected) - len(set(selected))
        if duplicate_count > 0:
            print(
                f"Input datasets include {duplicate_count} duplicate entr"
                f"{'y' if duplicate_count == 1 else 'ies'}; keeping them as requested.",
                flush=True,
            )
    else:
        # Preserve order while removing accidental duplicates from explicit lists.
        original_selected_count = len(selected)
        selected = list(dict.fromkeys(selected))
        removed_duplicates = original_selected_count - len(selected)
        if removed_duplicates > 0:
            print(
                f"Input datasets contained {removed_duplicates} duplicate entr"
                f"{'y' if removed_duplicates == 1 else 'ies'}; duplicates were skipped.",
                flush=True,
            )

    if args.list_only:
        suite_label = f"suite={args.suite}" if args.suite else "direct"
        override_label = f"  batch-override={Path(args.batch_override_file).name}" if args.batch_override_file else ""
        print(f"Selected datasets ({suite_label}{override_label}):")
        for name in selected:
            print(name)
        return 0

    # Log effective run configuration for transparency.
    print(f"Suite         : {args.suite or 'default'}", flush=True)
    print(f"Dataset mode  : {dataset_mode}  ({len(selected)} selected)", flush=True)
    if args.batch_override_file:
        print(f"Batch overrides: {args.batch_override_file}", flush=True)
    print(f"No-timeout    : {args.no_timeout}", flush=True)
    print("", flush=True)

    here = Path(__file__).resolve().parent
    single_runner = here / "run_remote_batchbuilder_validation.py"

    suite_ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    suite_dir = here / "artifacts" / f"suite_{suite_ts}"
    runs_root = suite_dir / "runs"
    suite_dir.mkdir(parents=True, exist_ok=True)
    runs_root.mkdir(parents=True, exist_ok=True)

    run_results: List[Dict[str, object]] = []

    total = len(selected)
    for index, dataset in enumerate(selected, start=1):
        print(f"[{index}/{total}] Running dataset: {dataset}", flush=True)
        existing_run_names = {p.name for p in runs_root.glob("run_*")}
        cmd = [
            sys.executable,
            str(single_runner),
            "--config", str(Path(args.config).resolve()),
            "--source-host", source_host,
            "--dest-host", dest_host,
            "--username", username,
            "--password", password,
            "--source-dir", source_dir,
            "--dest-dir", dest_dir,
            "--dataset", dataset,
            "--temp-sample-interval", str(temp_sample_interval),
            "--artifacts-base-dir", str(runs_root),
        ]
        if cleanup_output:
            cmd.append("--cleanup-output")
        else:
            cmd.append("--no-cleanup-output")
        if cleanup_remote:
            cmd.append("--cleanup-remote")
        else:
            cmd.append("--no-cleanup-remote")
        if args.batch_override_file:
            cmd.extend(["--batch-override-file", args.batch_override_file])
        if args.no_timeout:
            cmd.append("--no-timeout")

        try:
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=per_dataset_timeout_sec)
        except subprocess.TimeoutExpired as exc:
            run_results.append(
                {
                    "dataset": dataset,
                    "status": "FAILED",
                    "run_name": "-",
                    "report": {
                        "dataset": dataset,
                        "status": "FAILED",
                        "issues": [f"Per-dataset timeout after {per_dataset_timeout_sec}s"],
                        "steps": [],
                    },
                    "stdout": exc.stdout or "",
                    "stderr": exc.stderr or "",
                }
            )
            continue

        new_run_dirs = sorted(
            [p for p in runs_root.glob("run_*") if p.name not in existing_run_names],
            key=lambda p: p.stat().st_mtime,
        )
        if not new_run_dirs:
            run_results.append(
                {
                    "dataset": dataset,
                    "status": "FAILED",
                    "run_name": "-",
                    "report": {
                        "dataset": dataset,
                        "status": "FAILED",
                        "issues": ["No run directory created."],
                        "steps": [],
                    },
                    "stdout": proc.stdout,
                    "stderr": proc.stderr,
                }
            )
            continue

        run_dir = new_run_dirs[-1]
        report_path = run_dir / "validation_report.json"
        if report_path.exists():
            report = json.loads(read_text(report_path))
        else:
            report = {
                "dataset": dataset,
                "status": "FAILED",
                "issues": ["validation_report.json missing"],
                "steps": [],
            }

        print(
            f"[{index}/{total}] Completed dataset: {dataset} status={report.get('status', 'FAILED')}",
            flush=True,
        )

        run_results.append(
            {
                "dataset": dataset,
                "status": str(report.get("status", "FAILED")),
                "run_name": run_dir.name,
                "report": report,
                "stdout": proc.stdout,
                "stderr": proc.stderr,
            }
        )

    metrics_rows = [extract_run_metrics(run) for run in run_results]
    baseline_path = here / "artifacts" / "performance_baselines.json"
    annotate_and_update_performance_warnings(metrics_rows, baseline_path)
    variation = compute_variation(metrics_rows)

    perf_flag_count = sum(
        len(row.get("perf_warnings", []))
        for row in metrics_rows
        if isinstance(row.get("perf_warnings"), list)
    )

    suite_summary = {
        "suite": suite_dir.name,
        "generated_at": dt.datetime.now().isoformat(),
        "preset": args.suite or "default",
        "mode": dataset_mode,
        "dataset_pattern": dataset_pattern,
        "selected_count": len(selected),
        "datasets": selected,
        "batch_overrides_file": args.batch_override_file or None,
        "no_timeout": args.no_timeout,
        "results": [
            {
                "dataset": run["dataset"],
                "status": run["status"],
                "run_name": run["run_name"],
            }
            for run in run_results
        ],
        "variation": variation,
        "performance_warning_count": perf_flag_count,
        "performance_baseline_file": str(baseline_path),
    }
    write_text(suite_dir / "suite_summary.json", json.dumps(suite_summary, indent=2))

    write_structured_reports(suite_dir, metrics_rows, variation, suite_summary)

    html_report = render_shareable_dashboard(
        suite_name=f"BatchBuilder Suite Report ({suite_dir.name})",
        generated_at=dt.datetime.now().isoformat(timespec="seconds"),
        metrics_rows=metrics_rows,
        variation=variation,
    )
    suite_html = suite_dir / "shareable_report.html"
    write_text(suite_html, html_report)

    zip_path = shutil.make_archive(str(suite_dir / "shareable_bundle"), "zip", root_dir=suite_dir)

    print(f"Suite directory: {suite_dir}")
    if dataset_mode == "explicit":
        print("Discovered datasets on source host: skipped (explicit mode)")
    else:
        print(f"Discovered datasets on source host: {len(available)}")
    print(f"Selected datasets for this run: {len(selected)}")
    print(f"Shareable HTML: {suite_html}")
    print(f"Structured reports: {suite_dir / 'structured_reports'}")
    print(f"Shareable bundle: {zip_path}")

    return 0 if all(run["status"] == "PASSED" for run in run_results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
