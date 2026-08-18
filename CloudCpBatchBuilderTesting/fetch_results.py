#!/usr/bin/env python3
"""
Fetch metrics from a completed BatchBuilder suite run and regenerate the HTML dashboard.

After an unattended suite run finishes (triggered via start_unattended_suite.ps1), all
raw metrics are stored as JSON files inside the suite's artifacts directory. This script
reads those JSON files and re-renders a fresh HTML report without needing to re-run any
remote validation.

Workflow
--------
1. Run the suite (unattended):
       .\\start_unattended_suite.ps1 -Password 'mypass'
2. Wait for it to finish (check with check_unattended_suite.ps1).
3. Fetch and render the report on this machine:
       python fetch_results.py
       python fetch_results.py --open

Usage
-----
    # List all suite runs in the artifacts directory
    python fetch_results.py --list

    # Regenerate report from the most recent completed suite
    python fetch_results.py

    # Regenerate report from a specific suite directory
    python fetch_results.py --suite-dir artifacts/suite_20260818_153000

    # Regenerate and open in the default browser
    python fetch_results.py --open

    # Override where the output HTML is written
    python fetch_results.py --out-html my_report.html --open
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional

BASE_DIR = Path(__file__).resolve().parent
ARTIFACTS_DIR = BASE_DIR / "artifacts"

# ---------------------------------------------------------------------------
# Import reporting helpers (same ones run_dataset_suite.py uses).
# ---------------------------------------------------------------------------
try:
    from reports.report_lib import (
        annotate_and_update_performance_warnings,
        compute_variation,
        extract_run_metrics,
        render_shareable_dashboard,
        write_structured_reports,
    )
except ImportError as exc:
    print(f"ERROR: Could not import reporting library: {exc}", file=sys.stderr)
    print("Make sure you are running from the repo root or the remote_batchbuilder_validation directory.", file=sys.stderr)
    raise SystemExit(1)


def _read_json(path: Path) -> Dict[str, object]:
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as fh:
        fh.write(content)


def find_suite_dirs(artifacts_dir: Path) -> List[Path]:
    """Return all suite_* directories sorted newest-first."""
    dirs = sorted(
        [d for d in artifacts_dir.iterdir() if d.is_dir() and d.name.startswith("suite_")],
        key=lambda d: d.name,
        reverse=True,
    )
    return dirs


def find_run_reports(suite_dir: Path) -> List[Path]:
    """Return all validation_report.json files found inside the suite's runs/ tree."""
    runs_root = suite_dir / "runs"
    if not runs_root.exists():
        return []
    return sorted(
        runs_root.glob("run_*/validation_report.json"),
        key=lambda p: p.parent.name,
    )


def reconstruct_run_results(suite_dir: Path) -> List[Dict[str, object]]:
    """
    Re-build the run_results list that run_dataset_suite.py normally assembles
    while running. Each entry is constructed from a saved validation_report.json.
    """
    run_results: List[Dict[str, object]] = []
    for report_path in find_run_reports(suite_dir):
        run_dir = report_path.parent
        try:
            report = _read_json(report_path)
        except Exception as exc:
            print(f"  WARNING: Could not read {report_path}: {exc}", file=sys.stderr)
            report = {"status": "FAILED", "issues": [str(exc)], "steps": []}

        run_results.append(
            {
                "dataset": str(report.get("dataset", run_dir.name)),
                "status": str(report.get("status", "FAILED")),
                "run_name": run_dir.name,
                "report": report,
                # stdout/stderr not needed for report regeneration.
                "stdout": "",
                "stderr": "",
            }
        )
    return run_results


def load_existing_suite_summary(suite_dir: Path) -> Optional[Dict[str, object]]:
    """Load the suite_summary.json written by the original run, if available."""
    path = suite_dir / "suite_summary.json"
    if path.exists():
        try:
            return _read_json(path)
        except Exception:
            pass
    return None


def regenerate_report(
    suite_dir: Path,
    out_html: Optional[Path] = None,
    verbose: bool = True,
) -> Path:
    """
    Read all JSON artifacts from suite_dir and re-render the shareable HTML report.

    Returns the path of the generated HTML file.
    """
    if not suite_dir.exists():
        raise FileNotFoundError(f"Suite directory not found: {suite_dir}")

    run_results = reconstruct_run_results(suite_dir)
    if not run_results:
        raise RuntimeError(
            f"No completed run reports found in {suite_dir / 'runs'}. "
            "The suite may still be running or no datasets were processed."
        )

    if verbose:
        print(f"Suite directory  : {suite_dir}")
        print(f"Runs found       : {len(run_results)}")

    # Compute metrics and variation using the same pipeline as run_dataset_suite.py.
    metrics_rows = [extract_run_metrics(run) for run in run_results]
    baseline_path = ARTIFACTS_DIR / "performance_baselines.json"
    annotate_and_update_performance_warnings(metrics_rows, baseline_path)
    variation = compute_variation(metrics_rows)

    passed = sum(1 for r in run_results if r["status"] == "PASSED")
    failed = len(run_results) - passed

    if verbose:
        print(f"Results          : PASSED={passed}  FAILED={failed}")

    # Load original suite summary if available, or build a minimal one.
    existing_summary = load_existing_suite_summary(suite_dir)
    suite_summary = existing_summary or {
        "suite": suite_dir.name,
        "generated_at": dt.datetime.now().isoformat(),
        "selected_count": len(run_results),
        "datasets": [r["dataset"] for r in run_results],
        "results": [
            {"dataset": r["dataset"], "status": r["status"], "run_name": r["run_name"]}
            for r in run_results
        ],
    }
    # Always refresh the generation timestamp.
    suite_summary["regenerated_at"] = dt.datetime.now().isoformat(timespec="seconds")

    # Re-render structured JSON reports (overwrites existing ones).
    write_structured_reports(suite_dir, metrics_rows, variation, suite_summary)

    # Re-render the shareable HTML dashboard.
    html_content = render_shareable_dashboard(
        suite_name=f"BatchBuilder Suite — {suite_dir.name}",
        generated_at=dt.datetime.now().isoformat(timespec="seconds"),
        metrics_rows=metrics_rows,
        variation=variation,
    )

    dest_html = out_html or (suite_dir / "shareable_report.html")
    _write_text(dest_html, html_content)

    # Also refresh suite_summary.json with the regenerated timestamp.
    _write_text(
        suite_dir / "suite_summary.json",
        json.dumps(suite_summary, indent=2),
    )

    if verbose:
        print(f"Report written   : {dest_html}")
        print(f"Structured data  : {suite_dir / 'structured_reports'}")

    return dest_html


def cmd_list(args: argparse.Namespace) -> int:
    """List all available suite runs."""
    suites = find_suite_dirs(ARTIFACTS_DIR)
    if not suites:
        print(f"No suite runs found in: {ARTIFACTS_DIR}")
        return 0

    print(f"{'Suite':<40}  {'Runs':>5}  {'Status'}")
    print("-" * 70)
    for suite in suites:
        run_count = len(find_run_reports(suite))
        summary_path = suite / "suite_summary.json"
        status = "complete" if summary_path.exists() else "in-progress / incomplete"
        if summary_path.exists():
            try:
                sm = _read_json(summary_path)
                passed = sum(1 for r in sm.get("results", []) if r.get("status") == "PASSED")
                failed = sum(1 for r in sm.get("results", []) if r.get("status") != "PASSED")
                status = f"complete  PASSED={passed} FAILED={failed}"
            except Exception:
                pass
        print(f"{suite.name:<40}  {run_count:>5}  {status}")
    return 0


def cmd_fetch(args: argparse.Namespace) -> int:
    """Regenerate HTML report from an existing suite directory."""
    if args.suite_dir:
        suite_dir = Path(args.suite_dir).resolve()
    else:
        suites = find_suite_dirs(ARTIFACTS_DIR)
        completed = [s for s in suites if (s / "suite_summary.json").exists()]
        if completed:
            suite_dir = completed[0]
            print(f"Using most recent completed suite: {suite_dir.name}")
        elif suites:
            suite_dir = suites[0]
            print(f"No completed suite found. Using most recent (may be in-progress): {suite_dir.name}")
        else:
            print(f"ERROR: No suite runs found in {ARTIFACTS_DIR}", file=sys.stderr)
            print("Run the suite first with start_unattended_suite.ps1 or run_dataset_suite.py.", file=sys.stderr)
            return 1

    out_html = Path(args.out_html).resolve() if args.out_html else None

    try:
        report_path = regenerate_report(suite_dir, out_html=out_html, verbose=True)
    except (FileNotFoundError, RuntimeError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    if args.open:
        _open_in_browser(report_path)

    return 0


def _open_in_browser(path: Path) -> None:
    """Open a file in the system default browser."""
    url = path.as_uri()
    if sys.platform == "win32":
        os.startfile(str(path))
    elif sys.platform == "darwin":
        subprocess.run(["open", url], check=False)
    else:
        subprocess.run(["xdg-open", url], check=False)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Fetch results from a completed BatchBuilder suite run and regenerate the HTML report.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    sub = parser.add_subparsers(dest="command")

    # -- list subcommand --
    sub.add_parser("list", help="List all available suite runs.")

    # -- fetch subcommand (default) --
    fetch_p = sub.add_parser("fetch", help="Regenerate HTML from a suite directory (default).")
    fetch_p.add_argument(
        "--suite-dir", default="",
        help="Path to a specific suite_<timestamp> directory. "
             "Defaults to the most recent completed suite.",
    )
    fetch_p.add_argument(
        "--out-html", default="",
        help="Override the output HTML file path. "
             "Defaults to <suite-dir>/shareable_report.html.",
    )
    fetch_p.add_argument(
        "--open", action="store_true",
        help="Open the HTML report in the default browser after generating.",
    )

    # Allow top-level flags as shorthand (no subcommand required).
    parser.add_argument("--list", action="store_true", help="List available suite runs.")
    parser.add_argument("--suite-dir", default="", help="Suite directory for regeneration.")
    parser.add_argument("--out-html", default="", help="Override output HTML path.")
    parser.add_argument("--open", action="store_true", help="Open report in browser after generating.")

    args = parser.parse_args()

    # Route to the correct handler.
    if args.command == "list" or args.list:
        return cmd_list(args)

    # Default action: fetch/regenerate.
    return cmd_fetch(args)


if __name__ == "__main__":
    raise SystemExit(main())
