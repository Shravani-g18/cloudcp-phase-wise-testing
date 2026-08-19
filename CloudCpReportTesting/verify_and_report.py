#!/usr/bin/env python3
"""Phase 4 (Reporting & Verification) automation — plugin-based orchestrator.

Every test case lives as its own file under cases/ (CASE_ID, DESCRIPTION,
run(out_dir) -> (passed, details)). This script only discovers and runs them,
so adding a brand-new case (P4-09, or any future case) never requires
touching this file — just drop a new module into cases/.

Each case plugin:
  1. builds a synthetic source.index + upload-report fixture,
  2. runs it through the reference merge-join verification engine in
     report_engine.py (the algorithm documented in
     ../docs/bcloud_redesign_proposal.md §5 and ../docs/bcloud_final_design.md
     §16 — used here because no live transfer / real engine binary can be
     invoked in this environment),
  3. asserts its own pass criteria against the resulting final_report.

Everything runs in an isolated output directory. Nothing here reads or writes
/etc/bryck/... or any other phase's files. Reports (final_report.csv,
final_report_summary.txt, results.json) are always written and kept on disk
by default under reports/run_<timestamp>/ - pass --cleanup to opt into
deleting them after the run.

Usage:
    verify_and_report.py --list
    verify_and_report.py --all [--dry-run]
    verify_and_report.py --case P4-01,P4-05 [--out DIR] [--cleanup]
"""
import argparse
import datetime
import importlib.util
import json
import os
import subprocess
import sys
import shutil
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
CASES_DIR = SCRIPT_DIR / "cases"
sys.path.insert(0, str(SCRIPT_DIR))  # so case plugins can `import report_engine`


def discover_cases():
    """Import every cases/*.py file and collect (CASE_ID, DESCRIPTION, STEPS, run, check_live).

    A file is skipped (with a warning) if it doesn't define CASE_ID/DESCRIPTION/run,
    so one broken/incomplete new case can't crash the whole run. STEPS is optional
    (defaults to []) - used for manual-review documentation in reports/console.
    check_live(source_entries, report_rows, results, out_dir) is also optional - only
    cases with generic, data-agnostic assertions define it, so they double as live-mode
    checks against a real transfer's reconciliation results with zero changes here.
    """
    cases, funcs, steps, live_funcs = {}, {}, {}, {}
    for file in sorted(CASES_DIR.glob("*.py")):
        if file.name.startswith("_"):
            continue
        spec = importlib.util.spec_from_file_location(file.stem, file)
        module = importlib.util.module_from_spec(spec)
        try:
            spec.loader.exec_module(module)
        except Exception as exc:  # noqa: BLE001 - one bad plugin shouldn't kill the run
            print(f"Warning: could not load case file {file.name}: {exc}", file=sys.stderr)
            continue
        cid = getattr(module, "CASE_ID", None)
        desc = getattr(module, "DESCRIPTION", None)
        run_fn = getattr(module, "run", None)
        if not (cid and desc and callable(run_fn)):
            print(f"Warning: {file.name} is missing CASE_ID/DESCRIPTION/run(), skipped",
                  file=sys.stderr)
            continue
        if cid in cases:
            print(f"Warning: duplicate CASE_ID {cid} in {file.name}, keeping first", file=sys.stderr)
            continue
        cases[cid] = desc
        funcs[cid] = run_fn
        steps[cid] = list(getattr(module, "STEPS", []))
        # check_live is optional: only cases whose assertions are data-agnostic
        # (structural, not hardcoded expected counts) can run against real transfers
        live_fn = getattr(module, "check_live", None)
        if callable(live_fn):
            live_funcs[cid] = live_fn
    return cases, funcs, steps, live_funcs



# ---------------------------------------------------------------------------
# Human-readable console output
# ---------------------------------------------------------------------------
RULE = "=" * 66


def _fmt_value(v):
    if isinstance(v, bool):
        return "yes" if v else "no"
    if isinstance(v, dict):
        return ", ".join(f"{k}={v2}" for k, v2 in v.items())
    return str(v)


def print_case_result(cid, desc, passed, details, steps=None):
    label = "PASS" if passed else "FAIL"
    print(RULE)
    print(f" {cid}  {desc}")
    print(f" Result: {label}")
    if steps:
        print("-" * 66)
        print(" Steps performed:")
        for i, step in enumerate(steps, 1):
            print(f"   {i}. {step}")
    print("-" * 66)
    for key, val in details.items():
        print(f"   {key.replace('_', ' '):<24} {_fmt_value(val)}")
    print(RULE + "\n")


def write_case_readme(case_dir, cid, desc, steps, passed, details):
    """Write a per-case README.txt next to final_report.csv - description, exact
    steps performed, and the result - so a reviewer can validate manually from the
    reports folder alone, without reading the script or any external doc."""
    label = "PASS" if passed else "FAIL"
    lines = [f"{cid} - {desc}", "=" * 60, f"Result: {label}", ""]
    if steps:
        lines.append("Steps performed:")
        lines.extend(f"  {i}. {s}" for i, s in enumerate(steps, 1))
        lines.append("")
    lines.append("Details:")
    for key, val in details.items():
        lines.append(f"  {key.replace('_', ' ')}: {_fmt_value(val)}")
    (Path(case_dir) / "README.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")


def print_inline_report(details):
    """Print final_report_summary.txt content right away so results are
    visible immediately, without opening the CSV separately."""
    report_path = details.get("final_report")
    if not report_path:
        return
    summary_path = Path(report_path).with_name("final_report_summary.txt")
    if summary_path.exists():
        print("   final_report_summary.txt:")
        for line in summary_path.read_text(encoding="utf-8").splitlines():
            print(f"     {line}")
        print()


def open_path(path):
    """Launch a file/folder in the OS default viewer (best-effort, non-fatal)."""
    try:
        if sys.platform.startswith("win"):
            os.startfile(path)  # noqa: S606 - opening our own generated report
        elif sys.platform == "darwin":
            subprocess.run(["open", str(path)], check=False)
        else:
            subprocess.run(["xdg-open", str(path)], check=False)
    except OSError as exc:
        print(f" (could not auto-open {path}: {exc})")


def print_summary(all_results, out_root, exit_code):
    print("SUMMARY")
    print("-" * 66)
    print(f" {'CASE':<8}{'DESCRIPTION':<42}{'RESULT'}")
    print("-" * 66)
    passed_count = 0
    for cid, r in all_results.items():
        mark = "PASS" if r["passed"] else "FAIL"
        passed_count += int(r["passed"])
        print(f" {cid:<8}{r['description']:<42}{mark}")
    print("-" * 66)
    print(f" {passed_count}/{len(all_results)} cases passed")
    print(f" Output dir: {out_root}")
    print(f" Exit code: {exit_code}  "
          f"({'all passed' if exit_code == 0 else 'one or more cases failed'})")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sel = p.add_mutually_exclusive_group()
    sel.add_argument("--case", help="Comma-separated case IDs, e.g. P4-01,P4-05")
    sel.add_argument("--all", action="store_true", help="Run every case found under cases/")
    sel.add_argument("--list", action="store_true", help="List discovered cases and exit")
    p.add_argument("--dry-run", action="store_true",
                    help="Print the plan (which cases + fixtures) without running them")
    p.add_argument("--out", help="Output dir for fixtures/reports "
                                   "(default: reports/run_<timestamp>/, always kept)")
    p.add_argument("--report", help="Consolidated JSON filename inside the output dir "
                                     "(default: results.json - always written)")
    p.add_argument("--cleanup", action="store_true",
                    help="Delete the output dir after the run (opt-in; reports are kept by default)")
    p.add_argument("--open", action="store_true",
                    help="Auto-open each case's final_report.csv in the OS default app "
                         "as soon as it's produced")
    p.add_argument("--verbose", "-v", action="store_true")

    live = p.add_argument_group("live mode (real transfer, read-only inputs)")
    live.add_argument("--live", action="store_true",
                       help="Reconcile a real transfer's source.index + upload report "
                            "instead of running synthetic cases")
    live.add_argument("--dir",
                       help="Shortcut: transfer's log dir (cloud_transfer_<id>/). Auto-finds "
                            "source.index, report/upload_report.*.csv, and manifest.json "
                            "inside it - use this instead of the 3 flags below")
    live.add_argument("--transfer-id", help="Real transfer id (used for labeling/output naming)")
    live.add_argument("--source-index", help="Path to the real source.index CSV (read-only)")
    live.add_argument("--report-shards",
                       help="Comma-separated upload_report/txhistory CSV shard paths, "
                            "oldest-first (read-only)")
    live.add_argument("--manifest",
                       help="Optional manifest.json with scan_state/pause_requested "
                            "to enforce the same ordering guards as P4-02/P4-03 (read-only)")
    return p.parse_args(argv)


def resolve_live_args(args):
    """Fill in --source-index/--report-shards/--manifest/--transfer-id from --dir
    if given, using the standard layout from bcloud_final_design.md §5. Explicit
    flags always win over what --dir would auto-discover."""
    if not args.dir:
        return
    base = Path(args.dir)
    if not args.transfer_id:
        # cloud_transfer_<id> -> <id>
        args.transfer_id = base.name.replace("cloud_transfer_", "") or base.name
    if not args.source_index:
        candidate = base / "source.index"
        if candidate.exists():
            args.source_index = str(candidate)
    if not args.report_shards:
        shards = sorted(str(p) for p in (base / "report").glob("upload_report.*.csv"))
        transfer_report = base / f"transfer_report_{args.transfer_id}.csv"
        if transfer_report.exists():
            shards.insert(0, str(transfer_report))
        if shards:
            args.report_shards = ",".join(shards)
    if not args.manifest:
        candidate = base / "manifest.json"
        if candidate.exists():
            args.manifest = str(candidate)


def run_live(args, out_root, cases, case_steps, live_funcs):
    """Reconcile a real transfer's source.index vs. upload report, read-only.
    Never writes into the transfer's own log directory - only into out_root.
    Also runs every discovered case's check_live() (if defined) against the
    real results, so new cases picked up by discover_cases() automatically
    apply to live transfers with zero changes to this function."""
    from report_engine import (load_source_index, load_upload_report_rows,
                                read_transfer_state, verify, write_final_report,
                                VerificationRefused)

    resolve_live_args(args)
    if not args.source_index or not args.report_shards:
        print("--live requires --dir (or --source-index + --report-shards)", file=sys.stderr)
        return 2

    shard_paths = [s.strip() for s in args.report_shards.split(",")]
    scan_state, pause_requested = "complete", False
    if args.manifest:
        scan_state, pause_requested = read_transfer_state(args.manifest)

    if args.dry_run:
        print("Plan (dry-run, nothing read):")
        print(f"  transfer id      : {args.transfer_id or '(none given)'}")
        print(f"  source.index     : {args.source_index}")
        print(f"  report shards    : {shard_paths}")
        print(f"  manifest         : {args.manifest or '(none - assumes scan_state=complete)'}")
        print(f"  live case checks : {', '.join(live_funcs) or '(none discovered)'}")
        return 0

    print(f"Reconciling live transfer {args.transfer_id or '(unlabeled)'}...")
    try:
        source_entries = load_source_index(args.source_index)
        report_rows = load_upload_report_rows(shard_paths)
        results = verify(source_entries, report_rows,
                          scan_state=scan_state, pause_requested=pause_requested)
    except VerificationRefused as e:
        print(f"Verification refused: {e}", file=sys.stderr)
        return 1
    except (OSError, ValueError) as e:
        print(f"Error reading live inputs: {e}", file=sys.stderr)
        return 2

    out_root.mkdir(parents=True, exist_ok=True)
    path = write_final_report(results, out_root)
    print_inline_report({"final_report": str(path)})
    if args.open:
        open_path(path)

    any_case_failed = False
    case_results = {}
    if live_funcs:
        print(f"\nRunning {len(live_funcs)} case check(s) against the real results...\n")
        for cid, live_fn in live_funcs.items():
            case_dir = out_root / cid.replace("-", "_")
            case_dir.mkdir(parents=True, exist_ok=True)
            try:
                passed, details = live_fn(source_entries, report_rows, results, case_dir)
            except Exception as exc:  # noqa: BLE001 - one bad case shouldn't kill the run
                passed, details = False, {"error": str(exc)}
            case_results[cid] = {"description": cases[cid], "steps": case_steps.get(cid, []),
                                  "passed": passed, "details": details}
            any_case_failed = any_case_failed or not passed
            print_case_result(cid, cases[cid], passed, details, case_steps.get(cid, []))
            write_case_readme(case_dir, cid, cases[cid], case_steps.get(cid, []), passed, details)

    report_path = out_root / (args.report or "results.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump({"reconciliation_final_report": str(path), "case_checks": case_results},
                   f, indent=2, default=str)

    print(f"All reports kept at: {out_root}")
    return 1 if any_case_failed else 0


def main(argv=None):
    args = parse_args(argv)
    cases, case_funcs, case_steps, live_funcs = discover_cases()

    if args.live:
        resolve_live_args(args)
        stamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        out_root = (Path(args.out) if args.out else
                    SCRIPT_DIR / "reports" / f"live_{args.transfer_id or 'unlabeled'}_{stamp}")
        return run_live(args, out_root, cases, case_steps, live_funcs)

    if args.list or (not args.case and not args.all):
        print(f"Available cases ({len(cases)} discovered under cases/):")
        for cid, desc in cases.items():
            live_tag = " [also runs in --live mode]" if cid in live_funcs else ""
            print(f"  {cid}  {desc}{live_tag}")
            for step in case_steps.get(cid, []):
                print(f"        - {step}")
        if not args.case and not args.all:
            return 0
        return 0

    case_ids = list(cases) if args.all else [c.strip() for c in args.case.split(",")]
    unknown = [c for c in case_ids if c not in case_funcs]
    if unknown:
        print(f"Unknown case(s): {', '.join(unknown)}. Run --list to see what's available.",
              file=sys.stderr)
        return 2

    if args.dry_run:
        print("Plan (dry-run, nothing executed):")
        for cid in case_ids:
            print(f"  would build fixture + verify + assert -> {cid}: {cases[cid]}")
            for step in case_steps.get(cid, []):
                print(f"        - {step}")
        return 0

    if args.out:
        out_root = Path(args.out)
    else:
        stamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        out_root = SCRIPT_DIR / "reports" / f"run_{stamp}"
    out_root.mkdir(parents=True, exist_ok=True)

    all_results = {}
    any_failed = False
    print(f"Running {len(case_ids)} case(s)...\n")
    for cid in case_ids:
        case_dir = out_root / cid.replace("-", "_")
        case_dir.mkdir(parents=True, exist_ok=True)
        try:
            passed, details = case_funcs[cid](case_dir)
        except Exception as exc:  # noqa: BLE001 - report, don't crash the whole run
            passed, details = False, {"error": str(exc)}
        all_results[cid] = {"description": cases[cid], "steps": case_steps.get(cid, []),
                             "passed": passed, "details": details}
        any_failed = any_failed or not passed
        if args.verbose:
            print_case_result(cid, cases[cid], passed, details, case_steps.get(cid, []))
        else:
            mark = "PASS" if passed else "FAIL"
            print(f" [{mark}] {cid}  {cases[cid]}")
        print_inline_report(details)
        write_case_readme(case_dir, cid, cases[cid], case_steps.get(cid, []), passed, details)
        if args.open and details.get("final_report"):
            open_path(details["final_report"])

    print_summary(all_results, out_root, exit_code=1 if any_failed else 0)

    # Consolidated JSON is always written - reports are mandatory, not opt-in.
    report_path = out_root / (args.report or "results.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2, default=str)
    print(f"\n Report JSON written -> {report_path}")

    if args.cleanup:
        shutil.rmtree(out_root, ignore_errors=True)
        print(" (--cleanup passed: output dir removed after reporting)")
    else:
        print(f" All reports kept at: {out_root}")

    return 1 if any_failed else 0


if __name__ == "__main__":
    sys.exit(main())
