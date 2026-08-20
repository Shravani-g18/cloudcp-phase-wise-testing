#!/usr/bin/env python3
"""Phase 4 (Reporting & Verification) LIVE test runner.

Implements the live end-to-end flow from cloud_cp_report_test_case_plan.md:

    datagen -> upload/download transfer -> report ZIP download -> parse -> verify

Every RT-* case lives under live_cases/ (CASE_ID, DESCRIPTION, STEPS,
run(ctx, out_dir) -> (status, details)) so adding a new live case never
requires touching this file.

This runner is separate from verify_and_report.py (which still drives the
synthetic P4-01..P4-08 fixtures against the reference merge-join engine in
report_engine.py, unchanged). Both scripts share report_engine.py's parsing
helpers.

Usage:
    run_report_tests.py --list
    run_report_tests.py --all
    run_report_tests.py --from RT-01 --to RT-05
    run_report_tests.py --one RT-03
    run_report_tests.py --manual RT-03
    run_report_tests.py --dry-run --all
    run_report_tests.py --config alt.json --all
    run_report_tests.py --no-cleanup --one RT-01
    run_report_tests.py --no-datagen --one RT-01
    run_report_tests.py --no-transfer --transfer-id 89 --one RT-01

Exit codes: 0 = all selected cases passed (or PASS_WITH_CLEANUP_FAILURE);
            1 = at least one case failed/errored; 2 = configuration/setup error.
"""
from __future__ import annotations

import argparse
import datetime
import html
import importlib.util
import json
import sys
import webbrowser
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
LIVE_CASES_DIR = SCRIPT_DIR / "live_cases"
sys.path.insert(0, str(SCRIPT_DIR))

import bryck_client as bc  # noqa: E402
import live_common as lc  # noqa: E402

PASS_LIKE_STATUSES = {"PASS", "PASS_WITH_CLEANUP_FAILURE"}
FAIL_LIKE_STATUSES = {
    "FAIL", "SETUP_ERROR", "TRANSFER_FAILED", "TIMEOUT",
    "REPORT_DOWNLOAD_ERROR", "REPORT_PARSE_ERROR",
}
STATUS_COLORS = {
    "PASS": "#2e7d32", "PASS_WITH_CLEANUP_FAILURE": "#f9a825",
    "FAIL": "#c62828", "SETUP_ERROR": "#c62828", "TRANSFER_FAILED": "#c62828",
    "TIMEOUT": "#ef6c00", "REPORT_DOWNLOAD_ERROR": "#c62828", "REPORT_PARSE_ERROR": "#c62828",
}


# ---------------------------------------------------------------------------
# Case discovery (RT-* plugins under live_cases/)
# ---------------------------------------------------------------------------
def discover_cases():
    """Import every live_cases/rt_*.py file; return dict CASE_ID -> module."""
    cases = {}
    for file in sorted(LIVE_CASES_DIR.glob("rt_*.py")):
        spec = importlib.util.spec_from_file_location(file.stem, file)
        module = importlib.util.module_from_spec(spec)
        try:
            spec.loader.exec_module(module)
        except Exception as exc:  # noqa: BLE001 - one bad plugin shouldn't kill the run
            print(f"Warning: could not load case file {file.name}: {exc}", file=sys.stderr)
            continue
        cid = getattr(module, "CASE_ID", None)
        run_fn = getattr(module, "run", None)
        desc = getattr(module, "DESCRIPTION", None)
        if not (cid and desc and callable(run_fn)):
            print(f"Warning: {file.name} missing CASE_ID/DESCRIPTION/run(), skipped",
                  file=sys.stderr)
            continue
        if cid in cases:
            print(f"Warning: duplicate CASE_ID {cid} in {file.name}, keeping first", file=sys.stderr)
            continue
        cases[cid] = module
    return cases


def ordered_case_ids(cases):
    return sorted(cases.keys(), key=lambda c: int(c.split("-")[1]))


def select_case_ids(args, cases):
    all_ids = ordered_case_ids(cases)
    if args.list:
        return all_ids
    if args.one:
        if args.one not in cases:
            raise SystemExit(f"Unknown case ID: {args.one}")
        return [args.one]
    if args.manual:
        if args.manual not in cases:
            raise SystemExit(f"Unknown case ID: {args.manual}")
        return [args.manual]
    if args.from_case or args.to_case:
        start = args.from_case or all_ids[0]
        end = args.to_case or all_ids[-1]
        if start not in cases or end not in cases:
            raise SystemExit("--from/--to must reference known case IDs")
        i0, i1 = all_ids.index(start), all_ids.index(end)
        if i1 < i0:
            raise SystemExit("--to must not come before --from")
        return all_ids[i0:i1 + 1]
    if args.all:
        return all_ids
    raise SystemExit("Specify one of --all, --from/--to, --one, --manual, or --list")


# ---------------------------------------------------------------------------
# Manual mode
# ---------------------------------------------------------------------------
def run_manual(cid, module):
    steps = list(getattr(module, "STEPS", []))
    print(f"\n[{cid}] {module.DESCRIPTION} - manual walkthrough ({len(steps)} steps)")
    for i, step in enumerate(steps, 1):
        while True:
            answer = input(f"[{cid}  Step {i}/{len(steps)}] {step}\n"
                            f"  Press Enter to continue, 's' to skip, 'q' to quit: ").strip().lower()
            if answer in ("", "s", "q"):
                break
        if answer == "q":
            print("Manual walkthrough aborted by user.")
            return
    print(f"Manual walkthrough of {cid} complete. Re-run without --manual to execute automatically.\n")


# ---------------------------------------------------------------------------
# Console + artifact output
# ---------------------------------------------------------------------------
RULE = "=" * 70


def print_case_result(cid, desc, status, details, steps=None):
    print(RULE)
    print(f" {cid}  {desc}")
    print(f" Result: {status}")
    if steps:
        print("-" * 70)
        print(" Steps performed:")
        for i, step in enumerate(steps, 1):
            print(f"   {i}. {step}")
    print("-" * 70)
    for key, val in details.items():
        print(f"   {key.replace('_', ' '):<28} {val}")
    print(RULE + "\n")


def write_case_artifacts(case_dir, cid, desc, steps, status, details):
    case_dir = Path(case_dir)
    case_dir.mkdir(parents=True, exist_ok=True)
    (case_dir / "assertions.json").write_text(
        json.dumps({"case_id": cid, "status": status, "details": details}, indent=2, default=str),
        encoding="utf-8",
    )
    lines = [f"{cid} - {desc}", "=" * 60, f"Result: {status}", ""]
    if steps:
        lines.append("Steps performed:")
        lines.extend(f"  {i}. {s}" for i, s in enumerate(steps, 1))
        lines.append("")
    lines.append("Details:")
    for key, val in details.items():
        lines.append(f"  {key.replace('_', ' ')}: {val}")
    (case_dir / "README.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")


def print_summary(all_results, out_root, exit_code):
    print("SUMMARY")
    print("-" * 70)
    print(f" {'CASE':<8}{'DESCRIPTION':<40}{'RESULT'}")
    print("-" * 70)
    passed = 0
    for cid, r in all_results.items():
        print(f" {cid:<8}{r['description']:<40}{r['status']}")
        if r["status"] in PASS_LIKE_STATUSES:
            passed += 1
    print("-" * 70)
    print(f" {passed}/{len(all_results)} cases passed")
    print(f" Output dir: {out_root}")
    print(f" Exit code: {exit_code}")


# ---------------------------------------------------------------------------
# HTML final report
# ---------------------------------------------------------------------------
def _html_detail_value(value):
    if isinstance(value, dict):
        if not value:
            return "-"
        return "<br>".join(
            f"{html.escape(str(k))}: {html.escape(str(v))}" for k, v in value.items()
        )
    if isinstance(value, (list, tuple, set)):
        return html.escape(", ".join(str(v) for v in value)) or "-"
    return html.escape(str(value))


def write_html_report(all_results, selected, out_root):
    """Render a single self-contained report.html: summary table + one
    expandable section per case with its steps and full details dict."""
    generated_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    passed = sum(1 for r in all_results.values() if r["status"] in PASS_LIKE_STATUSES)
    total = len(all_results)

    summary_rows = []
    sections = []
    for cid in selected:
        r = all_results[cid]
        color = STATUS_COLORS.get(r["status"], "#616161")
        desc = html.escape(r["description"])
        summary_rows.append(
            f'<tr><td><a href="#{cid}">{cid}</a></td><td>{desc}</td>'
            f'<td><span class="badge" style="background:{color}">{r["status"]}</span></td></tr>'
        )
        steps_html = ""
        if r.get("steps"):
            items = "".join(f"<li>{html.escape(s)}</li>" for s in r["steps"])
            steps_html = f"<ol>{items}</ol>"
        detail_rows = "".join(
            f"<tr><th>{html.escape(k.replace('_', ' '))}</th><td>{_html_detail_value(v)}</td></tr>"
            for k, v in r["details"].items()
        )
        sections.append(f'''
        <section id="{cid}">
          <h2>{cid} - {desc}
            <span class="badge" style="background:{color}">{r['status']}</span></h2>
          {steps_html}
          <table class="detail">{detail_rows}</table>
        </section>''')

    doc = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>CloudCP Phase 4 Report - {generated_at}</title>
<style>
 body {{ font-family: 'Segoe UI', Arial, sans-serif; margin: 24px; color: #222; }}
 h1 {{ margin-bottom: 4px; }}
 .meta {{ color: #666; margin-bottom: 20px; }}
 table {{ border-collapse: collapse; width: 100%; margin-bottom: 20px; }}
 th, td {{ border: 1px solid #ddd; padding: 6px 10px; text-align: left; vertical-align: top; }}
 table.detail th {{ background: #f5f5f5; width: 240px; }}
 .badge {{ color: #fff; padding: 2px 10px; border-radius: 10px; font-size: 0.85em; }}
 section {{ margin-bottom: 32px; border-top: 1px solid #eee; padding-top: 12px; }}
 a {{ color: #1565c0; text-decoration: none; }}
</style></head>
<body>
<h1>CloudCP Phase 4 - Reporting &amp; Verification (Live) Report</h1>
<div class="meta">Generated {generated_at} &middot; {passed}/{total} cases passed</div>
<table>
 <thead><tr><th>Case</th><th>Description</th><th>Result</th></tr></thead>
 <tbody>{''.join(summary_rows)}</tbody>
</table>
{''.join(sections)}
</body></html>
"""
    path = Path(out_root) / "report.html"
    path.write_text(doc, encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sel = p.add_mutually_exclusive_group()
    sel.add_argument("--all", action="store_true", help="Run all RT-xx cases in order")
    sel.add_argument("--one", metavar="ID", help="Run exactly one case, e.g. RT-03")
    sel.add_argument("--manual", metavar="ID", help="Print steps and wait for manual confirmation")
    sel.add_argument("--list", action="store_true", help="List discovered cases and exit")
    p.add_argument("--from", dest="from_case", metavar="ID", help="Start of an inclusive range")
    p.add_argument("--to", dest="to_case", metavar="ID", help="End of an inclusive range")
    p.add_argument("--dry-run", action="store_true",
                    help="Print the plan (which cases + specs) without running them")
    p.add_argument("--config", default=None, help="Alternate config.json (default: ./config.json)")
    p.add_argument("--out", default=None, help="Output dir (default: reports/run_<timestamp>/)")
    p.add_argument("--no-cleanup", action="store_true", dest="no_cleanup",
                    help="Skip cleanup (leave data and S3 objects for inspection)")
    p.add_argument("--no-datagen", action="store_true", dest="no_datagen",
                    help="Skip datagen (data assumed already present)")
    p.add_argument("--no-transfer", action="store_true", dest="no_transfer",
                    help="Skip transfer (use --transfer-id to supply an existing one)")
    p.add_argument("--transfer-id", default=None, dest="transfer_id",
                    help="Parse + verify a specific already-completed transfer")
    p.add_argument("--open", action="store_true",
                    help="Open the generated report.html in the default browser when the run finishes")
    p.add_argument("--verbose", "-v", action="store_true")
    return p.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    cases = discover_cases()
    if not cases:
        print("No RT-* cases found under live_cases/.", file=sys.stderr)
        return 2

    if args.list:
        for cid in ordered_case_ids(cases):
            print(f"{cid:<8}{cases[cid].DESCRIPTION}")
        return 0

    try:
        selected = select_case_ids(args, cases)
    except SystemExit as exc:
        print(str(exc), file=sys.stderr)
        return 2

    if args.dry_run:
        print("DRY RUN - would execute the following cases in order:")
        for cid in selected:
            print(f"  {cid}: {cases[cid].DESCRIPTION}")
        return 0

    if args.manual:
        run_manual(args.manual, cases[args.manual])
        return 0

    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_root = Path(args.out) if args.out else SCRIPT_DIR / "reports" / f"run_{ts}"
    out_root.mkdir(parents=True, exist_ok=True)

    try:
        cfg = bc.load_config(args.config)
        ctx = lc.build_context(args, cfg=cfg)
    except bc.LiveClientError as exc:
        print(f"Configuration/setup error: {exc}", file=sys.stderr)
        return 2

    all_results = {}
    try:
        for cid in selected:
            module = cases[cid]
            case_dir = out_root / cid
            steps = list(getattr(module, "STEPS", []))
            try:
                status, details = module.run(ctx, case_dir)
            except Exception as exc:  # noqa: BLE001 - one case shouldn't crash the whole run
                status, details = "SETUP_ERROR", {"error": f"unhandled exception: {exc}"}
            write_case_artifacts(case_dir, cid, module.DESCRIPTION, steps, status, details)
            print_case_result(cid, module.DESCRIPTION, status, details, steps)
            all_results[cid] = {"description": module.DESCRIPTION, "status": status,
                                 "details": details, "steps": steps}
    finally:
        ctx.close()

    (out_root / "results.json").write_text(
        json.dumps(all_results, indent=2, default=str), encoding="utf-8"
    )
    html_path = write_html_report(all_results, selected, out_root)

    any_failed = any(r["status"] in FAIL_LIKE_STATUSES for r in all_results.values())
    exit_code = 1 if any_failed else 0
    print_summary(all_results, out_root, exit_code)
    print(f" HTML report:  {html_path}")
    if args.open:
        webbrowser.open(html_path.resolve().as_uri())
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
