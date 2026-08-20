#!/usr/bin/env python3
"""
Cloud Transfer + Bryck Management Negative Test Suite — master-flow runner.

Standalone orchestrator. It does NOT reimplement any operation and does NOT
modify any existing script. Everything it does goes through:

  * cloud_transfer_test_runner.TestContext        — one call per existing
    bryck_*.py / change_ip.py / change_time.py script (login, format,
    mount, eject, cloud configure/deconfigure, transfer initiate/pause/
    resume/cancel/report, dataset generation, bryck_info, ...).
  * negative_environment_runner.EnvironmentManager / dispatch()  — the full
    negative catalog described in NEGATIVE_TEST_PLAN.md (CLI, AUTH, TID,
    AWS, PATH, LIFE, DATA, XFER, DOWNLOAD, STATE, RACE, DUP, REPORT, FAULT,
    REC, VERIFY, INT, CLEAN, MGMT).
  * negative_environment_runner.build_html()      — the SAME HTML report
    renderer used by the existing negative runner, so this report belongs
    to the same framework (same tables, badges, PASS/FAIL/BLOCKED styling,
    expandable command/detail rows, summary counters).

The only new logic here is the two end-to-end P0 "master flow" narratives
(upload and download) from the negative-test specification: IP change ->
eject -> format -> mount -> configure -> transfer -> pause/resume -> attempt
destructive/lifecycle operations during an active transfer -> completion ->
report -> cleanup. Each step is recorded as its own TestResult so it renders
in the shared HTML report exactly like a catalog case.

Usage
-----
    python3 cloud_transfer_negative_test_runner.py                    (--all, dry-run)
    python3 cloud_transfer_negative_test_runner.py --all --live
    python3 cloud_transfer_negative_test_runner.py --upload --live
    python3 cloud_transfer_negative_test_runner.py --download --live
    python3 cloud_transfer_negative_test_runner.py --static
    python3 cloud_transfer_negative_test_runner.py --concurrency --live
    python3 cloud_transfer_negative_test_runner.py --recovery --live --allow-service-faults
    python3 cloud_transfer_negative_test_runner.py --upload --live --confirm-destructive
"""
from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import json
import re
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import cloud_transfer_test_runner as ctr   # noqa: E402  existing, unmodified
import negative_environment_runner as ner  # noqa: E402  existing, unmodified
from session import ApiSession               # noqa: E402  existing, unmodified
from ssh_runner import SshRunner, SshRunnerError  # noqa: E402  existing, unmodified

SCRIPT_DIR = ctr.SCRIPT_DIR
DEFAULT_REMOTE_REPORT_DIR = "/opt/bryck/bryckapi/downloads/reports"

STATIC_SECTIONS = ["CLI", "AUTH", "TID", "AWS", "PATH", "LIFE", "DATA"]
CONCURRENCY_SECTIONS = ["RACE", "DUP"]
RECOVERY_SECTIONS = ["FAULT", "REC", "VERIFY", "INT", "SVC"]
ALL_HANDLED_SECTIONS = set(STATIC_SECTIONS) | set(CONCURRENCY_SECTIONS) | set(RECOVERY_SECTIONS)


# =============================================================================
# Centralized test registry (single source of truth for ID/name/module/order)
# =============================================================================
# Every test case already dispatched by this file (catalog cases via
# negative_environment_runner.dispatch(), the two P0 master flows, and the
# positive scenario/combination flows in cloud_transfer_test_runner.py) is
# given one stable, human-readable ID/name/module here. This does not
# replace or duplicate any execution logic below -- --test/--tests/--from/
# --to/--module/--modules/--list/--search all resolve to IDs from this same
# registry and then hand off to the unchanged dispatch/run_master_flow/
# scenario functions exactly as --sections/--test-id/--all already did.

MODULE_DESCRIPTIONS = {
    "CLI": "Command-line / input validation", "AUTH": "Authentication and session handling",
    "TID": "Transfer-ID validation", "AWS": "AWS cloud provider configuration",
    "PATH": "AWS bucket / object path handling", "LIFE": "Bryck lifecycle (mount/format/eject/erase/remove)",
    "DATA": "Dataset generation", "XFER": "Upload transfer negative cases",
    "DOWNLOAD": "Download transfer negative cases", "STATE": "Transfer state-transition matrix",
    "RACE": "Concurrency / race-condition cases", "DUP": "Duplicate / repeated operations",
    "REPORT": "Transfer report generation", "FAULT": "API/SSH fault injection",
    "REC": "Service restart / recovery", "VERIFY": "Transfer verification and completion",
    "INT": "Data integrity", "CLEAN": "Final state / cleanup audit", "MGMT": "Device management operations",
    "SVC": "Service fault-injection matrix", "SM": "Excel State Matrix cases", "F": "Excel Combination Flow cases",
    "MASTER": "P0 end-to-end master flows",
    "SCENARIO": "Positive dataset-size scenarios (small/large/million)",
    "COMBO": "Positive combination flows (happy path, priority, monitoring, ...)",
}

# Positive scenario/combination flows already implemented in cloud_transfer_test_runner.py --
# included in the same registry/CLI as the negative catalog instead of requiring a second,
# separate command. Each maps to the exact function already defined there (unmodified).
SCENARIO_FUNCTIONS = {
    "SCENARIO-SMALL": (ctr.run_scenario_small, "Small dataset (~1GB) basic upload/download flow"),
    "SCENARIO-LARGE": (ctr.run_scenario_large, "Large dataset (~500GB) pause/resume/cancel lifecycle"),
    "SCENARIO-MILLION": (ctr.run_scenario_million, "Million-file mixed upload/download stress flow"),
}
COMBO_FUNCTIONS = {
    "COMBO-HAPPY_PATH": (ctr.run_combo_happy_path, "End-to-end happy-path upload with pause/resume/report"),
    "COMBO-PAUSE_RESUME_CANCEL": (ctr.run_combo_pause_resume_cancel, "Pause, resume, then cancel an active upload"),
    "COMBO-PRIORITY": (ctr.run_combo_priority, "Full ~50GB upload/download lifecycle with checkpoints"),
    "COMBO-BOTH_MODE": (ctr.run_combo_both_mode, "Concurrent upload+download ('both' mode) lifecycle"),
    "COMBO-MONITORING": (ctr.run_combo_monitoring, "Read-only monitoring/status/network-info checks"),
    "COMBO-SETTINGS": (ctr.run_combo_settings, "Device date/time configuration change"),
    "COMBO-NEGATIVE": (ctr.run_combo_negative, "Invalid management/cloud-transfer operations sweep"),
}


@dataclasses.dataclass
class TestEntry:
    id: str
    name: str
    module: str
    description: str
    order: int
    kind: str  # "catalog" (ner.dispatch), "master" (run_master_flow*), "scenario" (ctr.run_scenario_*/run_combo_*)


def _module_of(test_id: str) -> str:
    m = re.match(r"[A-Z]+", test_id)
    return m.group(0) if m else "UNKNOWN"


def build_registry() -> dict[str, TestEntry]:
    registry: dict[str, TestEntry] = {}
    order = 0
    for case_id, _heading, description in ctr._negative_plan_entries(ner.PLAN_PATH):
        if not re.fullmatch(r"[A-Z]+-\d+", case_id):
            continue  # skip synthetic PLAN-### rows that have no dispatchable handler
        order += 1
        module = _module_of(case_id)
        registry[case_id] = TestEntry(
            id=case_id, name=description, module=module,
            description=f"{MODULE_DESCRIPTIONS.get(module, module)}: {description}", order=order, kind="catalog",
        )
    for master_id, name, description in [
        ("MASTER-UPLOAD", "P0 Master Upload Flow", "Format -> mount -> configure -> upload -> pause/resume -> completion -> cleanup"),
        ("MASTER-DOWNLOAD", "P0 Master Download Flow", "Format -> mount -> configure -> download -> pause/resume -> completion -> cleanup"),
        ("MASTER-BOTH", "P0 Master Both Flow", "Upload then download in one session with management operations interleaved"),
    ]:
        order += 1
        registry[master_id] = TestEntry(id=master_id, name=name, module="MASTER", description=description, order=order, kind="master")
    for scenario_id, (_fn, description) in {**SCENARIO_FUNCTIONS, **COMBO_FUNCTIONS}.items():
        order += 1
        registry[scenario_id] = TestEntry(
            id=scenario_id, name=description, module=_module_of(scenario_id), description=description,
            order=order, kind="scenario",
        )
    return registry


REGISTRY = build_registry()


def ordered_entries() -> list[TestEntry]:
    return sorted(REGISTRY.values(), key=lambda e: e.order)


def _lookup_id(raw: str) -> str | None:
    """Accept a bare ID or a full "ID - Name" string; return the canonical ID."""
    raw = raw.strip()
    if raw in REGISTRY:
        return raw
    head = raw.split(" - ", 1)[0].strip()
    return head if head in REGISTRY else None


def _print_no_match(query: str) -> None:
    print(f"ERROR: no test matches {query!r}. Closest available tests:")
    ql = query.lower()
    candidates = [e for e in ordered_entries() if ql in e.id.lower() or ql in e.name.lower()]
    for e in (candidates[:15] or ordered_entries()[:15]):
        print(f"  {e.id:<26}{e.module:<10}{e.name}")


def print_list() -> None:
    entries = ordered_entries()
    print(f"{'ID':<26}{'MODULE':<10}{'NAME'}")
    print("-" * 100)
    for e in entries:
        print(f"{e.id:<26}{e.module:<10}{e.name}")
    print(f"\n{len(entries)} test(s) registered across {len({e.module for e in entries})} module(s).")


def print_search(query: str) -> None:
    ql = query.lower()
    matches = [
        e for e in ordered_entries()
        if ql in e.id.lower() or ql in e.name.lower() or ql in e.description.lower() or ql in e.module.lower()
    ]
    if not matches:
        print(f"No tests match {query!r}.")
        return
    print(f"{'ID':<26}{'MODULE':<10}{'NAME'}")
    print("-" * 100)
    for e in matches:
        print(f"{e.id:<26}{e.module:<10}{e.name}")
    print(f"\n{len(matches)} match(es) for {query!r}.")


def _run_scenario_entry(entry_id: str, ctx: "ctr.TestContext", args) -> "ner.TestResult":
    """Run a positive scenario/combination flow and fold its ctr.StepResult sequence into
    one ner.TestResult so it renders in the same unified HTML report as every other case."""
    fn, _description = {**SCENARIO_FUNCTIONS, **COMBO_FUNCTIONS}[entry_id]
    entry = REGISTRY[entry_id]
    steps_before = len(ctx.steps)
    try:
        fn(ctx)
    except Exception as exc:  # noqa: BLE001 - a scenario crashing must not abort the whole selection
        ctx._record(f"UNHANDLED EXCEPTION in {entry_id}", str(fn), -1, "", str(exc), 0.0, notes="runner crashed")
    finally:
        if args.live:
            ctx.cleanup_transfers()
    new_steps = ctx.steps[steps_before:]
    passed = all(s.passed for s in new_steps) if new_steps else False
    status = "PASS" if passed else "FAIL"
    step_lines = [f"[{'PASS' if s.passed else 'FAIL'}] {s.name} (rc={s.returncode}, {s.duration_sec:.1f}s)" for s in new_steps]
    commands = [
        ner.CommandRecord(label=s.name, command=s.command, stdin="", stdout=s.stdout, stderr=s.stderr,
                         rc=s.returncode, duration=s.duration_sec)
        for s in new_steps
    ]
    result = ner.TestResult(
        test_id=entry_id, section=entry.module, name=entry.name, status=status,
        expected=entry.description, actual="\n".join(step_lines) or "(no steps recorded; requires --live)",
        reason="", baseline={}, env_before=None, env_after=None, commands=commands,
        cleanup_status="performed" if args.live else "not required",
        duration=sum(s.duration_sec for s in new_steps),
    )
    label, sentence, _css = ner.classify_outcome(status, expected_failure=False)
    result.outcome_label, result.outcome_sentence = label, sentence
    result.narrative = ner.build_narrative(result)
    print(f"    [{entry_id}] {entry.name} -> {status}")
    return result


def run_scenarios(scenario_ids: list[str], mgr: "ner.EnvironmentManager", args) -> list["ner.TestResult"]:
    return [_run_scenario_entry(sid, mgr.ctx, args) for sid in scenario_ids]


def resolve_name_based_selection(args: argparse.Namespace) -> None:
    """Resolve --test/--tests/--from/--to/--module/--modules/--list/--search against the
    registry, then translate the result into the EXISTING selection attributes
    (args.test_id, args.sections, args.upload/--download/--both) so the unchanged
    dispatch/run_master_flow machinery below needs no further changes. Adds
    args.scenario_ids for the (new) positive scenario/combination flows.

    Leaves args entirely untouched (falls back to the original behavior) when none
    of the new selection flags are used.
    """
    args.scenario_ids = []
    if args.list:
        print_list()
        raise SystemExit(0)
    if args.search:
        print_search(args.search)
        raise SystemExit(0)

    has_test = bool(args.test)
    has_tests = bool(args.tests)
    has_from_to = bool(args.range_from or args.range_to)
    has_module_new = bool(args.module or args.modules)
    if not (has_test or has_tests or has_from_to or has_module_new):
        return  # nothing new requested; fully preserve original behavior

    if (has_test or has_tests) and (has_from_to or has_module_new):
        raise SystemExit("ERROR: --test/--tests cannot be combined with --module/--modules/--from/--to.")

    entries = ordered_entries()
    by_id = {e.id: e for e in entries}
    selected: list[TestEntry] = []

    if has_test:
        tid = _lookup_id(args.test)
        if not tid:
            _print_no_match(args.test)
            raise SystemExit(2)
        selected = [by_id[tid]]
    elif has_tests:
        wanted = [t.strip() for t in args.tests.split(",") if t.strip()]
        missing = [w for w in wanted if not _lookup_id(w)]
        if missing:
            for m in missing:
                _print_no_match(m)
            raise SystemExit(2)
        ids = {_lookup_id(w) for w in wanted}
        selected = [e for e in entries if e.id in ids]
    else:
        scoped = entries
        if has_module_new:
            wanted_modules = {m.strip().upper() for m in (args.modules or args.module).split(",") if m.strip()}
            unknown = wanted_modules - {e.module for e in entries}
            if unknown:
                raise SystemExit(f"ERROR: unknown module(s) {sorted(unknown)}. Known modules: {sorted({e.module for e in entries})}")
            scoped = [e for e in entries if e.module in wanted_modules]
        if has_from_to:
            if not (args.range_from and args.range_to):
                raise SystemExit("ERROR: --from and --to must be given together")
            fid, tid = _lookup_id(args.range_from), _lookup_id(args.range_to)
            if not fid:
                _print_no_match(args.range_from)
                raise SystemExit(2)
            if not tid:
                _print_no_match(args.range_to)
                raise SystemExit(2)
            i0, i1 = sorted((by_id[fid].order, by_id[tid].order))
            scoped_ids = {e.id for e in scoped}
            selected = [e for e in entries if i0 <= e.order <= i1 and e.id in scoped_ids]
        else:
            selected = scoped

    if not selected:
        raise SystemExit("ERROR: selection resolved to zero tests.")

    catalog_ids = [e.id for e in selected if e.kind == "catalog"]
    master_ids = [e.id for e in selected if e.kind == "master"]
    args.scenario_ids = [e.id for e in selected if e.kind == "scenario"]

    # F-01/F-39 and F-02/F-40 are just aliases for the full MASTER-UPLOAD/
    # MASTER-DOWNLOAD flow (per NEGATIVE_TEST_PLAN.md) -- selecting them by
    # name/ID should actually run that flow instead of the "delegates to..."
    # stub, so every ID in the registry is independently runnable.
    upload_alias_ids = {"F-01", "F-39"} & set(catalog_ids)
    download_alias_ids = {"F-02", "F-40"} & set(catalog_ids)
    if upload_alias_ids:
        args.upload = True
        catalog_ids = [cid for cid in catalog_ids if cid not in upload_alias_ids]
    if download_alias_ids:
        args.download = True
        catalog_ids = [cid for cid in catalog_ids if cid not in download_alias_ids]

    if catalog_ids:
        args.test_id = ",".join(catalog_ids)
    args.upload = args.upload or "MASTER-UPLOAD" in master_ids
    args.download = args.download or "MASTER-DOWNLOAD" in master_ids
    args.both = args.both or "MASTER-BOTH" in master_ids
    print(f"Resolved name-based selection -> {len(selected)} test(s): {[e.id for e in selected]}")


# =============================================================================
# P0 master flows (upload / download)
# =============================================================================

def run_master_flow(direction: str, mgr: "ner.EnvironmentManager", args, work: Path) -> list["ner.TestResult"]:
    """Run the P0 master flow end-to-end and record every step as a TestResult.

    direction: "upload" or "download".
    """
    ctx = mgr.ctx
    prefix = "UP" if direction == "upload" else "DL"
    baseline = {"flow": f"P0 master {direction} flow"}
    counter = {"n": 0}
    results: list[ner.TestResult] = []

    def next_id() -> str:
        counter["n"] += 1
        return f"MASTER-{prefix}-{counter['n']:02d}"

    def add(name, expected, sr, env_before=None, env_after=None,
            cleanup_status="not required", cleanup_detail=""):
        tid = next_id()
        result = ner.result_from_step(
            tid, "MASTER", name, baseline, env_before, sr, mgr,
            expected=expected, cleanup_status=cleanup_status,
            cleanup_detail=cleanup_detail, env_after=env_after,
        )
        result.narrative = ner.build_narrative(result)
        results.append(result)
        print(f"    [{tid}] {name} -> {result.status}")
        return result

    def add_blocked(name, reason):
        tid = next_id()
        result = ner.blocked(tid, "MASTER", name, baseline, reason, mgr=mgr)
        result.narrative = ner.build_narrative(result)
        results.append(result)
        print(f"    [{tid}] {name} -> BLOCKED ({reason})")
        return result

    if not args.live:
        add_blocked(f"P0 master {direction} flow", "requires --live against the dedicated Bryck device")
        return results

    # 1. Baseline Bryck info
    env0 = mgr.snapshot(f"master_{direction}:baseline")
    info = mgr.cap(f"master_{direction}:info", ctx.bryck_info(f"{direction} master flow baseline"))
    add("Get Bryck info (baseline)", "Bryck info is queryable.", info, env_before=env0)

    # 2. Eject if mounted, then verify NOT mounted
    if env0["bryck_state"] == " Mounted":
        ej = mgr.cap(f"master_{direction}:eject", ctx.eject_bryck())
        add("Eject Bryck (was mounted)", "Eject succeeds when the Bryck was mounted.", ej)
    verify1 = mgr.cap(f"master_{direction}:verify_unmounted", ctx.bryck_info("verify not mounted before format"))
    verify1 = dataclasses.replace(verify1, passed=ctr._parse_bryck_state(verify1.stdout) in {" Ejected", " Removed"})
    add("Verify Bryck is NOT mounted", "Bryck reports Ejected/Removed before formatting.", verify1)

    # 3. Change P0 IP (network-affecting; gated separately from --confirm-destructive because it can
    # strand the very API/SSH session the runner is using, unlike the other destructive operations).
    if args.confirm_destructive and args.allow_ip_change:
        ip_params = SCRIPT_DIR / "change_ip_params.json"
        ip_sr = mgr.cap(f"master_{direction}:change_ip", ctx.run_py(
            "Change P0 IP", "change_ip.py", "--login", str(ctx.login_json),
            "--params", str(ip_params), timeout=180,
        ))
        add("Change P0 IP", "IP change is applied and accepted.", ip_sr)
        conn = mgr.cap(f"master_{direction}:verify_connectivity", ctx.bryck_info("verify connectivity after IP change"))
        add("Verify connectivity after IP change", "Bryck remains reachable/queryable after the IP change.", conn)
    else:
        add_blocked("Change P0 IP", "requires --confirm-destructive AND --allow-ip-change (changes device network configuration)")

    # 4-5. Format, verify NOT mounted
    fmt = mgr.cap(f"master_{direction}:format", ctx.format_bryck())
    if not fmt.passed:
        # A just-completed eject/IP-change can leave the device in 'Ejecting' for well over
        # a minute (observed up to 75s+ in practice) before format is accepted. A single
        # retry isn't enough -- poll prepare_format()+format_bryck() on a bounded deadline.
        deadline = time.time() + 300
        retry_num = 0
        while not fmt.passed and time.time() < deadline:
            retry_num += 1
            mgr.cap(f"master_{direction}:format_settle_{retry_num}", ctx.prepare_format())
            fmt = mgr.cap(f"master_{direction}:format_retry_{retry_num}", ctx.format_bryck())
    add("Format Bryck", "Format succeeds from an unmounted/ejected state.", fmt)
    if not fmt.passed:
        add_blocked("Master flow aborted", "Bryck could not be formatted; skipping mount/cloud-configure/datagen/initiate")
        _cleanup_master(mgr, None, None)
        return results
    verify2 = mgr.cap(f"master_{direction}:verify_unmounted2", ctx.bryck_info("verify not mounted after format"))
    verify2 = dataclasses.replace(verify2, passed=ctr._parse_bryck_state(verify2.stdout) in {" Ejected", " Removed"})
    add("Verify Bryck is NOT mounted after format", "Bryck is Ejected/Removed immediately after format.", verify2)

    # 6-7. Mount, Bryck info
    mnt = mgr.cap(f"master_{direction}:mount", ctx.ensure_mounted())
    if not mnt.passed:
        # Same stuck-Ejecting race as format above -- poll on a bounded deadline instead
        # of a single retry before giving up on the whole flow.
        deadline = time.time() + 300
        retried = False
        while not retried and time.time() < deadline:
            retried = mgr.ensure_mounted()
        mnt = ctr.StepResult(
            step=0, name="Mount Bryck (retry)", command="EnvironmentManager.ensure_mounted()",
            stdout="mounted after retry" if retried else "", stderr="" if retried else "mount failed after retry",
            returncode=0 if retried else 1, duration_sec=0.0, passed=retried,
        )
    add("Mount Bryck", "Mount succeeds after format.", mnt)
    if not mnt.passed:
        add_blocked("Master flow aborted", "Bryck could not be mounted; skipping cloud-configure/datagen/initiate")
        _cleanup_master(mgr, None, None)
        return results
    info2 = mgr.cap(f"master_{direction}:info_after_mount", ctx.bryck_info("after mount"))
    add("Get Bryck info (after mount)", "Bryck reports Mounted.", info2)

    # 8. Configure AWS
    cfg = mgr.cap(f"master_{direction}:configure_cloud", ctx.configure_cloud())
    if not cfg.passed:
        cfg = ctr.StepResult(
            step=0, name="Configure AWS (retry)", command="EnvironmentManager.ensure_cloud_configured()",
            stdout="configured after retry" if mgr.ensure_cloud_configured() else "",
            stderr="" if mgr.ctx.cloud_configured else "cloud configure failed after retry",
            returncode=0 if mgr.ctx.cloud_configured else 1, duration_sec=0.0, passed=mgr.ctx.cloud_configured,
        )
    add("Configure AWS", "Cloud configuration succeeds.", cfg)
    if not cfg.passed:
        add_blocked("Master flow aborted", "Cloud could not be configured; skipping datagen/initiate")
        _cleanup_master(mgr, None, None)
        return results

    seed_tid = None
    if direction == "upload":
        # 9-10. Generate ~2GB dataset (mgr.ensure_dataset aligns root/self-heals exactly like every
        # other dataset-dependent catalog case) and verify it matches configured bryck_src.
        ds_ok = mgr.ensure_dataset(spec="priority_2gb.yaml")
        ds = ctr.StepResult(
            step=0, name="datagen", command="EnvironmentManager.ensure_dataset(priority_2gb.yaml)",
            stdout="dataset generated and validated" if ds_ok else "",
            stderr="" if ds_ok else "dataset generation failed", returncode=0 if ds_ok else 1,
            duration_sec=0.0, passed=ds_ok,
        )
        add("Generate approximately 2GB dataset", "Dataset generation completes and file count is validated.", ds)
        val = mgr.cap(f"master_{direction}:validate_dataset", ctx.validate_dataset_source("priority_2gb.yaml"))
        add("Verify dataset exists under configured bryck_src", "datagen root matches cloud_ops.bryck_src exactly.", val)
    else:
        # 9. Generate ~2GB source data, then seed the cloud bucket with a real completed upload
        # (reuses mgr.ensure_dataset / mgr.create_transfer_at; no duplication).
        ds_ok = mgr.ensure_dataset(spec="priority_2gb.yaml")
        ds = ctr.StepResult(
            step=0, name="datagen", command="EnvironmentManager.ensure_dataset(priority_2gb.yaml)",
            stdout="dataset generated and validated" if ds_ok else "",
            stderr="" if ds_ok else "dataset generation failed", returncode=0 if ds_ok else 1,
            duration_sec=0.0, passed=ds_ok,
        )
        add("Generate approximately 2GB dataset (download source)", "Dataset generation completes and file count is validated.", ds)
        seed_tid = mgr.create_transfer_at("upload", "COMPLETED", timeout=7200)
        seeded = seed_tid is not None
        seed_sr = ctr.StepResult(
            step=0, name="seed", command="EnvironmentManager.create_transfer_at(upload, COMPLETED)",
            stdout="seed upload completed" if seeded else "",
            stderr="" if seeded else "seed upload did not reach COMPLETED",
            returncode=0 if seeded else 1, duration_sec=0.0, passed=seeded,
        )
        add("Seed cloud source data (upload to COMPLETED)", "Cloud bucket has an object available for the download flow.", seed_sr)
        show = mgr.cap(f"master_{direction}:show_cloud", ctx.show_cloud())
        add("Verify cloud source data", "Cloud configuration/bucket is visible and reachable.", show)

    # 11. Initiate transfer
    sr, ids = ctx.initiate_transfer(direction)
    mgr.cap(f"master_{direction}:initiate", sr)
    add(f"Initiate {direction}", f"{direction.capitalize()} transfer is admitted and returns a transfer ID.", sr)
    if not ids:
        add_blocked("Master flow aborted", f"could not obtain a transfer ID for the {direction} master flow")
        _cleanup_master(mgr, None, seed_tid)
        return results
    tid = ids[0]

    # 12-13. Verify IN_PROGRESS, report while IN_PROGRESS
    wait1 = mgr.cap(f"master_{direction}:wait_in_progress", ctx.wait_for_state(tid, {"IN_PROGRESS"}, timeout=120))
    add("Check status immediately after initiation", "Transfer reaches IN_PROGRESS.", wait1)
    rep1 = mgr.cap(f"master_{direction}:report_in_progress", ctx.download_report(tid, "IN_PROGRESS"))
    add("Download report while IN_PROGRESS", "Report download returns a bounded, state-consistent result.", rep1)

    # 14-16. Pause, verify PAUSED, report while PAUSED
    pause1 = mgr.cap(f"master_{direction}:pause1", ctx.pause_transfer(tid))
    add("Pause transfer", "Pause succeeds while the transfer is active.", pause1)
    status_paused = mgr.cap(f"master_{direction}:status_paused", ctx.transfer_status(tid, "PAUSED check"))
    status_paused = dataclasses.replace(
        status_paused,
        passed=status_paused.passed and "PAUSED" in (status_paused.stdout + status_paused.stderr).upper(),
    )
    add("Verify PAUSED", "Transfer status reports PAUSED.", status_paused)
    rep2 = mgr.cap(f"master_{direction}:report_paused", ctx.download_report(tid, "PAUSED"))
    add("Download PAUSED report", "Report download succeeds while PAUSED.", rep2)

    # 17. Pause again -> rejected/idempotent
    pause2 = mgr.cap(f"master_{direction}:pause2", ctx.pause_transfer(tid, expect_fail=True))
    add("Attempt PAUSE again", "A second pause on an already-paused transfer is rejected/idempotent.", pause2)

    # 18-20. Resume, verify IN_PROGRESS/RESUMED, report while RESUMED
    resume1 = mgr.cap(f"master_{direction}:resume1", ctx.resume_transfer(tid))
    add("Resume transfer", "Resume succeeds from PAUSED.", resume1)
    wait2 = mgr.cap(f"master_{direction}:wait_resumed", ctx.wait_for_state(tid, {"IN_PROGRESS"}, timeout=120))
    add("Check status after resume", "Transfer reaches IN_PROGRESS/RESUMED.", wait2)
    rep3 = mgr.cap(f"master_{direction}:report_resumed", ctx.download_report(tid, "RESUMED"))
    add("Download RESUMED report", "Report download succeeds after resume.", rep3)

    # 21-26. Destructive/lifecycle attempts during an active transfer
    def attempt(label: str, script: str, extra_args: list[str]) -> ctr.StepResult:
        return mgr.cap(
            f"master_{direction}:attempt_{label}",
            ctx.run_py(f"Attempt {label} during active transfer", script,
                      "--login", str(ctx.login_json), *extra_args, expect_fail=True, timeout=600),
        )

    add("Attempt FORMAT during active transfer",
        "Format is blocked while a transfer is active; transfer/mount unaffected.",
        attempt("format", "bryck_format.py", ["--params", str(ctx.fmt_mount_json)]))
    add("Attempt EJECT during active transfer",
        "Eject is blocked while a transfer is active; transfer remains observable.",
        mgr.cap(f"master_{direction}:attempt_eject", ctx.eject_bryck(expect_fail=True)))
    add("Attempt MOUNT during active transfer",
        "Mount is rejected/no-op while already mounted and a transfer is active.",
        attempt("mount", "bryck_mount.py", ["--params", str(ctx.fmt_mount_json)]))

    if args.confirm_destructive:
        add("Attempt ERASE during active transfer",
            "Erase is blocked while a transfer is active; no data loss.",
            attempt("erase", "bryck_erase.py", []))
        add("Attempt REMOVE during active transfer",
            "Remove is blocked while a transfer is active.",
            attempt("remove", "bryck_remove.py", []))
    else:
        add_blocked("Attempt ERASE/REMOVE during active transfer", "requires --confirm-destructive")

    add("Attempt AWS DECONFIGURE during active transfer",
        "Cloud deconfigure does not silently detach the active transfer.",
        mgr.cap(f"master_{direction}:attempt_deconfigure", ctx.deconfigure_cloud(expect_fail=True)))

    # Post-attempt integrity check: transfer must still be observable, Bryck mounted, cloud configured
    post = mgr.snapshot(f"master_{direction}:post_attempts")
    integrity_ok = post["bryck_state"] == " Mounted" and post["cloud_configured"]
    integrity_sr = ctr.StepResult(
        step=0, name="post-attempt integrity", command="EnvironmentManager.snapshot",
        stdout=json.dumps(post), stderr="" if integrity_ok else "state drifted after attempted destructive ops",
        returncode=0 if integrity_ok else 1, duration_sec=0.0, passed=integrity_ok,
    )
    add("Verify system integrity after blocked operations",
        "Bryck remains mounted and AWS remains configured after all rejected attempts.", integrity_sr)

    # 27-29. Wait for completion, verify COMPLETED, download COMPLETED report
    wait3 = mgr.cap(f"master_{direction}:wait_completed", ctx.wait_for_state(tid, {"COMPLETED"}, timeout=7200))
    add("Wait for completion", "Transfer reaches COMPLETED.", wait3)
    rep4 = mgr.cap(f"master_{direction}:report_completed", ctx.download_report(tid, "COMPLETED"))
    add("Download COMPLETED report", "Report download succeeds after completion.", rep4)

    # 30-32. Deconfigure AWS, eject, final Bryck info
    add("Deconfigure AWS (cleanup)", "Cloud deconfigure succeeds once the transfer is terminal.",
        mgr.cap(f"master_{direction}:deconfigure_final", ctx.deconfigure_cloud()))
    add("Eject Bryck (cleanup)", "Eject succeeds once the transfer is terminal.",
        mgr.cap(f"master_{direction}:eject_final", ctx.eject_bryck()))
    add("Get final Bryck info", "Final Bryck info is queryable and reports Ejected/Removed.",
        mgr.cap(f"master_{direction}:final_info", ctx.bryck_info("final")),
        env_after=mgr.snapshot(f"master_{direction}:final"))

    _cleanup_master(mgr, tid, seed_tid)
    return results


def _cleanup_master(mgr: "ner.EnvironmentManager", tid: str | None, seed_tid: str | None) -> None:
    for t in (tid, seed_tid):
        if t:
            mgr.cleanup_transfer(t)


def run_master_flow_both(mgr: "ner.EnvironmentManager", args, work: Path) -> list["ner.TestResult"]:
    """P0 master 'both' flow: upload then download in one continuous session, sharing a single
    mounted+configured baseline, with read-only management operations (network info, diagnostic
    report, bryck info) interleaved while each transfer is actively IN_PROGRESS."""
    ctx = mgr.ctx
    baseline = {"flow": "P0 master both (upload+download) flow"}
    counter = {"n": 0}
    results: list[ner.TestResult] = []

    def next_id() -> str:
        counter["n"] += 1
        return f"MASTER-BOTH-{counter['n']:02d}"

    def add(name, expected, sr, env_before=None, env_after=None,
            cleanup_status="not required", cleanup_detail=""):
        tid = next_id()
        result = ner.result_from_step(
            tid, "MASTER", name, baseline, env_before, sr, mgr,
            expected=expected, cleanup_status=cleanup_status,
            cleanup_detail=cleanup_detail, env_after=env_after,
        )
        result.narrative = ner.build_narrative(result)
        results.append(result)
        print(f"    [{tid}] {name} -> {result.status}")
        return result

    def add_blocked(name, reason):
        tid = next_id()
        result = ner.blocked(tid, "MASTER", name, baseline, reason, mgr=mgr)
        result.narrative = ner.build_narrative(result)
        results.append(result)
        print(f"    [{tid}] {name} -> BLOCKED ({reason})")
        return result

    def mgmt_op(label: str, script: str, timeout: int) -> ctr.StepResult:
        extra_args: list[str] = []
        if script == "bryck_report.py":
            extra_args = ["--output-dir", str(ctx.report_dir)]
        return mgr.cap(f"master_both:{label}", ctx.run_py(
            label.replace("_", " "), script, "--login", str(ctx.login_json), *extra_args, timeout=timeout,
        ))

    if not args.live:
        add_blocked("P0 master both flow", "requires --live against the dedicated Bryck device")
        return results
    if not (mgr.ensure_mounted() and mgr.ensure_cloud_configured()):
        add_blocked("P0 master both flow", "could not establish mounted+configured baseline")
        return results

    env0 = mgr.snapshot("master_both:baseline")
    ds_ok = mgr.ensure_dataset(spec="priority_2gb.yaml")
    add("Generate dataset (shared source for upload leg)", "Dataset generation completes and is validated.",
        ctr.StepResult(step=0, name="datagen", command="EnvironmentManager.ensure_dataset(priority_2gb.yaml)",
                      stdout="dataset generated and validated" if ds_ok else "",
                      stderr="" if ds_ok else "dataset generation failed",
                      returncode=0 if ds_ok else 1, duration_sec=0.0, passed=ds_ok),
        env_before=env0)
    if not ds_ok:
        add_blocked("Master both flow aborted", "dataset generation failed; skipping both transfer legs")
        return results

    # --- Upload leg ---
    up_sr, up_ids = ctx.initiate_transfer("upload")
    mgr.cap("master_both:initiate_upload", up_sr)
    add("Initiate upload leg", "Upload transfer is admitted and returns a transfer ID.", up_sr)
    if not up_ids:
        add_blocked("Master both flow aborted", "could not obtain an upload transfer ID")
        return results
    up_tid = up_ids[0]
    wait_up = mgr.cap("master_both:wait_upload_in_progress", ctx.wait_for_state(up_tid, {"IN_PROGRESS"}, timeout=120))
    add("Verify upload reaches IN_PROGRESS", "Upload transfer reaches IN_PROGRESS.", wait_up)
    if not wait_up.passed:
        add_blocked("Master both flow aborted", "upload never reached IN_PROGRESS; skipping management ops and download leg")
        mgr.cleanup_transfer(up_tid)
        return results

    add("Management op: network info during active upload",
        "Network info remains queryable and correct without disturbing the active upload.",
        mgmt_op("mgmt_network_info_during_upload", "bryck_network_info.py", 60))
    add("Management op: diagnostic report during active upload",
        "Diagnostic report generation succeeds while an upload is IN_PROGRESS.",
        mgmt_op("mgmt_report_during_upload", "bryck_report.py", 180))
    add("Management op: bryck info during active upload",
        "bryck_info remains queryable and reports Mounted during an active upload.",
        ctx.bryck_info("mgmt check during active upload"))

    wait_up_done = mgr.cap("master_both:wait_upload_completed", ctx.wait_for_state(up_tid, {"COMPLETED"}, timeout=7200))
    add("Wait for upload completion", "Upload transfer reaches COMPLETED.", wait_up_done)
    if not wait_up_done.passed:
        add_blocked("Master both flow aborted", "upload did not reach COMPLETED; skipping download leg")
        mgr.cleanup_transfer(up_tid)
        return results

    # --- Download leg (reads back the data the upload leg just wrote) ---
    dl_sr, dl_ids = ctx.initiate_transfer("download")
    mgr.cap("master_both:initiate_download", dl_sr)
    add("Initiate download leg", "Download transfer is admitted and returns a transfer ID.", dl_sr)
    if not dl_ids:
        add_blocked("Master both flow aborted", "could not obtain a download transfer ID")
        mgr.cleanup_transfer(up_tid)
        return results
    dl_tid = dl_ids[0]
    wait_dl = mgr.cap("master_both:wait_download_in_progress", ctx.wait_for_state(dl_tid, {"IN_PROGRESS"}, timeout=120))
    add("Verify download reaches IN_PROGRESS", "Download transfer reaches IN_PROGRESS.", wait_dl)
    if not wait_dl.passed:
        add_blocked("Master both flow aborted", "download never reached IN_PROGRESS; skipping management ops")
        mgr.cleanup_transfer(up_tid)
        mgr.cleanup_transfer(dl_tid)
        return results

    add("Management op: network info during active download",
        "Network info remains queryable and correct without disturbing the active download.",
        mgmt_op("mgmt_network_info_during_download", "bryck_network_info.py", 60))
    add("Management op: diagnostic report during active download",
        "Diagnostic report generation succeeds while a download is IN_PROGRESS.",
        mgmt_op("mgmt_report_during_download", "bryck_report.py", 180))
    add("Management op: bryck info during active download",
        "bryck_info remains queryable and reports Mounted during an active download.",
        ctx.bryck_info("mgmt check during active download"))

    wait_dl_done = mgr.cap("master_both:wait_download_completed", ctx.wait_for_state(dl_tid, {"COMPLETED"}, timeout=7200))
    add("Wait for download completion", "Download transfer reaches COMPLETED.", wait_dl_done)

    cleanup_up = mgr.cleanup_transfer(up_tid)
    cleanup_dl = mgr.cleanup_transfer(dl_tid)
    env_after = mgr.snapshot("master_both:final")
    add("Final integrity check (both legs)",
        "Bryck remains mounted and cloud-configured, in a consistent, queryable state after both legs.",
        ctr.StepResult(step=0, name="final integrity", command="EnvironmentManager.snapshot",
                       stdout=json.dumps(env_after), stderr="", returncode=0, duration_sec=0.0,
                       passed=env_after["bryck_state"] == " Mounted" and env_after["cloud_configured"]),
        env_after=env_after, cleanup_status="performed", cleanup_detail=f"upload: {cleanup_up}; download: {cleanup_dl}")
    return results


# =============================================================================
# Catalog sections (delegates entirely to negative_environment_runner)
# =============================================================================

def run_catalog(sections: list[str], mgr: "ner.EnvironmentManager", args, work: Path) -> list["ner.TestResult"]:
    entries = ctr._negative_plan_entries(ner.PLAN_PATH)
    effective_sections = {s.strip().upper() for s in args.sections.split(",") if s.strip()} or set(sections or [])
    if effective_sections:
        entries = [e for e in entries if (re.match(r"[A-Z]+", e[0]) or [""])[0] in effective_sections]
    if args.test_id:
        wanted_ids = {t.strip() for t in args.test_id.split(",") if t.strip()}
        entries = [e for e in entries if e[0] in wanted_ids]
    if args.range:
        m = re.fullmatch(r"\s*(\d+)\s*-\s*(\d+)\s*", args.range)
        if not m:
            print(f"ERROR: --range must look like START-END (e.g. 3-88), got {args.range!r}")
        else:
            start, end = int(m.group(1)), int(m.group(2))
            if 1 <= start <= end <= len(entries):
                entries = entries[start - 1:end]
                print(f"Selected range {start}-{end}: {len(entries)} case(s) -> {[e[0] for e in entries]}")
            else:
                print(f"ERROR: --range {args.range!r} is invalid for {len(entries)} selected case(s)")
    results: list[ner.TestResult] = []
    for case_id, _heading, desc in entries:
        mgr.commands = []
        result = ner.dispatch(case_id, desc, mgr, args, work, {})
        result.narrative = ner.build_narrative(result)
        results.append(result)
        print(f"    [{case_id}] {desc} -> {result.status}")
    return results


# =============================================================================
# CLI
# =============================================================================

def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Cloud Transfer + Bryck Management Negative Test Suite (master-flow runner).",
    )
    sel = p.add_argument_group("suite selection (default: --all)")
    sel.add_argument("--upload", action="store_true", help="run only the P0 upload master flow")
    sel.add_argument("--download", action="store_true", help="run only the P0 download master flow")
    sel.add_argument("--both", action="store_true",
                     help="run the P0 'both' master flow: upload then download in one continuous "
                          "session, with management operations interleaved during each active transfer")
    sel.add_argument("--static", action="store_true", help="run only CLI/AUTH/TID/AWS/PATH/LIFE/DATA catalog sections")
    sel.add_argument("--concurrency", action="store_true", help="run only the RACE/DUP catalog sections")
    sel.add_argument("--recovery", action="store_true", help="run only the FAULT/REC/VERIFY/INT catalog sections")
    sel.add_argument("--all", action="store_true", help="run both master flows plus the entire negative catalog")

    names = p.add_argument_group("test selection by name/ID (resolves against the centralized registry)")
    names.add_argument("--test", default="", help='run one test by ID/name, e.g. --test AUTH-01 or --test "AUTH-01 - Invalid username"')
    names.add_argument("--tests", default="", help="run multiple tests, comma-separated, e.g. AUTH-01,AWS-03,STATE-01")
    names.add_argument("--from", dest="range_from", default="", help="start of an inclusive ID range (use with --to)")
    names.add_argument("--to", dest="range_to", default="", help="end of an inclusive ID range (use with --from)")
    names.add_argument("--module", default="", help="run every test in one module, e.g. --module AWS (also: SM, F, MASTER, SCENARIO, COMBO)")
    names.add_argument("--modules", default="", help="run every test in multiple modules, comma-separated, e.g. AWS,STATE")
    names.add_argument("--list", action="store_true", help="print every registered test (ID/module/name) and exit")
    names.add_argument("--search", default="", help="search IDs/names/descriptions/modules and print matches, then exit")

    mode = p.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="print planned commands without executing them (default)")
    mode.add_argument("--live", action="store_true", help="execute against the dedicated Bryck device")
    p.add_argument("--confirm-destructive", action="store_true",
                   help="allow erase / remove / eject-during-transfer inside the master flow and catalog destructive cases")
    p.add_argument("--allow-ip-change", action="store_true",
                   help="also allow the master flow's IP-change step (separate from --confirm-destructive because "
                        "it can strand the runner's own API/SSH session)")
    p.add_argument("--allow-service-faults", action="store_true")
    p.add_argument("--allow-network-faults", action="store_true")
    p.add_argument("--allow-reboot", action="store_true",
                   help="deprecated/no-op: reboot test cases (REC-04, F-37, F-38) are excluded from this suite")
    p.add_argument("--sections", default="")
    p.add_argument("--test-id", default="")
    p.add_argument("--range", default="")
    p.add_argument("--override", action="append", default=[])
    p.add_argument("--login", default=str(ctr.DEFAULT_LOGIN_JSON))
    p.add_argument("--cloud-ops", default=str(ctr.DEFAULT_CLOUD_OPS_JSON))
    p.add_argument("--format-mount-params", default=str(ctr.DEFAULT_FORMAT_MOUNT_PARAMS_JSON))
    p.add_argument("--report-dir", default=str(ctr.DEFAULT_REPORT_DIR))
    p.add_argument("--results-dir", default=str(SCRIPT_DIR / "results"))
    p.add_argument("--datagen-bin", default=ctr.DATAGEN_BIN)
    p.add_argument("--spec-dir", default=str(ctr.SPEC_DIR))
    p.add_argument("--ssh-user", default=None)
    p.add_argument("--ssh-host", default=None)
    p.add_argument("--upload-to-server", action="store_true",
                   help="after the run, SFTP the HTML/JSON report to --remote-report-dir on the Bryck device "
                        "(requires --live; uses bryckserver_username/bryckserver_password from login.json)")
    p.add_argument("--remote-report-dir", default=DEFAULT_REMOTE_REPORT_DIR,
                   help=f"remote directory for --upload-to-server (default: {DEFAULT_REMOTE_REPORT_DIR})")

    args = p.parse_args(argv)
    resolve_name_based_selection(args)
    if not args.dry_run and not args.live:
        args.dry_run = True
    if not any([args.upload, args.download, args.both, args.static, args.concurrency, args.recovery,
               args.sections, args.test_id, args.scenario_ids]):
        args.all = True
    return args


def run_final_diagnostic_report(mgr: "ner.EnvironmentManager", args) -> "ner.TestResult":
    """Generate one bryck_report.py diagnostic ZIP after the whole selected run finishes --
    same final-audit pattern already used by CLEAN-10 (bryck_info/bryck_network_info), just
    added once at the end of every run instead of only inside that one catalog case."""
    ctx = mgr.ctx
    sr = mgr.cap("final_diagnostic_report", ctx.run_py(
        "Final diagnostic report", "bryck_report.py",
        "--login", str(ctx.login_json), "--output-dir", str(ctx.report_dir), timeout=900,
    ))
    result = ner.result_from_step(
        "FINAL-REPORT", "CLEAN", "Final diagnostic report (bryck_report.py)", {}, None, sr, mgr,
        expected="A diagnostic report ZIP is generated for the whole run without a traceback.",
    )
    result.narrative = ner.build_narrative(result)
    print(f"    [FINAL-REPORT] Final diagnostic report -> {result.status}")
    return result


def main(argv=None) -> int:
    args = parse_args(argv)
    ctx = ner.build_context(args)
    mgr = ner.EnvironmentManager(ctx)

    run_id = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d_%H%M%S")
    results_dir = Path(args.results_dir)
    results_dir.mkdir(parents=True, exist_ok=True)
    started = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")

    all_results: list[ner.TestResult] = []
    with tempfile.TemporaryDirectory(prefix=f"bryck-negmaster-{run_id}-") as work_dir:
        work = Path(work_dir)
        try:
            if args.upload or args.all:
                print("\n=== P0 MASTER FLOW: UPLOAD ===")
                all_results.extend(run_master_flow("upload", mgr, args, work))
            if args.download or args.all:
                print("\n=== P0 MASTER FLOW: DOWNLOAD ===")
                all_results.extend(run_master_flow("download", mgr, args, work))
            if args.both or args.all:
                print("\n=== P0 MASTER FLOW: BOTH (upload + download, management ops interleaved) ===")
                all_results.extend(run_master_flow_both(mgr, args, work))
            if args.static or args.all:
                print("\n=== STATIC / CLI / AUTH / TRANSFER-ID / AWS / PATH / LIFECYCLE / DATASET CATALOG ===")
                all_results.extend(run_catalog(STATIC_SECTIONS, mgr, args, work))
            if args.concurrency or args.all:
                print("\n=== CONCURRENCY / DUPLICATE-OPERATION CATALOG ===")
                all_results.extend(run_catalog(CONCURRENCY_SECTIONS, mgr, args, work))
            if args.recovery or args.all:
                print("\n=== FAULT / RECOVERY / VERIFICATION / INTEGRITY CATALOG ===")
                all_results.extend(run_catalog(RECOVERY_SECTIONS, mgr, args, work))
            if args.all:
                print("\n=== REMAINING NEGATIVE CATALOG (XFER/DOWNLOAD/STATE/REPORT/CLEAN/MGMT) ===")
                all_results.extend(run_catalog(
                    [s for s in {re.match(r"[A-Z]+", e[0]).group(0)
                                 for e in ctr._negative_plan_entries(ner.PLAN_PATH)} if s not in ALL_HANDLED_SECTIONS],
                    mgr, args, work,
                ))
            if args.sections or args.test_id:
                print("\n=== AD-HOC CATALOG SELECTION (--sections/--test-id/--range) ===")
                all_results.extend(run_catalog([], mgr, args, work))
            if args.scenario_ids:
                print("\n=== POSITIVE SCENARIO / COMBINATION FLOWS (selected by name) ===")
                all_results.extend(run_scenarios(args.scenario_ids, mgr, args))
        except Exception as exc:  # noqa: BLE001 - one failing section must not abort the whole suite
            print(f"WARNING: negative test suite raised {type(exc).__name__}: {exc}")

    print("\n=== FINAL DIAGNOSTIC REPORT ===")
    all_results.append(run_final_diagnostic_report(mgr, args))

    finished = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")

    counts = {"PASS": 0, "FAIL": 0, "BLOCKED": 0}
    for r in all_results:
        counts[r.status] = counts.get(r.status, 0) + 1

    report_id = f"negative_{run_id}"
    html_path = results_dir / f"cloud_transfer_negative_report_{run_id}.html"
    json_path = results_dir / f"cloud_transfer_negative_report_{run_id}.json"
    html_path.write_text(ner.build_html(report_id, started, finished, all_results), encoding="utf-8")
    json_path.write_text(
        json.dumps([dataclasses.asdict(r) for r in all_results], indent=2, default=str), encoding="utf-8"
    )

    print("\n" + "=" * 60)
    print("Cloud Transfer Negative Test Execution Complete")
    print("=" * 60)
    print(f"\nTotal   : {len(all_results)}")
    print(f"PASS    : {counts.get('PASS', 0)}")
    print(f"FAIL    : {counts.get('FAIL', 0)}")
    print("SKIP    : 0")
    print(f"ERROR   : {counts.get('BLOCKED', 0)}")  # BLOCKED == fixture/precondition could not be established
    print(f"\nHTML Report:\n{html_path}")
    print("=" * 60)

    if args.upload_to_server and args.live:
        upload_report_to_server(args, html_path, json_path)
    elif args.upload_to_server:
        print("NOTE: --upload-to-server ignored because the run was not --live (no device to reach).")

    return 1 if counts.get("FAIL", 0) else 0


def upload_report_to_server(args, html_path: Path, json_path: Path) -> None:
    """Best-effort SFTP copy of the report pair to the Bryck device; never fails the run."""
    remote_dir = args.remote_report_dir.rstrip("/")
    try:
        session = ApiSession.from_login_json(args.login)
        with SshRunner.from_session(session) as ssh:
            ssh.run(f"mkdir -p {remote_dir}")
            ssh.put(str(html_path), f"{remote_dir}/{html_path.name}")
            ssh.put(str(json_path), f"{remote_dir}/{json_path.name}")
        print(f"Uploaded report to {session.host}:{remote_dir}/")
    except (SshRunnerError, OSError) as exc:
        print(f"WARNING: could not upload report to Bryck device: {exc}")


if __name__ == "__main__":
    raise SystemExit(main())
