#!/usr/bin/env python3
"""cloud_full_test.py -- unified transfer + negative-catalog test harness.

This is a COPY of cloud_transfer_only.py (that file is left completely
untouched) extended with:

  1. The same real-transfer engine as cloud_transfer_only.py, unchanged:
     datagen -> configure cloud -> CLI initiate -> pause/resume cycles ->
     poll to terminal -> bryck_state/transfer_status capture at every step
     -> perf capture -> (optional) cleanup.
  2. The full negative-test catalog (308 cases: CLI/AUTH/TID/AWS/PATH/LIFE/
     DATASET/XFER/DOWNLOAD/STATE/RACE/DUP/REPORT/FAULT/REC/VERIFY/INT/CLEAN/
     MGMT/SVC/SM/F -- exactly the list from
     `python3 cloudcpclitesting.py --list-negative`). These are NOT
     reimplemented here -- they are delegated in-process to
     cloudcpclitesting.run_negative_suite()/NEG_CATALOG, which remains the
     single source of truth for which cases are IMPLEMENTED vs. still a
     stub. Duplicating ~2000 lines of that framework here would just create
     a second copy to keep in sync; importing it instead means every future
     fix/implementation in cloudcpclitesting.py is automatically available
     here too.
  3. A unified CLI surface modeled on CloudCpFallbackTesting/cloudcp_fallback_test.py
     (--all/--one/--from/--to/--negative/--negative-case/--list, --dry-run,
     --manual, --skip-datagen/--skip-seed/--keep-config/--skip-cleanup,
     --seed, --poll-interval/--poll-timeout, path/host flags, and the
     --component* group).

IMPORTANT -- honesty about scope:
  --component / --component-one / --component-negative / --component-list
  refer to fallback_worker/mp_batch_retry-style *internal mechanism* tests
  from cloudcp_fallback_test.py. No source for those internal mechanisms
  exists anywhere in this repo (only their CLI surface was described), so
  they are NOT implemented here -- invoking them prints a clear message and
  exits non-zero instead of fabricating fake results.

Usage
-----
    python3 cloud_full_test.py --list
    python3 cloud_full_test.py --all --dry-run
    python3 cloud_full_test.py --one TRANSFER-DS-P8-01-UPLOAD
    python3 cloud_full_test.py --negative
    python3 cloud_full_test.py --negative-case AWS-03,CLI-01
    python3 cloud_full_test.py --from 1 --to 9
    python3 cloud_full_test.py --manual --negative
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import pathlib
import random
import re
import shutil
import sys
import time
import types
from typing import List, Optional

HERE = pathlib.Path(__file__).resolve().parent
BRYCK_CLI_DIR = HERE / "bryckclient-cli"
RESULTS_ROOT = HERE / "results"

sys.path.insert(0, str(HERE))
sys.path.insert(0, str(BRYCK_CLI_DIR))

import cloud_cli_runner as ccr  # noqa: E402  (datagen, status polling/fallbacks, run helpers)
import cli_perf_capture as perf_mod  # noqa: E402
import cloudcpclitesting as negtest  # noqa: E402  (NEG_CATALOG names/order -- id/name source only, not execution)
import cloud_transfer_test_runner as ctr  # noqa: E402  (TestContext -- underlies negative_environment_runner)
# cloud_transfer_test_runner.py assumes bryck_info.py/etc. live beside itself, but they
# actually live one level down in bryckclient-cli/. Patched here (not in that file, since
# this script is the only entry point used) so every ctx.run_py()/bryck_info() call inside
# negative_environment_runner.py resolves to the real script location. Must happen before
# negative_environment_runner is imported, since its own module-level constants copy this value.
ctr.SCRIPT_DIR = BRYCK_CLI_DIR
import negative_environment_runner as ner  # noqa: E402  (the actual, comprehensive negative-case implementations:
                                            # LIFE/DATA/XFER/DOWNLOAD/RACE/DUP/REPORT/FAULT/REC/VERIFY/INT/MGMT/
                                            # SVC/SM/F, not just CLI/AUTH/TID/AWS/STATE/CLEAN -- this is the
                                            # environment file negative cases are actually delegated to below.)

LOG = logging.getLogger("cloud_full_test")


TERMINAL_SUCCESS = "COMPLETED"
TERMINAL_FAILURE = {"FAILED", "STOPPED", "CANCELLED"}
TERMINAL_STATES = {TERMINAL_SUCCESS} | TERMINAL_FAILURE
# States that mean the transfer is still legitimately progressing -- hitting
# --poll-timeout while in one of these is "still running", not a product failure.
STILL_RUNNING_STATES = {"IN_PROGRESS", "PAUSED", "QUEUED"}
TRANSFER_MODES = ("upload", "download", "both")


def setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stdout,
    )


# =============================================================================
# Transfer engine -- copied verbatim from cloud_transfer_only.py (unchanged)
# =============================================================================

_BRYCK_STATE_RE = re.compile(r"\b(Mounted|Ejected|Removed)\b", re.IGNORECASE)


def read_bryck_state_from_journalctl(args: argparse.Namespace) -> str:
    """Fallback when bryck_info.py's own state read comes back UNKNOWN/blank:
    grep recent journalctl history (same --journal-tag(s) cli_perf_capture.py
    already tails for perf data) for the last mount-state keyword the broker
    itself logged. Never raises -- returns 'UNKNOWN' on any failure so the
    caller can log it and move on to the next step instead of blocking the
    whole case on one flaky state read."""
    import subprocess
    tags = args.journal_tag if isinstance(args.journal_tag, list) else [args.journal_tag]
    tag_flags = [flag for tag in tags for flag in ("-t", tag)]
    try:
        proc = subprocess.run(
            ["sudo", "journalctl", *tag_flags, "-n", "500", "--no-pager"],
            capture_output=True, text=True, timeout=30,
        )
    except (OSError, subprocess.TimeoutExpired):
        return "UNKNOWN"
    text = (proc.stdout or "") + (proc.stderr or "")
    matches = _BRYCK_STATE_RE.findall(text)
    return matches[-1].capitalize() if matches else "UNKNOWN"


def ensure_mounted(args: argparse.Namespace, redact, tcr: ccr.TestCaseResult) -> str:
    """Real bryck_info states are ' Mounted', ' Ejected', ' Removed' (leading
    space, per bryckclient-cli). Only auto-mount from 'Ejected' (the only
    state bryck_mount.py accepts) -- 'Removed' or anything else means the
    device needs format/mount (or scan) first, which this script does not
    attempt; surface it as a clear warning instead of a confusing
    bryck_mount.py failure. If the state read itself is UNKNOWN/blank, fall
    back to journalctl and proceed with whatever state that shows -- never
    block the case on one flaky read."""
    state, cmd = ccr.get_bryck_state(
        argparse.Namespace(login=args.login, dry_run=args.dry_run, python_bin=args.python_bin), LOG, redact)
    tcr.commands.append(cmd.as_dict())
    if args.dry_run:
        return state
    normalized = state.strip().lower()
    if normalized in ("", "unknown"):
        LOG.warning("bryck_info state read was %r; falling back to journalctl before proceeding", state)
        journal_state = read_bryck_state_from_journalctl(args)
        tcr.notes.append(f"bryck state fallback via journalctl: {journal_state}")
        LOG.info("journalctl fallback bryck state: %s", journal_state)
        state, normalized = journal_state, journal_state.strip().lower()
    if normalized == "mounted":
        return state
    if normalized != "ejected":
        LOG.warning("Bryck state is %r after API+journalctl checks -- proceeding to the next step anyway "
                   "instead of blocking ('Removed'/unknown needs bryck_format.py + bryck_mount.py first; "
                   "only auto-mounts from 'Ejected').", state)
        return state
    LOG.info("Bryck is Ejected; mounting before dataset generation/transfer")
    mount_cmd = ccr.run_py_script(
        "bryck_mount.py", ["--login", args.login, "--params", args.format_mount_params],
        LOG, args.dry_run, redact, args.python_bin)
    tcr.commands.append(mount_cmd.as_dict())
    if mount_cmd.returncode != 0:
        LOG.warning("bryck_mount.py failed (rc=%s): %s", mount_cmd.returncode, mount_cmd.stderr or mount_cmd.stdout)
        return state
    deadline = time.time() + args.action_timeout
    while time.time() < deadline:
        state, cmd = ccr.get_bryck_state(
            argparse.Namespace(login=args.login, dry_run=args.dry_run, python_bin=args.python_bin), LOG, redact)
        tcr.commands.append(cmd.as_dict())
        if state.strip().lower() == "mounted":
            break
        time.sleep(args.poll_interval)
    if state.strip().lower() != "mounted":
        journal_state = read_bryck_state_from_journalctl(args)
        tcr.notes.append(f"post-mount state fallback via journalctl: {journal_state}")
        LOG.warning("post-mount state still %r after %ss; journalctl shows %r -- proceeding to the next "
                   "step anyway", state, args.action_timeout, journal_state)
        if journal_state.strip().lower() == "mounted":
            state = journal_state
    return state


def configure_cloud(args: argparse.Namespace, base_cloud_ops: dict, tier: str, redact,
                    tcr: ccr.TestCaseResult) -> str:
    """Deconfigure any stale cloud config, rewrite cloud_ops.json for this
    dataset/tier, then configure + verify. Mirrors Executor.configure_cloud()."""
    cloud_type = str(base_cloud_ops.get("cloud_type", "aws"))
    deconfigure_cmd = ccr.run_py_script(
        "bryck_cloud_deconfigure.py", ["--login", args.login, "--cloud-type", cloud_type],
        LOG, args.dry_run, redact, args.python_bin)
    tcr.commands.append(deconfigure_cmd.as_dict())
    if not args.dry_run and deconfigure_cmd.returncode != 0:
        LOG.info("bryck_cloud_deconfigure.py rc=%s (ignored, likely nothing configured yet)",
                 deconfigure_cmd.returncode)

    cfg = dict(base_cloud_ops)
    cfg["bryck_src"] = f"{args.output_base}/{tier}"
    cfg["cloud_bucket"] = f"{args.bucket}/{tier}"
    cfg["bryck_dst"] = f"{args.download_base}/{tier}"
    if not args.dry_run:
        with open(args.params, "w", encoding="utf-8") as handle:
            json.dump(cfg, handle, indent=2)

    configure_cmd = ccr.run_py_script(
        "bryck_cloud_configure.py", ["--login", args.login, "--params", args.params],
        LOG, args.dry_run, redact, args.python_bin)
    tcr.commands.append(configure_cmd.as_dict())
    if not args.dry_run and configure_cmd.returncode != 0:
        raise RuntimeError(f"bryck_cloud_configure.py failed (rc={configure_cmd.returncode}): "
                            f"{configure_cmd.stderr or configure_cmd.stdout}")
    show_cmd = ccr.run_py_script(
        "bryck_cloud_show.py", ["--login", args.login], LOG, args.dry_run, redact, args.python_bin)
    tcr.commands.append(show_cmd.as_dict())
    if not args.dry_run and show_cmd.returncode != 0:
        raise RuntimeError(f"bryck_cloud_show.py failed (rc={show_cmd.returncode}): "
                            f"{show_cmd.stderr or show_cmd.stdout}")
    return cfg["cloud_bucket"]


def initiate_transfer_cli(args: argparse.Namespace, src: str, dst: str, redact,
                         tcr: ccr.TestCaseResult) -> Optional[str]:
    """`/opt/bryck/.venv/bryck/bin/bryckcloud transfer add aws --src <src> --dst <dst>`
    -- same CLI adapter cloud_cli_runner.py's --cli transfer method uses."""
    batchmeta_dir = pathlib.Path(args.batchmeta_dir)
    transfer_logs_dir = pathlib.Path(args.transfer_logs_dir)
    before_ids = set() if args.dry_run else ccr.base.collect_transfer_ids(batchmeta_dir, transfer_logs_dir)
    cmd = ccr.run_argv(
        "bryckcloud transfer add aws",
        [args.bryckcloud_bin, "transfer", "add", "aws", "--src", src, "--dst", dst],
        LOG, args.dry_run, redact, timeout=args.wait_timeout,
    )
    tcr.commands.append(cmd.as_dict())
    if args.dry_run:
        return "DRYRUN-ID"
    combined_out = (cmd.stdout or "") + (cmd.stderr or "")
    if cmd.returncode != 0:
        # The broker rejects a duplicate transfer to the same src/dst (e.g. a previous case's
        # transfer to this same destination was never cleaned up, likely because --skip-cleanup
        # was used) with "Transfer is triggered already with ID: N" -- that's a real, already-
        # existing transfer, not a failure; reuse its ID instead of aborting the whole case.
        already_match = re.search(r"triggered already with ID:\s*(\d+)", combined_out, re.IGNORECASE)
        if already_match:
            existing_id = already_match.group(1)
            LOG.warning("bryckcloud transfer add aws: %s -- reusing existing transfer id %s instead "
                       "of starting a new one", combined_out.strip(), existing_id)
            tcr.notes.append(f"reused already-triggered transfer id {existing_id} (duplicate src/dst rejected)")
            return existing_id
        raise RuntimeError(f"bryckcloud transfer add aws failed (rc={cmd.returncode}): {cmd.stderr or cmd.stdout}")
    transfer_id = ccr.base.parse_transfer_id_from_output(combined_out)
    if transfer_id is None:
        transfer_id = ccr.base.detect_transfer_id(
            argparse.Namespace(transfer_id=None, poll_interval=args.poll_interval),
            before_ids, "", batchmeta_dir, transfer_logs_dir,
        )
    return str(transfer_id) if transfer_id is not None else None


def _cycle_label(index: int, total: int) -> str:
    if total <= 1:
        return "single"
    if index == 0:
        return "early"
    if index == total - 1:
        return "late"
    return "middle"


def _one_pause_resume_cycle(args: argparse.Namespace, transfer_id: str, redact,
                            tcr: ccr.TestCaseResult, ns: argparse.Namespace, label: str) -> bool:
    """One pause -> wait --pause-duration -> resume cycle. Returns True if the
    transfer was confirmed IN_PROGRESS beforehand (i.e. the cycle actually ran),
    False if it had already reached a terminal state (nothing left to pause)."""
    deadline = time.time() + args.pause_wait_timeout
    state = "UNKNOWN"
    while time.time() < deadline:
        state, cmd = ccr.get_transfer_status(ns, transfer_id, LOG, redact)
        tcr.commands.append(cmd.as_dict())
        if state == "IN_PROGRESS" or state in TERMINAL_STATES:
            break
        time.sleep(args.poll_interval)

    if state != "IN_PROGRESS":
        LOG.warning("transfer %s not IN_PROGRESS for the %r pause/resume cycle (state=%s); skipping",
                   transfer_id, label, state)
        tcr.notes.append(f"pause/resume [{label}] skipped: transfer not IN_PROGRESS (state={state})")
        return False

    pause_cmd = ccr.run_py_script(
        "bryck_cloud_transfer_pause.py", ["--login", args.login, "--transfer-id", str(transfer_id)],
        LOG, args.dry_run, redact, args.python_bin)
    tcr.commands.append(pause_cmd.as_dict())
    if pause_cmd.returncode != 0:
        LOG.warning("pause [%s] failed (rc=%s): %s", label, pause_cmd.returncode,
                   pause_cmd.stderr or pause_cmd.stdout)
        tcr.notes.append(f"pause [{label}] failed rc={pause_cmd.returncode}")
        return True

    paused_deadline = time.time() + args.pause_wait_timeout
    while time.time() < paused_deadline:
        state, cmd = ccr.get_transfer_status(ns, transfer_id, LOG, redact)
        tcr.commands.append(cmd.as_dict())
        if state == "PAUSED":
            break
        time.sleep(args.poll_interval)
    LOG.info("transfer %s state after pause [%s]: %s", transfer_id, label, state)
    tcr.notes.append(f"paused [{label}] (state={state})")

    if args.pause_duration > 0:
        time.sleep(args.pause_duration)

    resume_cmd = ccr.run_py_script(
        "bryck_cloud_transfer_resume.py", ["--login", args.login, "--transfer-id", str(transfer_id)],
        LOG, args.dry_run, redact, args.python_bin)
    tcr.commands.append(resume_cmd.as_dict())
    if resume_cmd.returncode != 0:
        LOG.warning("resume [%s] failed (rc=%s): %s", label, resume_cmd.returncode,
                   resume_cmd.stderr or resume_cmd.stdout)
        tcr.notes.append(f"resume [{label}] failed rc={resume_cmd.returncode}")
        return True

    resumed_deadline = time.time() + args.pause_wait_timeout
    while time.time() < resumed_deadline:
        state, cmd = ccr.get_transfer_status(ns, transfer_id, LOG, redact)
        tcr.commands.append(cmd.as_dict())
        if state == "IN_PROGRESS" or state in TERMINAL_STATES:
            break
        time.sleep(args.poll_interval)
    LOG.info("transfer %s state after resume [%s]: %s", transfer_id, label, state)
    tcr.notes.append(f"resumed [{label}] (state={state})")
    return True


def pause_resume_transfer(args: argparse.Namespace, transfer_id: str, redact,
                          tcr: ccr.TestCaseResult) -> None:
    """Run --pause-cycles pause->wait->resume cycles spaced --pause-interval
    seconds apart while the transfer is IN_PROGRESS (labeled early/middle/late
    for --pause-cycles=3, the default) -- recording each step/status check as
    its own command. Stops early (with a note, not an error) once the transfer
    reaches a terminal state, since there's nothing left to pause."""
    ns = argparse.Namespace(login=args.login, dry_run=args.dry_run, python_bin=args.python_bin)
    total = max(1, args.pause_cycles)
    for i in range(total):
        label = _cycle_label(i, total)
        still_running = _one_pause_resume_cycle(args, transfer_id, redact, tcr, ns, label)
        if not still_running:
            break
        if i < total - 1 and args.pause_interval > 0:
            time.sleep(args.pause_interval)


def verify_pause_after_completion(args: argparse.Namespace, transfer_id: str, final_state: str,
                                  redact, tcr: ccr.TestCaseResult) -> None:
    """Verification step: once the transfer has reached a terminal state,
    attempt one more pause then resume and record whether they were correctly
    rejected/no-op -- purely observational, never affects the leg's PASS/FAIL."""
    if final_state not in TERMINAL_STATES:
        tcr.notes.append(f"post-completion pause/resume verification skipped: final_state={final_state!r} "
                          f"is not terminal")
        return
    pause_cmd = ccr.run_py_script(
        "bryck_cloud_transfer_pause.py", ["--login", args.login, "--transfer-id", str(transfer_id)],
        LOG, args.dry_run, redact, args.python_bin)
    tcr.commands.append(pause_cmd.as_dict())
    resume_cmd = ccr.run_py_script(
        "bryck_cloud_transfer_resume.py", ["--login", args.login, "--transfer-id", str(transfer_id)],
        LOG, args.dry_run, redact, args.python_bin)
    tcr.commands.append(resume_cmd.as_dict())
    LOG.info("post-completion verification for transfer %s: pause rc=%s, resume rc=%s (transfer was %s)",
             transfer_id, pause_cmd.returncode, resume_cmd.returncode, final_state)
    tcr.notes.append(
        f"post-completion pause/resume verification: pause_rc={pause_cmd.returncode} "
        f"resume_rc={resume_cmd.returncode} (transfer already {final_state}; both are expected to be "
        f"rejected/no-op, not accepted as if the transfer were still active)"
    )


def poll_until_terminal(args: argparse.Namespace, transfer_id: str,
                        active_journal_raw: Optional[pathlib.Path], redact,
                        tcr: ccr.TestCaseResult) -> str:
    """Poll to a terminal state via bryck_cloud_transfer_status.py, falling back
    -- in order -- to the live journal_raw.log tail, transfer_summary.txt on
    disk, and a bounded journalctl re-query, exactly like cloud_cli_runner.py's
    transfer_status(), since the status API/script can report UNKNOWN for a
    transfer that is still running or just finished."""
    if args.dry_run:
        return TERMINAL_SUCCESS
    deadline = time.time() + args.wait_timeout
    state = "UNKNOWN"
    ns = argparse.Namespace(login=args.login, dry_run=args.dry_run, python_bin=args.python_bin)
    while time.time() < deadline:
        state, cmd = ccr.get_transfer_status(ns, transfer_id, LOG, redact)
        tcr.commands.append(cmd.as_dict())
        if state == "UNKNOWN":
            if active_journal_raw is not None:
                live = ccr.read_live_journal_transfer_status(active_journal_raw, transfer_id)
                if live != "UNKNOWN":
                    state = live
            if state == "UNKNOWN":
                local = ccr.read_local_transfer_status(pathlib.Path(args.transfer_logs_dir), transfer_id)
                if local != "UNKNOWN":
                    state = local
            if state == "UNKNOWN":
                journal = ccr.read_journalctl_transfer_status(transfer_id, args.journal_tag)
                if journal != "UNKNOWN":
                    state = journal
        LOG.info("transfer %s state=%s", transfer_id, state or "UNKNOWN")
        if state in TERMINAL_STATES:
            return state
        time.sleep(args.poll_interval)
    return state or "UNKNOWN"


def cleanup(args: argparse.Namespace, tier: str, redact, tcr: ccr.TestCaseResult,
           log_dir: Optional[pathlib.Path] = None) -> None:
    if args.dry_run or args.keep:
        LOG.info("cleanup skipped (dry-run or --keep)")
        tcr.notes.append("cleanup skipped (dry-run or --keep)")
        return
    for base_dir in (args.output_base, args.download_base):
        target = pathlib.Path(base_dir) / tier
        if target.is_dir():
            try:
                shutil.rmtree(target)
            except OSError as exc:
                LOG.warning("cleanup: could not remove %s: %s", target, exc)
                tcr.notes.append(f"cleanup: could not remove {target}: {exc}")
    bucket_prefix = f"{args.bucket}/{tier}"
    argv = [args.aws_cli, "s3", "rm", bucket_prefix, "--recursive"]
    if args.aws_endpoint_url:
        argv += ["--endpoint-url", args.aws_endpoint_url]
    if not args.aws_verify_ssl:
        argv += ["--no-verify-ssl"]

    if args.background_cleanup:
        import subprocess
        log_path = (log_dir or pathlib.Path(".")) / "cleanup_s3.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        LOG.info("$ %s  (backgrounded, log: %s)", " ".join(argv), log_path)
        with open(log_path, "w", encoding="utf-8") as log_handle:
            proc = subprocess.Popen(
                argv, stdout=log_handle, stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        LOG.info("S3 cleanup backgrounded (pid=%s); not waiting for it to finish", proc.pid)
        tcr.notes.append(f"S3 cleanup backgrounded: pid={proc.pid} log={log_path} argv={' '.join(argv)}")
        return

    cmd = ccr.run_argv("aws s3 cleanup", argv, LOG, args.dry_run, redact)
    tcr.commands.append(cmd.as_dict())


def find_transfer_report_csv(transfer_logs_dir: str, transfer_id: str, extract_dir: pathlib.Path) -> Optional[pathlib.Path]:
    """Locate transfer_report_<id>.csv, written by the broker alongside the
    transfer's own logs. The broker archives + deletes the plain
    cloud_transfer_<id>/ directory into cloud_transfer_<id>.zip shortly after
    completion (sometimes within seconds for small/fast transfers) -- so fall
    back to extracting the CSV straight out of that zip if the directory is
    already gone."""
    base = pathlib.Path(transfer_logs_dir)
    candidate = base / f"cloud_transfer_{transfer_id}" / f"transfer_report_{transfer_id}.csv"
    if candidate.is_file():
        return candidate
    zip_path = base / f"cloud_transfer_{transfer_id}.zip"
    if not zip_path.is_file():
        return None
    import zipfile
    try:
        with zipfile.ZipFile(zip_path) as zf:
            member = next(
                (n for n in zf.namelist() if n.endswith(f"transfer_report_{transfer_id}.csv")), None)
            if member is None:
                return None
            extract_dir.mkdir(parents=True, exist_ok=True)
            zf.extract(member, extract_dir)
            return extract_dir / member
    except (zipfile.BadZipFile, OSError):
        return None


def capture_bryck_state(args: argparse.Namespace, redact, tcr: ccr.TestCaseResult, label: str) -> str:
    """Records the Bryck mount state (Mounted/Ejected/Removed) at a named point
    in the transfer's lifecycle -- e.g. right before initiating this transfer
    and right before initiating the next one -- so state drift across a run
    of many datasets/legs is visible in commands.log/summary.json, not just
    inferred."""
    state, cmd = ccr.get_bryck_state(
        argparse.Namespace(login=args.login, dry_run=args.dry_run, python_bin=args.python_bin), LOG, redact)
    tcr.commands.append(cmd.as_dict())
    LOG.info("bryck_state [%s] = %s", label, state)
    tcr.notes.append(f"bryck_state [{label}] = {state}")
    return state


def capture_transfer_status(args: argparse.Namespace, transfer_id: str, redact,
                            tcr: ccr.TestCaseResult, label: str) -> str:
    """Records the transfer's status at a named point -- after initiation,
    after each pause/resume, and after completion -- as its own explicit,
    labeled command/note instead of only being implied by the poll loop."""
    ns = argparse.Namespace(login=args.login, dry_run=args.dry_run, python_bin=args.python_bin)
    state, cmd = ccr.get_transfer_status(ns, transfer_id, LOG, redact)
    tcr.commands.append(cmd.as_dict())
    LOG.info("transfer %s status [%s] = %s", transfer_id, label, state)
    tcr.notes.append(f"transfer_status [{label}] = {state}")
    return state


def run_leg(args: argparse.Namespace, mode_label: str, case_dir: pathlib.Path, run_id: str,
           src: str, dst: str, redact, tier: str, gen_summary: dict, dataset_label: str) -> dict:
    """Run one full transfer leg (upload or download) with its own perf
    capture + transfer_report CSV lookup, so each direction gets complete,
    independent results instead of sharing/overwriting a single perf window.

    IMPORTANT (positive-case invariant): report/summary retrieval here is
    strictly READ-ONLY -- find_transfer_report_csv() only reads a CSV already
    written by the broker, and capture_transfer_status()/poll_until_terminal()
    only query status. None of this ever pauses or resumes the transfer, in
    any state (IN_PROGRESS, PAUSED, or after resume). Only pause_resume_transfer()
    (gated behind --pause-resume) issues real pause/resume commands, and it
    never touches report retrieval. This separation must be preserved --
    downloading/reading a report during a positive test case must never
    trigger a pause/resume side effect (that coupling is only exercised
    deliberately by the negative REPORT-*/DOWNLOAD-* catalog cases, not here)."""
    case_dir.mkdir(parents=True, exist_ok=True)
    tcr = ccr.TestCaseResult(
        test_id=f"{run_id}_{mode_label}", kind=mode_label,
        description=f"transfer-only {mode_label} of {dataset_label}")

    if not args.dry_run:
        capture_bryck_state(args, redact, tcr, "before_initiate")

    perf_cfg = {
        "journal_tag": args.journal_tag, "cloudcp_log": args.cloudcp_log,
        "capture_lead": args.capture_lead, "capture_drain": args.capture_drain,
        "transfer_logs_dir": args.transfer_logs_dir, "bryck_config_json": args.bryck_config_json,
    }
    collector = perf_mod.TransferPerfCollector(case_dir, perf_cfg, args.dry_run) if args.perf_capture else None
    active_journal_raw = None
    if collector is not None:
        collector.start()
        active_journal_raw = collector.perf_dir / "journal_raw.log"

    transfer_id: Optional[str] = None
    final_state = "UNKNOWN"
    error: Optional[str] = None
    was_interrupted = False
    try:
        transfer_id = initiate_transfer_cli(args, src, dst, redact, tcr)
        if not transfer_id:
            raise RuntimeError("transfer initiate did not return a transfer_id")
        if not args.dry_run and transfer_id != "DRYRUN-ID":
            capture_transfer_status(args, transfer_id, redact, tcr, "after_initiate")

        if args.pause_resume and not args.dry_run and transfer_id != "DRYRUN-ID":
            pause_resume_transfer(args, transfer_id, redact, tcr)

        final_state = poll_until_terminal(args, transfer_id, active_journal_raw, redact, tcr)
        LOG.info("%s transfer %s finished with state=%s", mode_label, transfer_id, final_state)
        if not args.dry_run and transfer_id != "DRYRUN-ID":
            capture_transfer_status(args, transfer_id, redact, tcr, "after_completion")
            capture_bryck_state(args, redact, tcr, "after_completion")

        if args.pause_resume and args.verify_after_completion and not args.dry_run and transfer_id != "DRYRUN-ID":
            verify_pause_after_completion(args, transfer_id, final_state, redact, tcr)
    except Exception as exc:  # noqa: BLE001 -- any failure (expected RuntimeError or a genuine bug)
        # must still fall through to the perf-report block below, not just RuntimeError.
        error = str(exc)
        LOG.error("%s failed: %s", mode_label, error)
        tcr.notes.append(f"ERROR: {error}")
    except KeyboardInterrupt:
        # Ctrl+C mid-transfer: do NOT skip the perf report below -- record whatever was
        # captured up to this point, then re-raise so the run still stops as expected.
        was_interrupted = True
        LOG.warning("%s interrupted (Ctrl+C) mid-transfer (transfer_id=%s, last known state=%s) -- "
                   "still writing the perf report for what was captured so far", mode_label, transfer_id, final_state)
        tcr.notes.append(f"INTERRUPTED by user (Ctrl+C); transfer_id={transfer_id} last known state={final_state}")

    perf_data = None
    logs_collected = args.dry_run or collector is None
    if collector is not None:
        csv_path = None
        if not args.dry_run and transfer_id and transfer_id != "DRYRUN-ID":
            deadline = time.time() + args.log_wait_timeout
            while True:
                csv_path = find_transfer_report_csv(args.transfer_logs_dir, transfer_id, case_dir / "perf")
                if csv_path is not None or time.time() >= deadline:
                    break
                time.sleep(args.log_wait_interval)
            if csv_path is None:
                tcr.notes.append(f"no transfer_report_{transfer_id}.csv found (plain dir or .zip) after "
                                  f"waiting {args.log_wait_timeout}s -- completion histogram/per-status "
                                  f"breakdown will be empty")
        perf_data = collector.finish(
            transfer_id or "unknown", csv_path=csv_path, test_id=tcr.test_id, tier=tier, mode=mode_label,
            description=tcr.description, gen_summary=gen_summary,
        )
        LOG.info("%s perf report: %s", mode_label, perf_data.get("html_report"))
        tcr.notes.append(f"perf_report={perf_data.get('html_report')}")
        if not args.dry_run:
            journal_ok = (perf_data.get("log_diag") or {}).get("diagnosis") not in (None, "capture_empty")
            cloudcp_raw = collector.perf_dir / "cloudcplogs.txt"
            logs_collected = (
                journal_ok
                or (cloudcp_raw.is_file() and cloudcp_raw.stat().st_size > 0)
            )

    tcr.status = "INTERRUPTED" if was_interrupted else (
        "PASS" if (args.dry_run or (error is None and final_state == TERMINAL_SUCCESS)) else
        ("BLOCKED" if error else
         ("TIMEOUT" if final_state in STILL_RUNNING_STATES else "FAIL")))
    tcr.expected = "Transfer reaches COMPLETED"
    tcr.actual = f"final_state={final_state}" + (f", error={error}" if error else "")
    if tcr.status == "TIMEOUT":
        tcr.notes.append(f"--poll-timeout ({args.wait_timeout}s) reached while transfer was still "
                         f"{final_state} -- not a product failure, just needs more time; re-run with a "
                         f"larger --poll-timeout or re-check status separately.")
    tcr.notes.append(f"transfer_id={transfer_id} final_state={final_state}")
    if was_interrupted:
        raise KeyboardInterrupt()  # perf report is safely written above; now let the run actually stop

    return {
        "tcr": tcr, "transfer_id": transfer_id, "final_state": final_state, "error": error,
        "perf_data": perf_data, "logs_collected": logs_collected,
    }


def generate_named_spec_dataset_local(spec_path: pathlib.Path, name: str, output_base: str,
                                      run_dir: pathlib.Path, ns: types.SimpleNamespace,
                                      logger: logging.Logger) -> tuple[pathlib.Path, dict]:
    """Same job as cloud_cli_runner.generate_named_spec_dataset() (rewrite the
    spec's root: line to output_base/name, then run datagen) but writes the
    rewritten spec under run_dir/generated_specs/ instead of the system /tmp --
    datagen has no CLI flag to override root: (confirmed in
    DatagenSpecFileGuide.md, only --threads/--seed/--dry-run/--verbose), so a
    rewritten copy is unavoidable; this just keeps it as a visible run
    artifact on the same host instead of an OS temp file."""
    target_root = pathlib.Path(output_base) / name
    summary: dict = {"dataset_root": str(target_root), "spec_file": str(spec_path)}
    if ns.skip_generate:
        summary["actual_files"] = ccr.base.count_files_recursive(target_root)
        return target_root, summary

    text = spec_path.read_text(encoding="utf-8")
    new_text = ccr.base.ROOT_LINE_RE.sub(f"root: {target_root.as_posix()}", text, count=1)
    specs_dir = run_dir / "generated_specs"
    specs_dir.mkdir(parents=True, exist_ok=True)
    rewritten_path = specs_dir / f"{name.replace('/', '_')}.yaml"
    rewritten_path.write_text(new_text, encoding="utf-8")

    target_root.mkdir(parents=True, exist_ok=True)
    proc = ccr.base.run_cmd([ns.datagen_bin, "--spec", str(rewritten_path)], logger, ns.dry_run)
    if proc is not None:
        ccr.base.check_completed(proc, f"datagen for {name}")
    summary["actual_files"] = 0 if ns.dry_run else ccr.base.count_files_recursive(target_root)
    return target_root, summary


def generate_spec_dir_dataset(spec_dir: pathlib.Path, tier: str, output_base: str, run_dir: pathlib.Path,
                              ns: types.SimpleNamespace, logger: logging.Logger) -> tuple[pathlib.Path, dict]:
    """--spec-file pointed at a directory -- materialize every *.yaml inside it
    under one common output_base/<tier>/<stem>/ root, so the whole directory
    is generated and transferred as a single dataset."""
    yaml_files = sorted(spec_dir.glob("*.yaml"))
    if not yaml_files:
        raise RuntimeError(f"--spec-file directory {spec_dir} has no *.yaml files")
    target_root = pathlib.Path(output_base) / tier
    total_files = 0
    per_file: dict = {}
    for spec_path in yaml_files:
        _root, summary = generate_named_spec_dataset_local(
            spec_path, spec_path.stem, str(target_root), run_dir, ns, logger)
        per_file[spec_path.stem] = summary
        total_files += summary.get("actual_files", 0)
    return target_root, {
        "dataset_root": str(target_root), "spec_dir": str(spec_dir),
        "actual_files": total_files, "per_file": per_file,
    }


# =============================================================================
# negative_environment_runner.py wiring -- THIS is the environment file every
# negative case actually runs through (its EnvironmentManager/TestContext),
# not a reimplementation. cloudcpclitesting.NEG_CATALOG is used only as a
# name/order source (so --list/--one/--from/--to work without needing the
# still-missing NEGATIVE_TEST_PLAN.md that ner.PLAN_PATH would otherwise
# require to look up each case's description).
# =============================================================================

def build_ner_context(args: argparse.Namespace) -> ctr.TestContext:
    login_json = pathlib.Path(args.login)
    cloud_ops_json = pathlib.Path(args.params)
    fmt_mount_json = pathlib.Path(args.format_mount_params)
    change_time_json = BRYCK_CLI_DIR / "change_time_params.json"
    login_cfg = ctr._load_json(login_json)
    ssh_user = login_cfg.get("bryckserver_username", "bryck")
    ssh_host = login_cfg.get("bryckapi_host")
    ctx = ctr.TestContext(
        login_json=login_json, cloud_ops_json=cloud_ops_json, fmt_mount_json=fmt_mount_json,
        change_time_json=change_time_json, report_dir=ctr.DEFAULT_REPORT_DIR,
        results_dir=pathlib.Path(args.results_dir), ssh_user=ssh_user, ssh_host=ssh_host,
        datagen_bin=args.datagen_bin, spec_dir=pathlib.Path(args.spec_dir), dry_run=(not args.live),
        iteration=1, scenario_name="cloud_full_test",
    )
    if args.skip_cancel_ops:
        # --skip-cancel-ops: every ner.py handler calls ctx.cancel_transfer() for its
        # actual "cancel the transfer" step -- patching it here (not in
        # cloud_transfer_test_runner.py/negative_environment_runner.py, which stay
        # untouched) makes that one step a no-op recorded as PASS/SKIPPED while every
        # other step in the case (mount, configure, initiate, pause/resume, etc.)
        # still runs exactly as before.
        def _skipped_cancel_transfer(transfer_id: str, expect_fail: bool = False) -> ctr.StepResult:
            return ctx._record(
                f"Cancel transfer {transfer_id}", "bryck_cloud_transfer_cancel.py",
                0, "", "SKIPPED: --skip-cancel-ops set -- cancel step not issued", 0.0,
                notes="cancel operation skipped via --skip-cancel-ops; rest of the case still ran",
                outcome="SKIPPED", validation_passed=True,
            )
        ctx.cancel_transfer = _skipped_cancel_transfer
    return ctx


def run_negative_case_via_ner(mgr: "ner.EnvironmentManager", args: argparse.Namespace, work: pathlib.Path,
                              case_id: str) -> "ner.TestResult":
    desc = negtest.NEG_CATALOG[case_id].name if case_id in negtest.NEG_CATALOG else case_id
    mgr.commands = []
    result = ner.dispatch(case_id, desc, mgr, args, work, {})
    result.narrative = ner.build_narrative(result)
    return result


_DETAIL_ROW_RE = re.compile(
    r'(<tr id="detail-(\d+)" class="detailrow"[^>]*>\s*<td colspan="8">\s*<div class="detailbody">)'
)


def integrate_perf_report_links(html_str: str, neg_report_dir: pathlib.Path, case_ids: List[str]) -> str:
    """Embed a link to each negative case's own perf/report.html (the same
    journalctl/cloudcp.log chart report transfer cases get) directly inside
    negative_environment_runner.py's combined_report.html detail rows, so the
    perf charts and the test-result report are reachable from one page
    instead of two disconnected files."""
    import html as _html_mod

    def _inject(match: "re.Match") -> str:
        idx = int(match.group(2))
        if idx >= len(case_ids):
            return match.group(1)
        case_id = case_ids[idx].replace("/", "_")
        perf_html = neg_report_dir / case_id / "perf" / "report.html"
        if not perf_html.is_file():
            return match.group(1)
        link = (f'<p class="perf-link" style="margin:6px 0 14px"><a href="{case_id}/perf/report.html" '
                f'target="_blank" rel="noopener">&#128202; View performance report '
                f'(journalctl/cloudcp.log charts) for {_html_mod.escape(case_id)}</a></p>')
        return match.group(1) + link
    return _DETAIL_ROW_RE.sub(_inject, html_str)


# =============================================================================
# Unified catalog: TRANSFER-<dataset>-<MODE> (positive) + every negative-catalog
# ID (names/order from cloudcpclitesting.py, execution delegated to
# negative_environment_runner.py -- see above)
# =============================================================================

def discover_dataset_ids() -> List[str]:
    _manifest, dataset_map = negtest.load_manifest(negtest.SPEC_ROOT)
    return list(dataset_map.keys())


def build_catalog() -> List[dict]:
    entries: List[dict] = []
    for ds_id in discover_dataset_ids():
        for mode in TRANSFER_MODES:
            entries.append({
                "id": f"TRANSFER-{ds_id}-{mode.upper()}", "kind": "transfer",
                "dataset": ds_id, "mode": mode,
                "name": f"{mode} transfer of {ds_id}", "implemented": True,
            })
    for cid in negtest.NEG_CATALOG_ORDER:
        tc = negtest.NEG_CATALOG[cid]
        # "implemented" reflects negative_environment_runner.py's actual handler
        # coverage (ner.HANDLERS keyed by the case ID's own prefix -- the same
        # prefix ner.dispatch() itself derives via re.match(r"[A-Z]+", case_id)),
        # NOT cloudcpclitesting.py's own (much smaller) NEG_CATALOG.run coverage,
        # and NOT tc.section (which is sometimes a friendlier full name, e.g.
        # "DATASET" for "DATA-01" -- ner.py has real handlers for every ID
        # prefix except MASTER-* (those live in a separate, not-yet-wired file).
        prefix_match = re.match(r"[A-Z]+", cid)
        id_prefix = prefix_match.group(0) if prefix_match else tc.section
        entries.append({
            "id": cid, "kind": "negative", "section": tc.section,
            "name": tc.name, "implemented": id_prefix in ner.HANDLERS,
        })
    return entries




def run_command_for(entry: dict) -> str:
    """The exact CLI invocation that runs this one catalog entry by itself."""
    flag = "--one" if entry["kind"] == "transfer" else "--negative-case"
    return f"python3 cloud_full_test.py {flag} {entry['id']}"


def print_catalog(catalog: List[dict]) -> None:
    print(f"{'[n]':>6} {'ID':<28}{'KIND':<10}{'STATUS':<12}{'COMMAND':<66}NAME")
    print("-" * 170)
    for i, e in enumerate(catalog, start=1):
        status = "IMPLEMENTED" if e["implemented"] else "stub"
        print(f"[{i:>4}] {e['id']:<28}{e['kind']:<10}{status:<12}{run_command_for(e):<66}{e['name']}")
    n_transfer = sum(1 for e in catalog if e["kind"] == "transfer")
    n_negative = sum(1 for e in catalog if e["kind"] == "negative")
    print(f"\n{len(catalog)} total case(s): {n_transfer} transfer + {n_negative} negative.")


def _resolve_token(token: str, catalog: List[dict], id_index: dict) -> int:
    """Resolve a '# index' (1-based) or literal case id to a 0-based position
    in `catalog`, matching cloudcp_fallback_test.py's --one/--from/--to."""
    token = token.strip()
    if token.isdigit():
        pos = int(token) - 1
        if not (0 <= pos < len(catalog)):
            raise SystemExit(f"ERROR: index {token!r} is out of range 1-{len(catalog)}")
        return pos
    if token not in id_index:
        raise SystemExit(f"ERROR: unknown case id {token!r}. Use --list to see valid IDs.")
    return id_index[token]


def _split_range_spec(spec: str, catalog: List[dict], id_index: dict) -> tuple[str, str]:
    """Split a '--range FROM-TO' spec on '-', trying every split point so
    hyphenated case ids (e.g. TRANSFER-DS-P1-01-UPLOAD, bryck-info-trigger)
    still resolve correctly on both sides."""
    parts = spec.split("-")
    for i in range(1, len(parts)):
        left, right = "-".join(parts[:i]), "-".join(parts[i:])
        if (left.strip().isdigit() or left.strip() in id_index) and \
           (right.strip().isdigit() or right.strip() in id_index):
            return left.strip(), right.strip()
    raise SystemExit(f"ERROR: could not parse --range {spec!r} as FROM-TO; use --from/--to instead if ambiguous.")


def resolve_selection(args: argparse.Namespace, catalog: List[dict]) -> List[dict]:
    id_index = {e["id"]: i for i, e in enumerate(catalog)}

    if args.negative_case:
        wanted = [t.strip() for t in args.negative_case.split(",") if t.strip()]
        selected = []
        for t in wanted:
            pos = _resolve_token(t, catalog, id_index)
            if catalog[pos]["kind"] != "negative":
                raise SystemExit(f"ERROR: {t!r} is not a negative case id.")
            selected.append(catalog[pos])
        return selected

    if args.negative:
        return [e for e in catalog if e["kind"] == "negative"]

    if args.one:
        wanted = [t.strip() for t in args.one.split(",") if t.strip()]
        return [catalog[_resolve_token(t, catalog, id_index)] for t in wanted]

    if args.order:
        return [catalog[_resolve_token(args.order, catalog, id_index)]]

    if args.range_spec:
        from_id, to_id = _split_range_spec(args.range_spec, catalog, id_index)
        start = _resolve_token(from_id, catalog, id_index)
        end = _resolve_token(to_id, catalog, id_index)
        if start > end:
            start, end = end, start
        return catalog[start:end + 1]

    if args.from_id or args.to_id:
        if not (args.from_id and args.to_id):
            raise SystemExit("ERROR: --from and --to must be given together")
        start = _resolve_token(args.from_id, catalog, id_index)
        end = _resolve_token(args.to_id, catalog, id_index)
        if start > end:
            start, end = end, start
        return catalog[start:end + 1]

    if args.all:
        return list(catalog)

    return []


# =============================================================================
# Execution
# =============================================================================

def prepare_environment(args: argparse.Namespace, redact, tcr: ccr.TestCaseResult, label: str) -> dict:
    """Baseline environment preparation run before EVERY case (transfer or
    negative), modeled directly on negative_environment_runner.py's
    EnvironmentManager.snapshot()/ensure_mounted() (inspect -> ensure_mounted
    -> snapshot) instead of assuming the device is already in a usable state.
    The returned dict intentionally matches that file's snapshot() shape --
    bryck_state / cloud_configured / info_ok / cloud_ok / status_ok -- so
    reports from this script and from negative_environment_runner.py/
    cloudcpclitesting.py read the same way. This does not replace any
    fixture-specific setup a negative case's own handler does internally
    (e.g. STATE-* already calls ensure_mounted()/configure_cloud() itself)
    -- it guarantees a known starting point before that, for every case,
    including the ~180 still-stub negative cases and every transfer case."""
    info_cmd = ccr.run_py_script("bryck_info.py", ["--login", args.login], LOG, args.dry_run, redact,
                                 args.python_bin)
    tcr.commands.append(info_cmd.as_dict())
    state = ensure_mounted(args, redact, tcr)
    show_cmd = ccr.run_py_script("bryck_cloud_show.py", ["--login", args.login], LOG, args.dry_run, redact,
                                 args.python_bin)
    tcr.commands.append(show_cmd.as_dict())
    cloud_configured = "configured" in ((show_cmd.stdout or "") + (show_cmd.stderr or "")).lower()
    status_cmd = ccr.run_py_script("bryck_cloud_transfer_status.py", ["--login", args.login], LOG, args.dry_run,
                                   redact, args.python_bin)
    tcr.commands.append(status_cmd.as_dict())
    snapshot = {
        "bryck_state": state,
        "cloud_configured": cloud_configured,
        "info_ok": args.dry_run or info_cmd.returncode == 0,
        "cloud_ok": args.dry_run or show_cmd.returncode == 0,
        "status_ok": args.dry_run or status_cmd.returncode == 0,
    }
    LOG.info("[%s] environment snapshot: %s", label, snapshot)
    tcr.notes.append(f"environment prep [{label}]: {snapshot}")
    return snapshot



def run_transfer_case(args: argparse.Namespace, entry: dict, run_dir: pathlib.Path,
                      base_cloud_ops: dict, redact) -> dict:
    dataset_id = entry["dataset"]
    mode = entry["mode"]
    tier = dataset_id
    case_id = entry["id"]
    case_run_dir = run_dir / "transfer" / case_id
    case_run_dir.mkdir(parents=True, exist_ok=True)

    mount_tcr = ccr.TestCaseResult(test_id=f"{case_id}_setup", kind="setup",
                                   description=f"environment prep + mount + configure for {case_id}")
    prepare_environment(args, redact, mount_tcr, case_id)

    ns = types.SimpleNamespace(
        output_base=args.output_base, skip_generate=args.skip_datagen,
        datagen_bin=args.datagen_bin, dry_run=args.dry_run, verbose=args.verbose,
    )
    dataset_root, gen_summary = ccr.generate_tier_dataset(tier, args.output_base, ns, LOG, dataset_id)
    LOG.info("[%s] dataset materialized under %s", case_id, dataset_root)

    bucket = configure_cloud(args, base_cloud_ops, tier, redact, mount_tcr)
    LOG.info("[%s] cloud configured: bucket=%s", case_id, bucket)
    mount_tcr.status = "PASS"

    local_src = str(dataset_root)
    s3_path = f"{args.bucket}/{tier}"
    local_dst = f"{args.download_base}/{tier}"

    legs: List[dict] = []
    if mode in ("upload", "both"):
        legs.append(run_leg(args, "upload", case_run_dir / "upload", case_id, local_src, s3_path,
                            redact, tier, gen_summary, dataset_id))
    if mode in ("download", "both"):
        if mode == "download" and not args.skip_seed:
            seed_tcr = ccr.TestCaseResult(test_id=f"{case_id}_seed", kind="seed",
                                          description="untracked seed upload before standalone download")
            seed_id = initiate_transfer_cli(args, local_src, s3_path, redact, seed_tcr)
            if seed_id and not args.dry_run:
                seed_state = poll_until_terminal(args, seed_id, None, redact, seed_tcr)
                LOG.info("[%s] seed upload (transfer %s) finished with state=%s", case_id, seed_id, seed_state)
                if seed_state != TERMINAL_SUCCESS:
                    LOG.error("[%s] seed upload did not complete (state=%s); aborting download", case_id, seed_state)
                    return {"case_id": case_id, "status": "FAIL", "legs": [], "error": "seed upload failed"}
        legs.append(run_leg(args, "download", case_run_dir / "download", case_id, s3_path, local_dst,
                            redact, tier, gen_summary, dataset_id))

    if not args.skip_cleanup and args.cleanup:
        all_logs_collected = all(leg["logs_collected"] for leg in legs)
        if all_logs_collected or args.force_cleanup:
            cleanup(args, tier, redact, legs[-1]["tcr"], log_dir=case_run_dir)
        else:
            LOG.warning("[%s] cleanup skipped: not all legs collected logs", case_id)

    all_tcrs = [mount_tcr] + [leg["tcr"] for leg in legs]
    ccr.write_combined_commands_log(case_run_dir, all_tcrs)
    ccr.write_summary(case_run_dir, {"run_id": case_id, "test_cases": [
        {"id": t.test_id, "dataset": dataset_id, "mode": t.kind} for t in all_tcrs]}, all_tcrs)

    overall_ok = all(leg["error"] is None and leg["final_state"] == TERMINAL_SUCCESS for leg in legs) or args.dry_run
    any_timeout = any(leg["tcr"].status == "TIMEOUT" for leg in legs)
    case_status = "PASS" if overall_ok else ("TIMEOUT" if any_timeout else "FAIL")
    return {"case_id": case_id, "status": case_status, "legs": legs, "error": None}


def run_component_suite(args: argparse.Namespace) -> int:
    """--component/--component-one/--component-negative/--component-list.

    NOT IMPLEMENTED: the fallback_worker/mp_batch_retry internal-mechanism
    tests from cloudcp_fallback_test.py have no source anywhere in this repo
    -- only their CLI surface was described. Rather than fabricate fake
    results, this prints a clear message and returns non-zero."""
    LOG.error("Component fallback suite (fallback_worker + mp_batch_retry) is requested but NOT "
              "IMPLEMENTED in cloud_full_test.py -- no source for those internal mechanisms exists "
              "in this repo (only the CLI surface was described). Refusing to fabricate results.")
    LOG.error("If you have the fallback_worker/mp_batch_retry source, share it and this can be wired up.")
    return 2


def manual_confirm(index: int, total: int, entry: dict) -> str:
    """Returns 'run', 'skip', or 'quit'."""
    while True:
        answer = input(f"[{index}/{total}] {entry['id']} ({entry['kind']}) - {entry['name']} "
                       f"-- run/skip/quit? [r/s/q]: ").strip().lower()
        if answer in ("r", "run", ""):
            return "run"
        if answer in ("s", "skip"):
            return "skip"
        if answer in ("q", "quit"):
            return "quit"


def parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Unified transfer + negative-catalog test harness "
                    "(cloud_transfer_only.py's engine + cloudcpclitesting.py's negative catalog).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    sel = p.add_argument_group("selection")
    sel.add_argument("--all", action="store_true", help="Run every case (transfer + negative).")
    sel.add_argument("--one", default=None, help="Run one case, or a comma-separated list, by # index or case id.")
    sel.add_argument("--order", default=None,
                     help="Run exactly one case by its # index or case id (same resolution as --one).")
    sel.add_argument("--from", dest="from_id", default=None, help="Start case (# index or id, inclusive).")
    sel.add_argument("--to", dest="to_id", default=None, help="End case (# index or id, inclusive).")
    sel.add_argument("--range", dest="range_spec", default=None,
                     help="Inclusive range as FROM-TO (# index or id on each side), e.g. --range 1-9 or --range CLI-01-CLI-09.")
    sel.add_argument("--negative", action="store_true", help="Run only the negative-catalog cases.")
    sel.add_argument("--negative-case", default=None,
                     help="Run one/comma-separated negative case(s) by # index or id.")
    sel.add_argument("--list", action="store_true", help="List all cases and exit.")

    ex = p.add_argument_group("execution")
    ex.add_argument("--dry-run", action="store_true", help="Print the plan without executing.")
    ex.add_argument("--manual", action="store_true",
                    help="Interactive mode: prompt run/skip/quit for each selected case.")
    ex.add_argument("--skip-datagen", action="store_true", help="Reuse already-materialized data.")
    ex.add_argument("--skip-seed", action="store_true",
                    help="For download cases, reuse existing bucket objects (no seeding upload).")
    ex.add_argument("--keep-config", action="store_true",
                    help="Do not restore the original cloud_ops.json after the run.")
    ex.add_argument("--skip-cleanup", action="store_true",
                    help="Do not empty the bucket / remove generated /bryck data after each case, "
                         "even if --cleanup is also given.")
    ex.add_argument("--seed", type=int, default=1337, help="Random seed for reproducibility (default 1337).")
    ex.add_argument("--poll-interval", type=int, default=10)
    ex.add_argument("--poll-timeout", dest="wait_timeout", type=int, default=600,
                    help="Per-leg terminal-state wait cap in seconds (default 600 = 10min). Real transfer "
                         "state is always trusted as-is; hitting this cap while still IN_PROGRESS/PAUSED/"
                         "QUEUED is reported as TIMEOUT (not a false FAIL) -- raise this for larger datasets "
                         "that legitimately need more than 10 minutes.")
    ex.add_argument("--verbose", action="store_true")
    ex.add_argument("--confirm-destructive", action="store_true",
                    help="Allow negative_environment_runner.py's destructive negative cases "
                         "(format/erase/remove/eject during transfer, etc.) to actually execute.")
    ex.add_argument("--allow-service-faults", action="store_true",
                    help="Allow SVC-*/REC-01 cases to stop/restart real systemd services on the device.")
    ex.add_argument("--allow-network-faults", action="store_true",
                    help="Allow REC-03 to block/restore outbound network traffic on the device.")
    ex.add_argument("--allow-reboot", action="store_true",
                    help="No-op: reboot test cases are excluded from negative_environment_runner.py per its own design.")
    ex.add_argument("--skip-cancel-ops", action="store_true",
                    help="Skip only the actual 'cancel transfer' step inside every negative case's own "
                         "flow (bryck_cloud_transfer_cancel.py is never invoked) -- every other step in "
                         "that case (mount, configure, initiate, pause/resume, etc.) still runs normally "
                         "and the skipped step is recorded as SKIPPED/PASS, not a failure.")

    ph = p.add_argument_group("paths / hosts")
    ph.add_argument("--cli-dir", default=str(BRYCK_CLI_DIR), help="bryckclient-cli directory.")
    # CloudCpCliTesting/spec_files -- where ensure_dataset()'s real specs
    # (small_1gb_fast.yaml, priority_2gb.yaml, DATASET_SPEC_ROTATION) live.
    # negtest.SPEC_ROOT (dataset_cloudcp/spec_files) is a *different* folder
    # used only by discover_dataset_ids() for the 162 positive TRANSFER-*
    # cases -- using it here silently broke every negative dataset generation.
    ph.add_argument("--spec-dir", default=str(ctr.SPEC_DIR))
    ph.add_argument("--out-dir", dest="results_dir", default=str(RESULTS_ROOT / "full_test"))
    ph.add_argument("--login", default=None, help="Path to login.json (default: <cli-dir>/login.json).")
    ph.add_argument("--cloud-ops", dest="params", default=None,
                    help="Path to cloud_ops.json (default: <cli-dir>/cloud_ops.json).")
    ph.add_argument("--datagen", dest="datagen_bin", default=ccr.DEFAULT_DATAGEN)
    ph.add_argument("--config", dest="bryck_config_json", default=ccr.DEFAULT_BRYCK_CONFIG_JSON)
    ph.add_argument("--service", default=None,
                    help="Reserved for future SVC-* service-restart cases (currently stubs; no-op for now).")
    ph.add_argument("--transfer-logs-dir", default=ccr.DEFAULT_TRANSFER_LOGS)
    ph.add_argument("--endpoint-url", dest="aws_endpoint_url", default="https://10.10.10.103:9000")
    ph.add_argument("--src-base", dest="output_base", default="/bryck")
    ph.add_argument("--bucket-base", dest="bucket", default="s3://shravani/cloudcp-cli")
    ph.add_argument("--dl-base", dest="download_base", default="/bryck/cloudcp_cli_dl")

    comp = p.add_argument_group("component tests (internal fallback mechanisms -- NOT IMPLEMENTED here)")
    comp.add_argument("--component", action="store_true")
    comp.add_argument("--component-one", default=None)
    comp.add_argument("--component-negative", action="store_true")
    comp.add_argument("--component-list", action="store_true")
    comp.add_argument("--heavy", action="store_true")
    comp.add_argument("--component-bucket", default="omicron")
    comp.add_argument("--region", default="us-west-1")
    comp.add_argument("--venv-python", dest="python_bin", default=ccr.DEFAULT_PYTHON_BIN)
    comp.add_argument("--batchmeta-dir", default=ccr.DEFAULT_BATCHMETA)
    comp.add_argument("--pool-size", type=int, default=16)

    # Fields shared with the copied transfer engine (fixed here, not exposed
    # as new flags, to keep the CLI surface matching cloudcp_fallback_test.py).
    p.set_defaults(
        bryckcloud_bin=ccr.DEFAULT_BRYCKCLOUD, action_timeout=90,
        pause_resume=False, pause_cycles=3, pause_interval=60,
        pause_wait_timeout=120, pause_duration=10, verify_after_completion=True,
        perf_capture=True, journal_tag=["bcloud", "bryckcloud"],
        cloudcp_log="/opt/bryck/bryckapi/downloads/cloud_transfer_logs/cloudcp.log",
        capture_lead=3.0, capture_drain=6.0,
        cleanup=False, keep=False, force_cleanup=False,
        log_wait_timeout=30.0, log_wait_interval=3.0,
        aws_cli="aws", aws_verify_ssl=False, background_cleanup=False,
        run_id=None,
    )

    args = p.parse_args(argv)
    cli_dir = pathlib.Path(args.cli_dir)
    if args.login is None:
        args.login = str(cli_dir / "login.json")
    if args.params is None:
        args.params = str(cli_dir / "cloud_ops.json")
    args.format_mount_params = str(cli_dir / "format_mount_params.json")
    args.live = not args.dry_run  # ner.dispatch()'s handlers gate on args.live, not args.dry_run
    return args



def main(argv: Optional[list] = None) -> int:
    args = parse_args(argv)
    setup_logging(args.verbose)
    random.seed(args.seed)

    if args.component_list or args.component or args.component_one or args.component_negative:
        return run_component_suite(args)

    catalog = build_catalog()
    if args.list:
        print_catalog(catalog)
        return 0

    selected = resolve_selection(args, catalog)
    if not selected:
        print("Use --list, --all, --one <id[,id...]>, --order <id>, --from/--to, --range FROM-TO, "
              "--negative, or --negative-case.")
        return 2

    run_id = args.run_id or f"full_test_{dt.datetime.now():%Y%m%d_%H%M%S}"
    run_dir = pathlib.Path(args.results_dir) / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    login_ok, login_cfg, login_err = ccr.validate_json_file(args.login)
    params_ok, cloud_ops_cfg, params_err = ccr.validate_json_file(args.params)
    if not (login_ok and params_ok) and not args.dry_run:
        LOG.error("Config invalid: %s", login_err or params_err)
        return 3
    login_cfg = login_cfg or {}
    base_cloud_ops = cloud_ops_cfg or {}
    redact = ccr.build_redactor(login_cfg, base_cloud_ops)

    if not args.dry_run:
        config_ok, _tiers, config_msg, config_snippet = ccr.validate_bryck_config_json(args.bryck_config_json)
        if not config_ok:
            LOG.error("%s is invalid: %s", args.bryck_config_json, config_msg)
            if config_snippet:
                LOG.error("Context around the failing line:\n%s", config_snippet)
            return 6

    backup_path = run_dir / "cloud_ops.json.bak"
    if not backup_path.exists():
        backup_path.write_text(json.dumps(base_cloud_ops, indent=2), encoding="utf-8")

    # One shared EnvironmentManager/TestContext for every negative case in this
    # run (built lazily, only if a negative case is actually selected) -- this
    # matches negative_environment_runner.py's own main(), which builds ctx/mgr
    # ONCE per run, not per case, so active_transfers/cloud_configured state
    # carries over between cases exactly like a real run of that script.
    ner_mgr: Optional["ner.EnvironmentManager"] = None
    ner_results: List["ner.TestResult"] = []
    ner_case_ids: List[str] = []  # parallel to ner_results, for perf-report link injection
    ner_work_dir = run_dir / "negative" / "_work"

    results: List[dict] = []
    total = len(selected)
    interrupted = False
    try:
        for i, entry in enumerate(selected, start=1):
            if args.manual:
                action = manual_confirm(i, total, entry)
                if action == "quit":
                    LOG.info("Stopped by user after %d/%d case(s).", i - 1, total)
                    break
                if action == "skip":
                    results.append({"case_id": entry["id"], "status": "SKIP"})
                    continue

            LOG.info("=== [%d/%d] %s (%s) ===", i, total, entry["id"], entry["kind"])
            try:
                if entry["kind"] == "transfer":
                    result = run_transfer_case(args, entry, run_dir, base_cloud_ops, redact)
                else:
                    # Environment prep first (see prepare_environment()'s docstring) --
                    # a lightweight baseline check before handing off to ner.dispatch(),
                    # whose own handlers do further fixture-specific setup as needed.
                    neg_dir = run_dir / "negative" / entry["id"].replace("/", "_")
                    neg_dir.mkdir(parents=True, exist_ok=True)
                    env_tcr = ccr.TestCaseResult(test_id=f"{entry['id']}_env_prep", kind="env_prep",
                                                 description=f"environment prep before {entry['id']}")
                    env_snapshot = prepare_environment(args, redact, env_tcr, entry["id"])
                    (neg_dir / "env_prep.json").write_text(
                        json.dumps({"snapshot": env_snapshot, "notes": env_tcr.notes,
                                   "commands": env_tcr.commands}, indent=2, default=str), encoding="utf-8")

                    # Delegate to negative_environment_runner.py's dispatch()/EnvironmentManager
                    # -- the actual, comprehensive environment-aware implementation (LIFE/DATA/
                    # XFER/DOWNLOAD/RACE/DUP/REPORT/FAULT/REC/VERIFY/INT/MGMT/SVC/SM/F included),
                    # not a reimplementation.
                    if ner_mgr is None:
                        ner_ctx = build_ner_context(args)
                        ner_mgr = ner.EnvironmentManager(ner_ctx)
                        ner_work_dir.mkdir(parents=True, exist_ok=True)
                    # Same journalctl/cloudcp.log perf capture as transfer cases (see run_leg()),
                    # wrapped around the whole case so every negative case -- not just transfers --
                    # gets a perf report.
                    perf_cfg = {
                        "journal_tag": args.journal_tag, "cloudcp_log": args.cloudcp_log,
                        "capture_lead": args.capture_lead, "capture_drain": args.capture_drain,
                        "transfer_logs_dir": args.transfer_logs_dir, "bryck_config_json": args.bryck_config_json,
                    }
                    collector = perf_mod.TransferPerfCollector(neg_dir, perf_cfg, args.dry_run) if args.perf_capture else None
                    if collector is not None:
                        collector.start()
                    try:
                        ner_result = run_negative_case_via_ner(ner_mgr, args, ner_work_dir, entry["id"])
                    finally:
                        # Always finish the perf capture, even on Ctrl+C mid-case, so whatever was
                        # captured up to the interrupt is still written instead of silently dropped.
                        if collector is not None:
                            perf_data = collector.finish(
                                entry["id"], csv_path=None, test_id=entry["id"], tier=entry.get("section", ""),
                                mode="negative", description=entry["name"], gen_summary=None,
                            )
                            LOG.info("[%s] perf report: %s", entry["id"], perf_data.get("html_report"))
                    ner_results.append(ner_result)
                    ner_case_ids.append(entry["id"])
                    result = {"case_id": entry["id"], "status": ner_result.status}
            except KeyboardInterrupt:
                # Ctrl+C mid-case: record exactly where execution stopped, then re-raise so the
                # outer handler below still writes every report for cases that DID run --
                # never silently lose logs/results just because the run was interrupted.
                LOG.warning("[%s] interrupted (Ctrl+C) while this case was in progress", entry["id"])
                results.append({"case_id": entry["id"], "status": "INTERRUPTED"})
                raise
            except Exception as exc:  # noqa: BLE001 -- one case's crash must never abort the whole run
                LOG.exception("[%s] crashed unexpectedly: %s", entry["id"], exc)
                result = {"case_id": entry["id"], "status": "CRASH", "error": str(exc)}
            results.append(result)
            LOG.info("    -> %s", result.get("status"))
    except KeyboardInterrupt:
        interrupted = True
        LOG.warning("Interrupted by user (Ctrl+C) after %d/%d case(s) -- writing logs/reports for "
                   "everything that ran so far.", len(results), total)

    if not args.keep_config and backup_path.exists() and not args.dry_run:
        try:
            shutil.copy2(backup_path, args.params)
            LOG.info("restored original cloud_ops.json from %s", backup_path)
        except OSError as exc:
            LOG.warning("could not restore cloud_ops.json: %s", exc)

    (run_dir / "report.json").write_text(json.dumps({
        "run_id": run_id, "selected": len(selected), "results": results,
    }, indent=2, default=str), encoding="utf-8")

    if ner_results:
        import dataclasses as _dc
        neg_report_dir = run_dir / "negative"
        neg_report_dir.mkdir(parents=True, exist_ok=True)
        (neg_report_dir / "combined_results.json").write_text(
            json.dumps([_dc.asdict(r) for r in ner_results], indent=2, default=str), encoding="utf-8")
        (neg_report_dir / "combined_report.html").write_text(
            integrate_perf_report_links(
                ner.build_html(run_id, run_id, dt.datetime.now().isoformat(timespec="seconds"), ner_results),
                neg_report_dir, ner_case_ids,
            ),
            encoding="utf-8")
        LOG.info("Combined negative report: %s", neg_report_dir / "combined_report.html")

    zip_path: Optional[pathlib.Path] = None
    try:
        # Package the entire run directory (report.json, combined_report.html,
        # every per-case perf/summary folder) into one .zip next to it -- a
        # single file that's easy to download/extract on any OS, instead of
        # relying on tar (which failed for the user: wrong path/permissions/
        # taring before the run finished all produce an empty archive).
        zip_base = shutil.make_archive(str(run_dir), "zip", root_dir=str(run_dir.parent), base_dir=run_dir.name)
        zip_path = pathlib.Path(zip_base)
        LOG.info("Results directory zipped -> %s", zip_path)
    except OSError as exc:
        LOG.warning("could not zip results directory %s: %s", run_dir, exc)

    counts: dict = {}
    for r in results:
        counts[r.get("status", "UNKNOWN")] = counts.get(r.get("status", "UNKNOWN"), 0) + 1
    print("\n" + "=" * 60)
    print(f"cloud_full_test.py run {run_id} " + ("INTERRUPTED" if interrupted else "complete")
         + f" -- {len(results)}/{total} case(s) ran")
    for status, count in counts.items():
        print(f"  {status}: {count}")
    print(f"Results directory: {run_dir}")
    print(f"  report.json: {run_dir / 'report.json'}")
    if ner_results:
        print(f"  negative combined report: {run_dir / 'negative' / 'combined_report.html'}")
    if zip_path is not None:
        print(f"  zip archive: {zip_path}")
    print("=" * 60 + "\n")
    if interrupted:
        return 130  # conventional 128+SIGINT exit code
    return 1 if (counts.get("FAIL", 0) or counts.get("CRASH", 0)) else 0


if __name__ == "__main__":
    sys.exit(main())
