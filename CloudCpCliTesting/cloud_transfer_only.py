#!/usr/bin/env python3
"""Transfer-only runner: datagen -> configure cloud -> CLI initiate ->
poll to terminal state -> perf capture -> (optional) cleanup.

No test matrix, no lifecycle/negative/edge cases -- just one real transfer,
run directly on the Bryck host (same execution model as cloud_cli_runner.py).

Reuses proven pieces instead of re-inventing them:
  - dataset generation, status polling (with journal/log-based fallbacks), and
    command/redaction plumbing from cloud_cli_runner.py;
  - transfer initiation via the same CLI command cloud_cli_runner.py's --cli
    adapter uses: `bryckcloud transfer add aws --src <path> --dst <path>`;
  - performance capture (journalctl + cloudcp.log tailing, HTML report) from
    cli_perf_capture.py, the same collector cloud_cli_runner.py uses.

Usage
-----
    python3 cloud_transfer_only.py --dataset 01_zero_byte --mode upload
    python3 cloud_transfer_only.py --dataset DS-P1-04 --mode both --run-id t1
    python3 cloud_transfer_only.py --dataset 03_small_files --dry-run
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import pathlib
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

LOG = logging.getLogger("cloud_transfer_only")

TERMINAL_SUCCESS = "COMPLETED"
TERMINAL_FAILURE = {"FAILED", "STOPPED", "CANCELLED"}
TERMINAL_STATES = {TERMINAL_SUCCESS} | TERMINAL_FAILURE


def setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stdout,
    )


def parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Run one real CloudCp transfer (datagen -> configure -> "
                     "REST-API initiate -> poll -> perf capture -> cleanup). "
                     "No test matrix -- just the transfer.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--dataset", required=True,
                   help="DS-P* manifest id (e.g. DS-P1-04) or a local spec_files/*.yaml name "
                        "(e.g. 01_zero_byte -- searches CloudCpCliTesting/spec_files and "
                        "CloudCpFallbackTesting/spec_files).")
    p.add_argument("--mode", choices=["upload", "download", "both"], default="upload")
    p.add_argument("--tier", default=None, help="Folder-name label (default: the --dataset value).")

    p.add_argument("--login", default=str(BRYCK_CLI_DIR / "login.json"))
    p.add_argument("--params", default=str(BRYCK_CLI_DIR / "cloud_ops.json"))
    p.add_argument("--format-mount-params", default=str(BRYCK_CLI_DIR / "format_mount_params.json"))

    p.add_argument("--output-base", default="/bryck")
    p.add_argument("--download-base", default="/bryck/cloudcp_cli_dl")
    p.add_argument("--bucket", default="s3://shravani/cloudcp-cli")

    p.add_argument("--datagen-bin", default=ccr.DEFAULT_DATAGEN)
    p.add_argument("--python-bin", default=ccr.DEFAULT_PYTHON_BIN)
    p.add_argument("--bryckcloud-bin", default=ccr.DEFAULT_BRYCKCLOUD,
                   help="bryckcloud CLI binary used to initiate the transfer "
                        "(transfer add aws --src <path> --dst <path>).")
    p.add_argument("--batchmeta-dir", default=ccr.DEFAULT_BATCHMETA)
    p.add_argument("--skip-datagen", action="store_true")
    p.add_argument("--skip-mount-check", action="store_true")

    p.add_argument("--poll-interval", type=int, default=10)
    p.add_argument("--wait-timeout", type=int, default=1800)
    p.add_argument("--action-timeout", type=int, default=90)

    p.add_argument("--results-dir", default=str(RESULTS_ROOT / "transfer_only"))
    p.add_argument("--run-id", default=None)

    p.add_argument("--no-perf", dest="perf_capture", action="store_false", default=True)
    p.add_argument("--journal-tag", nargs="+", default=["bcloud", "bryckcloud"])
    p.add_argument("--cloudcp-log", default="/opt/bryck/bryckapi/downloads/cloud_transfer_logs/cloudcp.log")
    p.add_argument("--capture-lead", type=float, default=3)
    p.add_argument("--capture-drain", type=float, default=6)
    p.add_argument("--bryck-config-json", default=ccr.DEFAULT_BRYCK_CONFIG_JSON)
    p.add_argument("--transfer-logs-dir", default=ccr.DEFAULT_TRANSFER_LOGS)

    p.add_argument("--keep", action="store_true", help="Skip cleanup (S3 objects + generated /bryck data).")
    p.add_argument("--force-cleanup", action="store_true",
                   help="Run cleanup even if the transfer_report CSV/logs could not be confirmed collected "
                        "(by default cleanup is skipped in that case, so evidence isn't destroyed before "
                        "you can investigate).")
    p.add_argument("--log-wait-timeout", type=float, default=30,
                   help="Seconds to wait/retry for the broker's transfer_report_<id>.csv to appear "
                        "(plain directory or .zip archive) before giving up and generating the perf "
                        "report without it.")
    p.add_argument("--log-wait-interval", type=float, default=3,
                   help="Seconds between transfer_report CSV lookup retries.")
    p.add_argument("--aws-cli", default="aws")
    p.add_argument("--aws-endpoint-url", default="https://10.10.10.103:9000",
                   help="S3 endpoint used for cleanup. Pass an empty string to omit --endpoint-url.")
    p.add_argument("--aws-no-verify-ssl", dest="aws_verify_ssl", action="store_false", default=False)
    p.add_argument("--aws-verify-ssl", dest="aws_verify_ssl", action="store_true")

    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--verbose", action="store_true")
    return p.parse_args(argv)


def ensure_mounted(args: argparse.Namespace, redact, tcr: ccr.TestCaseResult) -> str:
    """Real bryck_info states are ' Mounted', ' Ejected', ' Removed' (leading
    space, per bryckclient-cli). Only auto-mount from 'Ejected' (the only
    state bryck_mount.py accepts) -- 'Removed' or anything else means the
    device needs format/mount (or scan) first, which this script does not
    attempt; surface it as a clear error instead of a confusing bryck_mount.py
    failure."""
    state, cmd = ccr.get_bryck_state(
        argparse.Namespace(login=args.login, dry_run=args.dry_run, python_bin=args.python_bin), LOG, redact)
    tcr.commands.append(cmd.as_dict())
    if args.dry_run:
        return state
    normalized = state.strip().lower()
    if normalized == "mounted":
        return state
    if normalized != "ejected":
        LOG.error("Bryck state is %r -- expected 'Ejected' or 'Mounted'. Not attempting to mount "
                  "('Removed' needs bryck_format.py + bryck_mount.py first; run bryck_info.py "
                  "manually to confirm).", state)
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
    if cmd.returncode != 0:
        raise RuntimeError(f"bryckcloud transfer add aws failed (rc={cmd.returncode}): {cmd.stderr or cmd.stdout}")
    transfer_id = ccr.base.parse_transfer_id_from_output((cmd.stdout or "") + (cmd.stderr or ""))
    if transfer_id is None:
        transfer_id = ccr.base.detect_transfer_id(
            argparse.Namespace(transfer_id=None, poll_interval=args.poll_interval),
            before_ids, "", batchmeta_dir, transfer_logs_dir,
        )
    return str(transfer_id) if transfer_id is not None else None


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


def cleanup(args: argparse.Namespace, tier: str, redact, tcr: ccr.TestCaseResult) -> None:
    if args.dry_run or args.keep:
        LOG.info("cleanup skipped (dry-run or --keep)")
        tcr.notes.append("cleanup skipped (dry-run or --keep)")
        return
    for base_dir in (args.output_base, args.download_base):
        target = pathlib.Path(base_dir) / tier
        if target.is_dir():
            import shutil
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


def run_leg(args: argparse.Namespace, mode_label: str, case_dir: pathlib.Path, run_id: str,
           src: str, dst: str, redact, tier: str, gen_summary: dict) -> dict:
    """Run one full transfer leg (upload or download) with its own perf
    capture + transfer_report CSV lookup, so each direction gets complete,
    independent results instead of sharing/overwriting a single perf window."""
    case_dir.mkdir(parents=True, exist_ok=True)
    tcr = ccr.TestCaseResult(
        test_id=f"{run_id}_{mode_label}", kind=mode_label,
        description=f"transfer-only {mode_label} of {args.dataset}")

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
    try:
        transfer_id = initiate_transfer_cli(args, src, dst, redact, tcr)
        if not transfer_id:
            raise RuntimeError("transfer initiate did not return a transfer_id")

        final_state = poll_until_terminal(args, transfer_id, active_journal_raw, redact, tcr)
        LOG.info("%s transfer %s finished with state=%s", mode_label, transfer_id, final_state)
    except RuntimeError as exc:
        error = str(exc)
        LOG.error("%s failed: %s", mode_label, error)
        tcr.notes.append(f"ERROR: {error}")

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

    tcr.status = "PASS" if (args.dry_run or (error is None and final_state == TERMINAL_SUCCESS)) else \
        ("BLOCKED" if error else "FAIL")
    tcr.expected = "Transfer reaches COMPLETED"
    tcr.actual = f"final_state={final_state}" + (f", error={error}" if error else "")
    tcr.notes.append(f"transfer_id={transfer_id} final_state={final_state}")

    return {
        "tcr": tcr, "transfer_id": transfer_id, "final_state": final_state, "error": error,
        "perf_data": perf_data, "logs_collected": logs_collected,
    }


def main(argv: Optional[list] = None) -> int:
    args = parse_args(argv)
    setup_logging(args.verbose)

    tier = args.tier or args.dataset
    run_id = args.run_id or f"transfer_only_{dt.datetime.now():%Y%m%d_%H%M%S}"
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

    backup_path = run_dir / "cloud_ops.json.bak"
    if not backup_path.exists():
        backup_path.write_text(json.dumps(base_cloud_ops, indent=2), encoding="utf-8")

    mount_tcr = ccr.TestCaseResult(test_id=f"{run_id}_setup", kind="setup", description="mount + datagen + configure")

    state = "DRYRUN" if args.dry_run else ensure_mounted(args, redact, mount_tcr)
    if not args.skip_mount_check and not args.dry_run and state.strip().lower() != "mounted":
        LOG.error("Bryck did not reach Mounted state (last observed=%r; expected 'Mounted' or 'Ejected' "
                  "to auto-mount from -- 'Removed' or anything else needs manual "
                  "bryck_format.py/bryck_mount.py first); aborting.", state)
        return 4
    LOG.info("Bryck state: %s", state)

    ns = types.SimpleNamespace(
        output_base=args.output_base, skip_generate=args.skip_datagen,
        datagen_bin=args.datagen_bin, dry_run=args.dry_run, verbose=args.verbose,
    )
    dataset_root, gen_summary = ccr.generate_tier_dataset(tier, args.output_base, ns, LOG, args.dataset)
    LOG.info("Dataset materialized under %s (%s)", dataset_root, gen_summary)

    bucket = configure_cloud(args, base_cloud_ops, tier, redact, mount_tcr)
    LOG.info("Cloud configured: bucket=%s", bucket)
    mount_tcr.status = "PASS"

    local_src = str(dataset_root)
    s3_path = f"{args.bucket}/{tier}"
    local_dst = f"{args.download_base}/{tier}"

    # Each direction gets its own perf capture + transfer_report CSV lookup,
    # so --mode both captures full, independent results for upload AND
    # download instead of only the last leg's perf data.
    legs: List[dict] = []
    if args.mode in ("upload", "both"):
        legs.append(run_leg(args, "upload", run_dir / "upload", run_id, local_src, s3_path,
                            redact, tier, gen_summary))
    if args.mode in ("download", "both"):
        if args.mode == "download":
            # Standalone download needs source data in the bucket first --
            # seed it with an untracked upload, matching cloud_cli_runner.py's
            # _seed_upload_for_download(). Not one of the reported legs.
            seed_tcr = ccr.TestCaseResult(test_id=f"{run_id}_seed", kind="seed",
                                          description="untracked seed upload before standalone download")
            seed_id = initiate_transfer_cli(args, local_src, s3_path, redact, seed_tcr)
            if seed_id and not args.dry_run:
                seed_state = poll_until_terminal(args, seed_id, None, redact, seed_tcr)
                LOG.info("seed upload (transfer %s) finished with state=%s", seed_id, seed_state)
                if seed_state != TERMINAL_SUCCESS:
                    LOG.error("seed upload did not complete (state=%s); aborting download", seed_state)
                    return 5
        legs.append(run_leg(args, "download", run_dir / "download", run_id, s3_path, local_dst,
                            redact, tier, gen_summary))

    all_logs_collected = all(leg["logs_collected"] for leg in legs)
    if not all_logs_collected and not args.dry_run and not args.keep and not args.force_cleanup:
        LOG.warning("cleanup skipped: not all legs collected logs -- pass --force-cleanup to clean up anyway")
        for leg in legs:
            leg["tcr"].notes.append("cleanup skipped: logs not collected (use --force-cleanup to override)")
    else:
        cleanup(args, tier, redact, legs[-1]["tcr"])

    overall_ok = all(leg["error"] is None and leg["final_state"] == TERMINAL_SUCCESS for leg in legs) or args.dry_run
    result = {
        "run_id": run_id, "dataset": args.dataset, "tier": tier, "mode": args.mode,
        "legs": [
            {"mode": leg["tcr"].kind, "transfer_id": leg["transfer_id"], "final_state": leg["final_state"],
             "error": leg["error"], "perf": leg["perf_data"]}
            for leg in legs
        ],
        "dataset_summary": gen_summary,
        "started": run_id, "finished": dt.datetime.now().isoformat(timespec="seconds"),
    }
    (run_dir / "report.json").write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")

    all_tcrs = [mount_tcr] + [leg["tcr"] for leg in legs]
    plan = {"run_id": run_id, "test_cases": [
        {"id": t.test_id, "dataset": args.dataset, "mode": t.kind} for t in all_tcrs
    ]}
    commands_log = ccr.write_combined_commands_log(run_dir, all_tcrs)
    ccr.write_summary(run_dir, plan, all_tcrs)

    print("\n" + "=" * 60)
    for leg in legs:
        print(f"{leg['tcr'].kind} of {args.dataset} -> {leg['final_state']}"
              + (f" (transfer_id={leg['transfer_id']})" if leg["transfer_id"] else ""))
    print(f"Results directory : {run_dir}")
    print(f"  report.json      : {run_dir / 'report.json'}")
    print(f"  commands.log     : {commands_log}")
    print(f"  summary.json     : {run_dir / 'summary.json'}")
    print(f"  summary.md       : {run_dir / 'summary.md'}")
    print(f"  summary.html     : {run_dir / 'summary.html'}")
    print(f"  cloud_ops.json.bak: {backup_path}")
    for leg in legs:
        perf_data = leg["perf_data"]
        if perf_data is not None:
            print(f"  [{leg['tcr'].kind}] perf HTML report : {perf_data.get('html_report')}")
            print(f"  [{leg['tcr'].kind}] perf JSON data   : {perf_data.get('json_data')}")
            print(f"  [{leg['tcr'].kind}] perf zip         : {perf_data.get('zip')}")
    print("=" * 60 + "\n")

    return 0 if (args.dry_run or overall_ok) else 1


if __name__ == "__main__":
    sys.exit(main())
