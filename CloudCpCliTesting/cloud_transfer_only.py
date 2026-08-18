#!/usr/bin/env python3
"""Transfer-only runner: datagen -> configure cloud -> REST-API initiate ->
poll to terminal state -> perf capture -> (optional) cleanup.

No test matrix, no lifecycle/negative/edge cases -- just one real transfer,
run directly on the Bryck host (same execution model as cloud_cli_runner.py).

Reuses proven pieces instead of re-inventing them:
  - dataset generation, status-fallback helpers (journal/log-based), and
    command/redaction plumbing from cloud_cli_runner.py;
  - the REST-API transfer initiate/status pattern (BryckApi.initiate_cloud_transfer
    + get_cloud_transfer_status, via a persistent ApiSession) proven reliable in
    CloudCpFallbackTesting/cloudcp_fallback_test.py -- imported from there, not
    duplicated, and that file is never modified;
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
from typing import Any, Optional

HERE = pathlib.Path(__file__).resolve().parent
REPO_ROOT = HERE.parent
BRYCK_CLI_DIR = HERE / "bryckclient-cli"
RESULTS_ROOT = HERE / "results"
FALLBACK_DIR = REPO_ROOT / "CloudCpFallbackTesting"

sys.path.insert(0, str(HERE))
sys.path.insert(0, str(BRYCK_CLI_DIR))
sys.path.insert(0, str(FALLBACK_DIR))

import cloud_cli_runner as ccr  # noqa: E402  (datagen, status fallbacks, run helpers)
import cli_perf_capture as perf_mod  # noqa: E402
import cloudcp_fallback_test as fb  # noqa: E402  (proven REST initiate/status pattern; read-only reference)

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
    p.add_argument("--aws-cli", default="aws")
    p.add_argument("--aws-endpoint-url", default="https://10.10.10.103:9000",
                   help="S3 endpoint used for cleanup. Pass an empty string to omit --endpoint-url.")
    p.add_argument("--aws-no-verify-ssl", dest="aws_verify_ssl", action="store_false", default=False)
    p.add_argument("--aws-verify-ssl", dest="aws_verify_ssl", action="store_true")

    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--verbose", action="store_true")
    return p.parse_args(argv)


def ensure_mounted(args: argparse.Namespace, redact) -> str:
    state, _cmd = ccr.get_bryck_state(
        argparse.Namespace(login=args.login, dry_run=args.dry_run, python_bin=args.python_bin), LOG, redact)
    if args.dry_run or "mount" in state.lower():
        return state
    LOG.info("Bryck not mounted (state=%r); mounting before dataset generation/transfer", state)
    mount_cmd = ccr.run_py_script(
        "bryck_mount.py", ["--login", args.login, "--params", args.format_mount_params],
        LOG, args.dry_run, redact, args.python_bin)
    if mount_cmd.returncode != 0:
        LOG.warning("bryck_mount.py failed (rc=%s): %s", mount_cmd.returncode, mount_cmd.stderr or mount_cmd.stdout)
        return state
    deadline = time.time() + args.action_timeout
    while time.time() < deadline:
        state, _cmd = ccr.get_bryck_state(
            argparse.Namespace(login=args.login, dry_run=args.dry_run, python_bin=args.python_bin), LOG, redact)
        if "mount" in state.lower():
            break
        time.sleep(args.poll_interval)
    return state


def configure_cloud(args: argparse.Namespace, base_cloud_ops: dict, tier: str, redact) -> str:
    """Deconfigure any stale cloud config, rewrite cloud_ops.json for this
    dataset/tier, then configure + verify. Mirrors Executor.configure_cloud()."""
    cloud_type = str(base_cloud_ops.get("cloud_type", "aws"))
    deconfigure_cmd = ccr.run_py_script(
        "bryck_cloud_deconfigure.py", ["--login", args.login, "--cloud-type", cloud_type],
        LOG, args.dry_run, redact, args.python_bin)
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
    if not args.dry_run and configure_cmd.returncode != 0:
        raise RuntimeError(f"bryck_cloud_configure.py failed (rc={configure_cmd.returncode}): "
                            f"{configure_cmd.stderr or configure_cmd.stdout}")
    show_cmd = ccr.run_py_script(
        "bryck_cloud_show.py", ["--login", args.login], LOG, args.dry_run, redact, args.python_bin)
    if not args.dry_run and show_cmd.returncode != 0:
        raise RuntimeError(f"bryck_cloud_show.py failed (rc={show_cmd.returncode}): "
                            f"{show_cmd.stderr or show_cmd.stdout}")
    return cfg["cloud_bucket"]


def poll_until_terminal(api, args: argparse.Namespace, transfer_id: str,
                        active_journal_raw: Optional[pathlib.Path]) -> str:
    """Poll to a terminal state via the REST API (the pattern proven reliable
    in cloudcp_fallback_test.py's poll_transfer/_status_entry), falling back
    -- in order -- to the live journal_raw.log tail, transfer_summary.txt on
    disk, and a bounded journalctl re-query, exactly like cloud_cli_runner.py's
    transfer_status(), since the REST status endpoint can return 409 "Failed
    to find the transfer/s" for a transfer that is still running or just
    finished."""
    if args.dry_run:
        return TERMINAL_SUCCESS
    deadline = time.time() + args.wait_timeout
    state = "UNKNOWN"
    while time.time() < deadline:
        entry = fb._status_entry(api, transfer_id)
        state = str((entry or {}).get("state") or "").upper()
        if not state:
            if active_journal_raw is not None:
                live = ccr.read_live_journal_transfer_status(active_journal_raw, transfer_id)
                if live != "UNKNOWN":
                    state = live
            if not state or state == "UNKNOWN":
                local = ccr.read_local_transfer_status(pathlib.Path(args.transfer_logs_dir), transfer_id)
                if local != "UNKNOWN":
                    state = local
            if not state or state == "UNKNOWN":
                journal = ccr.read_journalctl_transfer_status(transfer_id, args.journal_tag)
                if journal != "UNKNOWN":
                    state = journal
        LOG.info("transfer %s state=%s", transfer_id, state or "UNKNOWN")
        if state in TERMINAL_STATES:
            return state
        time.sleep(args.poll_interval)
    return state or "UNKNOWN"


def cleanup(args: argparse.Namespace, tier: str, redact) -> None:
    if args.dry_run or args.keep:
        LOG.info("cleanup skipped (dry-run or --keep)")
        return
    for base_dir in (args.output_base, args.download_base):
        target = pathlib.Path(base_dir) / tier
        if target.is_dir():
            import shutil
            try:
                shutil.rmtree(target)
            except OSError as exc:
                LOG.warning("cleanup: could not remove %s: %s", target, exc)
    bucket_prefix = f"{args.bucket}/{tier}"
    argv = [args.aws_cli, "s3", "rm", bucket_prefix, "--recursive"]
    if args.aws_endpoint_url:
        argv += ["--endpoint-url", args.aws_endpoint_url]
    if not args.aws_verify_ssl:
        argv += ["--no-verify-ssl"]
    ccr.run_argv("aws s3 cleanup", argv, LOG, args.dry_run, redact)


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

    state = "DRYRUN" if args.dry_run else ensure_mounted(args, redact)
    if not args.skip_mount_check and not args.dry_run and "mount" not in state.lower():
        LOG.error("Bryck did not reach Mounted state (last observed=%r); aborting.", state)
        return 4
    LOG.info("Bryck state: %s", state)

    ns = types.SimpleNamespace(
        output_base=args.output_base, skip_generate=args.skip_datagen,
        datagen_bin=args.datagen_bin, dry_run=args.dry_run, verbose=args.verbose,
    )
    dataset_root, gen_summary = ccr.generate_tier_dataset(tier, args.output_base, ns, LOG, args.dataset)
    LOG.info("Dataset materialized under %s (%s)", dataset_root, gen_summary)

    bucket = configure_cloud(args, base_cloud_ops, tier, redact)
    LOG.info("Cloud configured: bucket=%s", bucket)

    perf_cfg = {
        "journal_tag": args.journal_tag, "cloudcp_log": args.cloudcp_log,
        "capture_lead": args.capture_lead, "capture_drain": args.capture_drain,
        "transfer_logs_dir": args.transfer_logs_dir, "bryck_config_json": args.bryck_config_json,
    }
    collector = perf_mod.TransferPerfCollector(run_dir, perf_cfg, args.dry_run) if args.perf_capture else None
    active_journal_raw = None
    if collector is not None:
        collector.start()
        active_journal_raw = collector.perf_dir / "journal_raw.log"

    creds = {"cloud_type": "aws", "access_key_id": None, "secret_access_key": None, "region": "us-east-1"}
    api = None
    session = None
    transfer_id: Optional[str] = None
    final_state = "UNKNOWN"
    error: Optional[str] = None

    try:
        if not args.dry_run:
            creds = fb._load_creds(pathlib.Path(args.params))
            from session import ApiSession  # type: ignore
            from bryck_api import BryckApi  # type: ignore
            session = ApiSession.from_login_json(args.login)
            session.login()
            api = BryckApi(session)

        rec = fb.Recorder(run_id)
        if not args.dry_run:
            fb.configure_cloud_provider(api, rec, creds)

        local_src = str(dataset_root)
        s3_path = f"{args.bucket}/{tier}"
        local_dst = f"{args.download_base}/{tier}"

        def do_initiate(src: str, dst: str) -> Optional[str]:
            if args.dry_run:
                LOG.info("(dry-run) would POST /api/bcloud/transfer src=%s dst=%s", src, dst)
                return "DRYRUN-ID"
            return fb.initiate_transfer(api, rec, creds["cloud_type"], src, dst)

        if args.mode == "upload":
            transfer_id = do_initiate(local_src, s3_path)
        elif args.mode == "download":
            # Standalone download needs source data in the bucket first --
            # seed it with an untracked upload, matching cloud_cli_runner.py's
            # _seed_upload_for_download().
            seed_id = do_initiate(local_src, s3_path)
            if seed_id and not args.dry_run:
                seed_state = poll_until_terminal(api, args, seed_id, active_journal_raw)
                LOG.info("seed upload (transfer %s) finished with state=%s", seed_id, seed_state)
                if seed_state != TERMINAL_SUCCESS:
                    raise RuntimeError(f"seed upload did not complete (state={seed_state}); "
                                       f"cannot proceed with download")
            transfer_id = do_initiate(s3_path, local_dst)
        else:  # both
            upload_id = do_initiate(local_src, s3_path)
            if upload_id and not args.dry_run:
                upload_state = poll_until_terminal(api, args, upload_id, active_journal_raw)
                LOG.info("upload (transfer %s) finished with state=%s", upload_id, upload_state)
            transfer_id = do_initiate(s3_path, local_dst)

        if not transfer_id:
            raise RuntimeError("transfer initiate did not return a transfer_id")

        final_state = poll_until_terminal(api, args, transfer_id, active_journal_raw)
        LOG.info("transfer %s finished with state=%s", transfer_id, final_state)
    except (fb.TransferInitError, RuntimeError) as exc:
        error = str(exc)
        LOG.error("transfer failed: %s", error)
    finally:
        if session is not None:
            try:
                session.close()
            except Exception:  # noqa: BLE001
                pass

    perf_data = None
    if collector is not None:
        perf_data = collector.finish(
            transfer_id or "unknown", test_id=run_id, tier=tier, mode=args.mode,
            description=f"transfer-only {args.mode} of {args.dataset}", gen_summary=gen_summary,
        )
        LOG.info("Perf report: %s", perf_data.get("html_report"))

    cleanup(args, tier, redact)

    result = {
        "run_id": run_id, "dataset": args.dataset, "tier": tier, "mode": args.mode,
        "transfer_id": transfer_id, "final_state": final_state, "error": error,
        "dataset_summary": gen_summary, "perf": perf_data,
        "started": run_id, "finished": dt.datetime.now().isoformat(timespec="seconds"),
    }
    (run_dir / "report.json").write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
    LOG.info("Report: %s", run_dir / "report.json")

    return 0 if (args.dry_run or (error is None and final_state == TERMINAL_SUCCESS)) else 1


if __name__ == "__main__":
    sys.exit(main())
