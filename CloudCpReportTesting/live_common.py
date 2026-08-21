"""Shared live-flow orchestration for RT-* cases under live_cases/.

Each RT-* module (live_cases/rt_NN_*.py) defines CASE_ID, DESCRIPTION, STEPS,
and run(ctx, out_dir) -> (status, details). Plain cases call run_upload_case()
below; RT-08/09/10 (idempotency / download / round-trip) compose the lower
level helpers directly because their flow isn't the single-upload shape.

status is one of: PASS, FAIL, SETUP_ERROR, TRANSFER_FAILED, TIMEOUT,
REPORT_DOWNLOAD_ERROR, REPORT_PARSE_ERROR, PASS_WITH_CLEANUP_FAILURE
(see cloud_cp_report_test_case_plan.md §13).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import bryck_client as bc
import report_engine as re_

logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).resolve().parent


@dataclass
class Context:
    cfg: dict
    args: Any
    session: Any = None
    api: Any = None
    _ssh: Any = field(default=None, repr=False)

    def ssh(self):
        if self._ssh is None:
            self._ssh = bc.connect_ssh(self.session)
        return self._ssh

    def close(self):
        if self._ssh is not None:
            self._ssh.close()
            self._ssh = None


def build_context(args, cfg=None):
    cfg = cfg or bc.load_config(getattr(args, "config", None))
    session = bc.get_session(cfg)
    api = bc.get_api(session)
    return Context(cfg=cfg, args=args, session=session, api=api)


def spec_path(spec_ref):
    """Resolve a spec_ref to an absolute path. spec_ref is a path to a real,
    already-existing datagen YAML elsewhere in the repo (e.g.
    '../CloudCpFallbackTesting/spec_files/03_small_files.yaml'), relative to
    this file's directory - RT-* cases deliberately do NOT ship their own
    duplicate copies of these specs."""
    p = Path(spec_ref)
    return p if p.is_absolute() else (SCRIPT_DIR / p).resolve()


def setup_case(ctx, remote_dirs, s3_uris):
    """Step 1: idempotent SSH + S3 cleanup before the case runs."""
    ssh = ctx.ssh()
    for d in remote_dirs:
        bc.ssh_rm_rf(ssh, d)
    if ctx.cfg.get("cleanup_s3", True):
        for uri in s3_uris:
            bc.s3_rm_recursive(ctx.cfg, uri)


def generate_data(ctx, case_id, spec_ref, remote_dir):
    """Step 2: push the real datagen spec (see spec_path), run it, enumerate
    the resulting files. push_spec_file rewrites the spec's root: to
    remote_dir at upload time, so reusing another phase's/case's spec file
    verbatim is always safe - it never writes into that other suite's dir.
    """
    if getattr(ctx.args, "no_datagen", False):
        entries = bc.enumerate_remote_files(ctx.ssh(), remote_dir)
        return entries
    local_spec = spec_path(spec_ref)
    if not local_spec.is_file():
        raise bc.LiveClientError(f"spec file not found: {local_spec}")
    remote_spec = bc.push_spec_file(ctx.ssh(), local_spec, case_id, remote_dir=remote_dir)
    bc.run_datagen(ctx.ssh(), ctx.cfg, remote_spec)
    entries = bc.enumerate_remote_files(ctx.ssh(), remote_dir)
    if not entries:
        raise bc.LiveClientError(f"datagen produced zero files under {remote_dir}")
    return entries


def initiate_and_wait(ctx, src, dst):
    """Steps 3-4: start the transfer (or reuse --transfer-id) and poll to terminal."""
    if getattr(ctx.args, "transfer_id", None):
        return str(ctx.args.transfer_id), "COMPLETED", []
    if getattr(ctx.args, "no_transfer", False):
        raise bc.LiveClientError("--no-transfer given without --transfer-id")
    transfer_id = bc.initiate_transfer(ctx.api, ctx.cfg, src, dst)
    state, _last, history = bc.poll_until_terminal(
        ctx.api, transfer_id,
        ctx.cfg.get("transfer_poll_interval_sec", 15),
        ctx.cfg.get("transfer_poll_timeout_sec", 3600),
    )
    return transfer_id, state, history


def download_and_parse(ctx, transfer_id, out_dir):
    """Steps 5-6: download the report ZIP, extract it, parse the known files."""
    zips_dir = Path(ctx.cfg.get("report_download_dir", "reports/zips"))
    if not zips_dir.is_absolute():
        zips_dir = Path(ctx.cfg["_base_dir"]) / zips_dir
    zip_path = bc.download_report(ctx.api, transfer_id, zips_dir)
    extracted = re_.unzip_report(zip_path, Path(out_dir) / f"cloud_transfer_{transfer_id}")
    files = re_.find_report_files(extracted)

    if "report_csv" not in files:
        raise bc.LiveClientError(f"transfer_report_*.csv missing from report ZIP for {transfer_id}")
    report_rows = re_.parse_transfer_report_csv(files["report_csv"])

    summary = re_.parse_transfer_summary_txt(files["summary_txt"]) if "summary_txt" in files else {}

    final_report_rows = None
    if "final_report_json" in files:
        final_report_rows = re_.parse_final_report_json(files["final_report_json"])

    return {
        "zip_path": zip_path,
        "extracted_dir": extracted,
        "files": files,
        "report_rows": report_rows,
        "summary": summary,
        "final_report_rows": final_report_rows,
    }


def cleanup_case(ctx, remote_dirs, s3_uris):
    """Step 8 + cleanup assertions (plan §12). Never raises; returns dict of booleans."""
    result = {"local_ok": True, "s3_ok": True}
    if getattr(ctx.args, "no_cleanup", False):
        return {"local_ok": None, "s3_ok": None, "skipped": True}
    ssh = ctx.ssh()
    for d in remote_dirs:
        if ctx.cfg.get("cleanup_local", True):
            bc.ssh_rm_rf(ssh, d)
        if bc.ssh_dir_exists(ssh, d):
            result["local_ok"] = False
    if ctx.cfg.get("cleanup_s3", True):
        for uri in s3_uris:
            bc.s3_rm_recursive(ctx.cfg, uri)
            count = bc.s3_object_count(ctx.cfg, uri)
            if count not in (0, None):
                result["s3_ok"] = False
    return result


def downgrade_status_for_cleanup(status, cleanup_result):
    """PASS -> PASS_WITH_CLEANUP_FAILURE when cleanup left data behind
    (plan §12: leftover data is surfaced, not treated as a hard failure)."""
    cleanup_failed = cleanup_result.get("local_ok") is False or cleanup_result.get("s3_ok") is False
    if status == "PASS" and cleanup_failed:
        return "PASS_WITH_CLEANUP_FAILURE"
    return status


def cross_cutting(report_rows, summary):
    return re_.cross_cutting_checks(report_rows, summary)


def run_upload_case(ctx, case_id, spec_ref, expected_count, out_dir, extra_assert=None,
                     remote_suffix=None, s3_suffix=None):
    """Generic single-upload flow (RT-01..RT-07): setup -> datagen -> upload ->
    poll -> download+parse -> assert -> cleanup. Returns (status, details).

    spec_ref points at a real, already-existing datagen spec elsewhere in the
    repo (see spec_path) - no per-case duplicate spec files are shipped here.
    extra_assert, if given, is called as
    extra_assert(source_entries, report_rows, summary, parsed) -> (bool, dict)
    for case-specific checks layered on top of the RT-01 baseline assertions.
    """
    remote_dir = bc.remote_case_dir(ctx.cfg, case_id, remote_suffix)
    s3_uri = bc.s3_case_uri(ctx.cfg, case_id, s3_suffix)
    details = {"remote_dir": remote_dir, "s3_uri": s3_uri}

    try:
        setup_case(ctx, [remote_dir], [s3_uri])
        entries = generate_data(ctx, case_id, spec_ref, remote_dir)
    except bc.LiveClientError as exc:
        details["error"] = str(exc)
        return "SETUP_ERROR", details

    details["source_file_count"] = len(entries)
    details["source_total_bytes"] = sum(sz for _, sz in entries)

    try:
        transfer_id, state, _history = initiate_and_wait(ctx, remote_dir, s3_uri)
    except bc.LiveClientError as exc:
        details["error"] = str(exc)
        return "SETUP_ERROR", details

    details["transfer_id"] = transfer_id
    details["final_state"] = state

    if state == "TIMEOUT":
        details["cleanup"] = cleanup_case(ctx, [remote_dir], [s3_uri])
        return "TIMEOUT", details

    if state != "COMPLETED":
        try:
            parsed = download_and_parse(ctx, transfer_id, out_dir)
            details["partial_report_dir"] = str(parsed["extracted_dir"])
        except (bc.LiveClientError, OSError):
            pass
        details["cleanup"] = cleanup_case(ctx, [remote_dir], [s3_uri])
        return "TRANSFER_FAILED", details

    try:
        parsed = download_and_parse(ctx, transfer_id, out_dir)
    except bc.LiveClientError as exc:
        details["error"] = str(exc)
        return "REPORT_DOWNLOAD_ERROR", details
    except (KeyError, ValueError, OSError) as exc:
        details["error"] = str(exc)
        return "REPORT_PARSE_ERROR", details

    report_rows = parsed["report_rows"]
    summary = parsed["summary"]
    checks = cross_cutting(report_rows, summary)
    details["cross_cutting_checks"] = checks
    details["report_row_count"] = len(report_rows)
    details["summary"] = summary

    all_success = bool(report_rows) and all(
        r.get("status", "").upper() == "SUCCESS" for r in report_rows
    )
    count_ok = expected_count is None or len(report_rows) == expected_count
    details["all_rows_success"] = all_success
    details["row_count_matches_expected"] = count_ok

    passed = all_success and count_ok and all(checks.values())

    if extra_assert is not None:
        extra_ok, extra_details = extra_assert(entries, report_rows, summary, parsed)
        details["extra_assertions"] = extra_details
        passed = passed and extra_ok

    status = "PASS" if passed else "FAIL"
    cleanup_result = cleanup_case(ctx, [remote_dir], [s3_uri])
    details["cleanup"] = cleanup_result
    status = downgrade_status_for_cleanup(status, cleanup_result)
    return status, details
