#!/usr/bin/env python3
"""Component-level fallback tests — driven from ``cloudcp_fallback_test.py``.

This module exercises the two internal fallback mechanisms **in isolation**,
the way they are invoked inside the running bcloud service, instead of driving
the whole pipeline through the REST API:

  * ``fallback_worker``            — the socket-free background daemon that drains
                                     cloudcp rc==2 retry ``.lst`` files via boto3.
  * ``mp_batch_retry.retry_whole_batch`` — the boto3 ProcessPool whole-batch
                                     retry for cloudcp rc==1.

See ``plan_cp_component_fallback.md`` for the full plan. It is imported by the
existing harness (which owns the SSH session + step recorder + reporting
primitives); nothing here is executed unless one of the ``--component*`` flags
is passed to ``cloudcp_fallback_test.py``.

The on-disk staging + the whole-batch driver run on the Bryck through
``component_stage.py`` (uploaded to ``/tmp/cc_component_stage.py`` and executed
with the bcloud venv interpreter so it can import the ``bryckcloud`` modules
under test).
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path

import cloudcp_fallback_test as base
from cloudcp_fallback_test import (DATASETS, Recorder, RemoteHost, VERDICT_COLORS,
                                   _now, _write_json, _clip)

LOG = logging.getLogger("component_test")

# The batch is placed under a plausible size tier; the worker locates a batch by
# name across every tier, so this only needs to be sensible.
TIER_OF = {
    "zero": "zero", "tiny": "tiny", "small": "small", "medium": "medium",
    "large": "large", "sparse": "medium", "fill": "small", "deep": "tiny",
    "unicode": "tiny", "special": "tiny", "mixed": "small", "scale": "tiny",
}

BATCH_NAME = "batch_000000.txt"
REMOTE_STAGE = "/tmp/cc_component_stage.py"
REMOTE_SPEC_DIR = "/tmp/cc_component_specs"
DEF_VENV_PYTHON = "/opt/bryck/.venv/bryck/bin/python3"
DEF_BATCHMETA = "/opt/bryck/bryckapi/downloads/bcloud_batchmeta"
DEF_COMPONENT_BUCKET = "omicron"
DEF_REGION = "us-west-1"

# Ordered dataset keys (matches spec_files/ numeric order).
_DATASET_ORDER = ["zero", "tiny", "small", "medium", "large", "sparse", "fill",
                  "deep", "unicode", "special", "mixed", "scale"]


# =============================================================================
# Case model + catalog
# =============================================================================

@dataclass
class CompCase:
    cid: str
    mechanism: str          # "worker" | "mp"
    group: str              # WORKER | MP | NEGATIVE
    dataset: str            # key into DATASETS
    transfer_type: str      # "upload" | "download"
    fault: str              # "" | "missing" | "download"
    expect: str             # "ok" | "fail"
    desc: str

    @property
    def ds(self):
        return DATASETS[self.dataset]

    @property
    def heavy(self) -> bool:
        return DATASETS[self.dataset].heavy


def build_component_catalog() -> list[CompCase]:
    cases: list[CompCase] = []

    # ---- Fallback worker matrix (CFW-U-*) -----------------------------------
    for i, key in enumerate(_DATASET_ORDER, 1):
        cases.append(CompCase(
            cid=f"CFW-U-{i:02d}", mechanism="worker", group="WORKER",
            dataset=key, transfer_type="upload", fault="", expect="ok",
            desc=f"Fallback worker drains a staged retry .lst for the "
                 f"'{key}' dataset — all records uploaded via boto3 "
                 f"(FALLBACK_OK) and the batch is completed."))

    # ---- Whole-batch retry matrix (CMP-U-*) ---------------------------------
    for i, key in enumerate(_DATASET_ORDER, 1):
        cases.append(CompCase(
            cid=f"CMP-U-{i:02d}", mechanism="mp", group="MP",
            dataset=key, transfer_type="upload", fault="", expect="ok",
            desc=f"retry_whole_batch() uploads the entire staged batch for the "
                 f"'{key}' dataset via the boto3 ProcessPool — ok==count, "
                 f"failed==0, all rows MP_OK."))

    # ---- Negative cases -----------------------------------------------------
    cases.append(CompCase(
        cid="CFW-N-01", mechanism="worker", group="NEGATIVE", dataset="tiny",
        transfer_type="upload", fault="missing", expect="fail",
        desc="Fallback worker with the source files deleted after staging — "
             "records fail terminally, the batch stays inprogress and the .lst "
             "is not retired; no hang / crash."))
    cases.append(CompCase(
        cid="CMP-N-01", mechanism="mp", group="NEGATIVE", dataset="tiny",
        transfer_type="download", fault="download", expect="fail",
        desc="retry_whole_batch() with transfer_type=download — downloads are "
             "not handled inline, so it returns (0, N, 0) with no crash."))
    cases.append(CompCase(
        cid="CMP-N-02", mechanism="mp", group="NEGATIVE", dataset="tiny",
        transfer_type="upload", fault="missing", expect="fail",
        desc="retry_whole_batch() with the source files deleted after staging — "
             "every record fails the stat/upload, failed==N and ok==0."))

    # ---- Break-condition / vulnerability coverage (B1-B9) -------------------
    # Worker break conditions
    cases.append(CompCase(
        cid="CFW-N-02", mechanism="worker", group="NEGATIVE", dataset="tiny",
        transfer_type="upload", fault="bad_lst", expect="fail",
        desc="B1 — Malformed .lst framing (2 fields/record, no size/error fields): "
             "read_retry_list misgroups fields; url_parse receives garbage paths and "
             "returns (None,None); all records fail terminally; no hang/crash."))
    cases.append(CompCase(
        cid="CFW-N-03", mechanism="worker", group="NEGATIVE", dataset="tiny",
        transfer_type="upload", fault="bad_bucket", expect="fail",
        desc="B2 — Valid .lst but s3path references a nonexistent bucket: all "
             "uploads fail with NoSuchBucket after max_attempts retries; batch "
             "stays inprogress; .lst not retired; no hang/crash."))
    cases.append(CompCase(
        cid="CFW-N-04", mechanism="worker", group="NEGATIVE", dataset="tiny",
        transfer_type="upload", fault="late_lst", expect="ok",
        desc="B3 — Done marker written BEFORE the .lst (reversed staging order): "
             "worker must still drain the list on its first poll pass and complete "
             "the batch cleanly (resilience to early-marker race)."))
    cases.append(CompCase(
        cid="CFW-N-05", mechanism="worker", group="NEGATIVE", dataset="tiny",
        transfer_type="upload", fault="no_batch", expect="warn",
        desc="B4 — .lst staged but the batch file is deleted before the worker "
             "runs: files are uploaded (FALLBACK_OK) and .lst is retired, but "
             "batch_state.complete() finds nothing to rename — state tree is "
             "inconsistent (no file in completed/)."))
    cases.append(CompCase(
        cid="CFW-N-06", mechanism="worker", group="NEGATIVE", dataset="tiny",
        transfer_type="upload", fault="empty_lst", expect="ok",
        desc="B5 — .lst truncated to 0 bytes after creation: worker sees zero "
             "records and immediately completes the batch + retires the .lst "
             "without uploading anything."))
    # MP break conditions
    cases.append(CompCase(
        cid="CMP-N-03", mechanism="mp", group="NEGATIVE", dataset="tiny",
        transfer_type="upload", fault="empty_batch", expect="ok",
        desc="B6 — Empty (0-record) batch file: retry_whole_batch returns (0,0,0) "
             "silently; the caller cannot distinguish this from a normal 0-file "
             "dataset — a silent no-op."))
    cases.append(CompCase(
        cid="CMP-N-04", mechanism="mp", group="NEGATIVE", dataset="tiny",
        transfer_type="upload", fault="bad_bucket", expect="fail",
        desc="B7 — Nonexistent bucket passed to retry_whole_batch: every upload "
             "fails with NoSuchBucket after max_attempts retries; ok==0, "
             "failed==count; no hang/crash."))
    cases.append(CompCase(
        cid="CMP-N-05", mechanism="mp", group="NEGATIVE", dataset="tiny",
        transfer_type="upload", fault="bad_fsp", expect="ok",
        desc="B8 — Wrong fs_prefix: compose_s3_key falls back to basename-only "
             "keys; uploads succeed but files with the same basename in different "
             "sub-directories collide to the same S3 key (last-writer-wins); "
             "ok==count but s3_objects may be < count."))
    cases.append(CompCase(
        cid="CMP-N-06", mechanism="mp", group="NEGATIVE", dataset="tiny",
        transfer_type="upload", fault="resume", expect="ok",
        desc="B9 — retry_whole_batch called twice for the same transfer_id: second "
             "call returns (0,0,0) because load_completed deduplicates all files "
             "already in the transfer report (idempotency / resume safety)."))
    return cases


# =============================================================================
# Selection
# =============================================================================

def _resolve_ids(tokens: list[str], catalog: list[CompCase]) -> list[str]:
    order = [c.cid for c in catalog]
    lower = {cid.lower(): cid for cid in order}
    resolved, unknown = [], []
    for tok in tokens:
        if tok.lower() in lower:
            resolved.append(lower[tok.lower()])
        elif tok.isdigit() and 1 <= int(tok) <= len(order):
            resolved.append(order[int(tok) - 1])
        else:
            unknown.append(tok)
    if unknown:
        LOG.warning("Unknown component case id/index: %s", ", ".join(unknown))
    return resolved


def select_component_cases(args, catalog: list[CompCase]) -> list[CompCase]:
    by_id = {c.cid: c for c in catalog}
    if getattr(args, "component_one", None):
        tokens = [x.strip() for x in args.component_one.split(",") if x.strip()]
        return [by_id[i] for i in _resolve_ids(tokens, catalog)]
    if getattr(args, "component_negative", False):
        return [c for c in catalog if c.group == "NEGATIVE"]
    if getattr(args, "component", False):
        heavy = getattr(args, "heavy", False)
        return [c for c in catalog if c.group == "NEGATIVE" or heavy or not c.heavy]
    return []


def print_component_list(catalog: list[CompCase] | None = None) -> None:
    catalog = catalog or build_component_catalog()
    print(f"{'#':<4} {'CASE':<10} {'MECH':<7} {'GROUP':<9} {'DATASET':<8} "
          f"{'TYPE':<9} {'FAULT':<8} EXPECT")
    for i, c in enumerate(catalog, 1):
        print(f"{i:<4} {c.cid:<10} {c.mechanism:<7} {c.group:<9} {c.dataset:<8} "
              f"{c.transfer_type:<9} {(c.fault or '-'):<8} {c.expect}")
    print(f"\n{len(catalog)} component cases. Heavy datasets (large, scale) run "
          f"only with --heavy. Select by # or case id via --component-one.")


# =============================================================================
# Runner
# =============================================================================

def _parse_json_line(out: str) -> dict:
    """Return the last JSON object printed by the stage helper (or {})."""
    for line in reversed((out or "").strip().splitlines()):
        line = line.strip()
        if line.startswith("{") and line.endswith("}"):
            try:
                return json.loads(line)
            except ValueError:
                continue
    return {}


class ComponentRunner:
    def __init__(self, args, host: RemoteHost):
        self.args = args
        self.host = host
        self.out_dir = Path(args.out_dir)
        self.spec_dir = Path(args.spec_dir)
        self.py = getattr(args, "venv_python", DEF_VENV_PYTHON)
        self.batchmeta = getattr(args, "batchmeta_dir", DEF_BATCHMETA)
        self.bucket = getattr(args, "component_bucket", DEF_COMPONENT_BUCKET)
        self.region = getattr(args, "region", DEF_REGION)
        self.endpoint = args.endpoint_url
        self.src_base = args.src_base
        self.logs_dir = args.transfer_logs_dir
        self.datagen = args.datagen
        self.pool_size = getattr(args, "pool_size", 16)
        self._stage_uploaded = False

    # -- helpers ------------------------------------------------------------
    def _stage_uploaded_once(self, rec: Recorder) -> None:
        if self._stage_uploaded:
            return
        local = Path(__file__).resolve().parent / "component_stage.py"
        self.host.put(rec, "upload stage helper", str(local), REMOTE_STAGE)
        self._stage_uploaded = True

    def _stage(self, rec: Recorder, name: str, sub: str, timeout: int = 300) -> dict:
        cmd = f"{self.py} {REMOTE_STAGE} {sub}"
        rc, out, err = self.host.run(rec, name, cmd, timeout=timeout, check=False)
        if self.host.dry_run:
            return {}
        data = _parse_json_line(out)
        if "error" in data:
            raise RuntimeError(f"{name} failed: {data['error']}")
        if rc != 0 and not data:
            raise RuntimeError(f"{name} failed (rc={rc}): {err.strip() or out.strip()}")
        return data

    def ensure_dataset(self, rec: Recorder, ds) -> int | None:
        src = f"{self.src_base}/{ds.tier_dir}"
        if getattr(self.args, "skip_datagen", False):
            rec.add("skip datagen", "local", f"reuse {src}", detail="--skip-datagen")
            return self._count_files(rec, src)
        local_spec = self.spec_dir / ds.spec
        remote_spec = f"{REMOTE_SPEC_DIR}/{ds.spec}"
        self.host.run(rec, "prep remote spec dir",
                      f"mkdir -p {REMOTE_SPEC_DIR}", check=False)
        self.host.put(rec, "upload spec", str(local_spec), remote_spec)
        self.host.run(rec, "datagen", f"{self.datagen} --spec {remote_spec}",
                      timeout=self.args.poll_timeout, check=True)
        return self._count_files(rec, src)

    def _count_files(self, rec: Recorder, path: str) -> int | None:
        rc, out, _ = self.host.run(rec, "count source files",
                                   f"find {path} -type f 2>/dev/null | wc -l",
                                   check=False)
        if self.host.dry_run:
            return None
        try:
            return int(out.strip().splitlines()[-1]) if out.strip() else 0
        except (ValueError, IndexError):
            return None

    def _s3_object_count(self, rec: Recorder, prefix: str) -> int | None:
        rc, out, _ = self.host.run(
            rec, "count s3 objects",
            f"aws s3 ls --recursive s3://{self.bucket}/{prefix} "
            f"--endpoint-url {self.endpoint} 2>/dev/null | wc -l", check=False)
        if self.host.dry_run:
            return None
        try:
            return int(out.strip().splitlines()[-1]) if out.strip() else 0
        except (ValueError, IndexError):
            return None

    def _preflight_bucket(self, rec: Recorder) -> None:
        # Non-fatal: proves boto3/aws creds + endpoint + bucket are reachable.
        self.host.run(rec, "preflight bucket",
                      f"aws s3 ls s3://{self.bucket} --endpoint-url {self.endpoint} "
                      f">/dev/null 2>&1 && echo OK || echo UNREACHABLE", check=False)

    # -- case dispatch ------------------------------------------------------
    def run_case(self, case: CompCase) -> dict:
        rec = Recorder(case.cid)
        run_dir = self.out_dir / case.cid
        run_dir.mkdir(parents=True, exist_ok=True)
        started = _now()
        rec.add("case", "local", "",
                detail=(f"{case.cid} [{case.group}] mechanism={case.mechanism} "
                        f"dataset={case.dataset} type={case.transfer_type} "
                        f"fault={case.fault or '-'} expect={case.expect}"))
        rec.add("description", "local", "", detail=case.desc)

        verdict, reasons, capture = "ERROR", [], {}
        transfer_id = None
        try:
            self._stage_uploaded_once(rec)
            self._preflight_bucket(rec)
            self.ensure_dataset(rec, case.ds)
            if case.mechanism == "worker":
                verdict, reasons, capture, transfer_id = self._run_worker(rec, case)
            else:
                verdict, reasons, capture, transfer_id = self._run_mp(rec, case)
        except KeyboardInterrupt:
            verdict, reasons = "INTERRUPTED", ["interrupted by user (Ctrl+C)"]
            rec.add("interrupted", "local", "", ok=False, detail="Ctrl+C received")
        except Exception as exc:  # noqa: BLE001
            verdict, reasons = "ERROR", [f"{type(exc).__name__}: {exc}"]
            rec.add("exception", "local", "", ok=False, detail=str(exc))
        finally:
            if transfer_id is not None:
                self._cleanup(rec, case, transfer_id)

        result = {
            "case_id": case.cid, "mechanism": case.mechanism, "group": case.group,
            "dataset": case.dataset, "spec": case.ds.spec,
            "transfer_type": case.transfer_type, "fault": case.fault,
            "expect": case.expect, "description": case.desc,
            "transfer_id": transfer_id, "capture": capture,
            "verdict": verdict, "reasons": reasons,
            "started": started, "finished": _now(), "steps": rec.steps,
        }
        _write_json(run_dir / "report.json", result)
        LOG.info("=== %s -> %s (%s)", case.cid, verdict, "; ".join(reasons) or "ok")
        if verdict == "INTERRUPTED":
            raise KeyboardInterrupt
        return result

    # -- fallback worker ----------------------------------------------------
    def _run_worker(self, rec: Recorder, case: CompCase):
        ds = case.ds
        src = f"{self.src_base}/{ds.tier_dir}"
        tier = TIER_OF.get(case.dataset, "small")
        prefix = ds.tier_dir

        alloc = self._stage(rec, "alloc transfer id",
                            f"alloc-id --batchmeta {self.batchmeta}")
        transfer_id = alloc.get("transfer_id") if not self.host.dry_run else "<id>"
        td = f"{self.batchmeta}/transfer_{transfer_id}"

        staged = self._stage(
            rec, "stage batch",
            f"stage-batch --src {src} --transfer-dir {td} --tier {tier} "
            f"--name {BATCH_NAME}")
        batch_file = staged.get("batch_file", f"{td}/batches/inprogress/{tier}/{BATCH_NAME}")
        count = staged.get("count")

        # B3 — write done marker BEFORE .lst so the worker starts with it already set.
        if case.fault == "late_lst":
            self._stage(rec, "write done marker (before .lst \u2014 B3 fault injection)",
                        f"done-marker --transfer-dir {td}")

        # Build the .lst — normal, malformed (B1), or with a bad bucket (B2).
        if case.fault == "bad_lst":
            lst = self._stage(
                rec, "make malformed .lst (B1: 2 fields/record, no size/error)",
                f"make-bad-lst --batch {batch_file} --transfer-id {transfer_id} "
                f"--bucket {self.bucket} --prefix {prefix} --fs-prefix {src}")
        elif case.fault == "bad_bucket":
            lst = self._stage(
                rec, "make .lst with nonexistent bucket (B2)",
                f"make-lst --batch {batch_file} --transfer-id {transfer_id} "
                f"--bucket nonexistent-bucket-xyz --prefix {prefix} "
                f"--fs-prefix {src}")
        else:
            lst = self._stage(
                rec, "make retry .lst",
                f"make-lst --batch {batch_file} --transfer-id {transfer_id} "
                f"--bucket {self.bucket} --prefix {prefix} --fs-prefix {src}")

        # Per-fault file-system injections applied after .lst is written.
        if case.fault == "missing":
            # Sizes are already captured in the .lst; delete sources so every
            # _transfer_one stat/upload fails terminally.
            self.host.run(rec, "inject fault: delete sources",
                          f"find {src} -type f -delete", check=False)
        elif case.fault == "no_batch":
            # B4 — batch file removed; complete() will find nothing to rename.
            self.host.run(rec, "inject fault: delete staged batch (B4)",
                          f"rm -f {batch_file}", check=False)
        elif case.fault == "empty_lst":
            # B5 — truncate the .lst to 0 bytes; worker sees zero records.
            lst_path = lst.get("lst_path", "")
            if lst_path:
                self.host.run(rec, "inject fault: truncate .lst to 0 bytes (B5)",
                              f"truncate -s 0 {lst_path}", check=False)

        # Done marker in normal order (late_lst already wrote it above).
        if case.fault != "late_lst":
            self._stage(rec, "write done marker",
                        f"done-marker --transfer-dir {td}")

        self.host.run(
            rec, "run fallback worker",
            f"{self.py} -m bryckcloud.lib.cloud.fallback_worker "
            f"--transfer-id {transfer_id} --transfer-type upload "
            f"--transfer-dir {td} --pool-size {self.pool_size}",
            timeout=self.args.poll_timeout, check=False)

        if self.host.dry_run:
            return "PLANNED", ["dry-run: no execution"], {}, transfer_id

        verify = self._stage(rec, "verify report",
                             f"verify --transfer-id {transfer_id}")
        bstate = self._stage(rec, "batch state",
                            f"batch-state --transfer-dir {td} --name {BATCH_NAME}")
        rc_done, done_out, _ = self.host.run(
            rec, "check .lst.done",
            f"ls {self.logs_dir}/cloud_transfer_{transfer_id}/"
            f"cloudcp_retry_{transfer_id}_batch_000000.txt.lst.done "
            f">/dev/null 2>&1 && echo YES || echo NO", check=False)
        lst_done = "YES" in done_out
        objs = self._s3_object_count(rec, prefix)

        by_status = verify.get("by_status", {})
        fallback_ok = by_status.get("FALLBACK_OK", 0)
        completed = bstate.get("completed", False)
        capture = {"file_count": count, "retry_list_count": lst.get("count"),
                   "by_status": by_status, "batch_completed": completed,
                   "lst_retired": lst_done, "s3_objects": objs}

        reasons = []
        if case.fault in ("bad_lst", "bad_bucket"):
            # B1/B2: all records fail terminally; batch stays inprogress; .lst not retired.
            ok = (fallback_ok == 0 and not completed and not lst_done)
            if fallback_ok != 0:
                reasons.append(f"expected 0 FALLBACK_OK rows, got {fallback_ok}")
            if completed:
                reasons.append("batch unexpectedly moved to completed/")
            if lst_done:
                reasons.append(".lst unexpectedly retired (should stay inprogress for resume)")
            verdict = "PASS" if ok else "FAIL"
            if ok:
                reasons = [f"all {count} record(s) failed terminally as expected; "
                           f"batch inprogress; .lst not retired"]

        elif case.fault == "no_batch":
            # B4: files upload OK, .lst retired, but no batch file moved to completed/.
            ok = (fallback_ok == count and lst_done and not completed)
            if fallback_ok != count:
                reasons.append(f"FALLBACK_OK rows={fallback_ok} != count={count}")
            if not lst_done:
                reasons.append(".lst not retired")
            if completed:
                reasons.append("batch unexpectedly found in completed/ (no batch was staged)")
            verdict = "PASS" if ok else "FAIL"
            if ok:
                reasons = [f"{fallback_ok} file(s) uploaded; .lst retired; "
                           f"batch_state.complete() silently no-op'd (no file to move) \u2014 expected"]

        elif case.fault == "empty_lst":
            # B5: 0 records → immediate complete + retire; 0 uploads.
            ok = (fallback_ok == 0 and completed and lst_done)
            if fallback_ok != 0:
                reasons.append(f"expected 0 FALLBACK_OK rows, got {fallback_ok}")
            if not completed:
                reasons.append("batch not moved to completed/ (expected immediate complete)")
            if not lst_done:
                reasons.append(".lst not retired")
            verdict = "PASS" if ok else "FAIL"
            if ok:
                reasons = ["0-record .lst \u2192 batch immediately completed; .lst retired; 0 uploads"]

        else:
            # Normal, late_lst, missing: standard verdict.
            ok = True
            if case.fault == "missing":
                # All transfers fail → batch stays inprogress, .lst not retired.
                if completed:
                    ok = False
                    reasons.append("batch unexpectedly completed (sources were missing)")
                if lst_done:
                    ok = False
                    reasons.append(".lst retired despite terminal failures")
            else:
                if count is not None and fallback_ok != count:
                    ok = False
                    reasons.append(f"FALLBACK_OK rows={fallback_ok} != file count={count}")
                if not completed:
                    ok = False
                    reasons.append("batch not moved to completed/")
                if not lst_done:
                    ok = False
                    reasons.append(".lst not retired to .lst.done")
                if objs is not None and count is not None and objs < count:
                    ok = False
                    reasons.append(f"s3 objects={objs} < file count={count}")
            verdict = "PASS" if ok else "FAIL"
            if ok:
                reasons = [f"drained {fallback_ok} file(s) via fallback; batch completed"]
        return verdict, reasons, capture, transfer_id

    # -- whole-batch retry --------------------------------------------------
    def _run_mp(self, rec: Recorder, case: CompCase):
        ds = case.ds
        src = f"{self.src_base}/{ds.tier_dir}"
        tier = TIER_OF.get(case.dataset, "small")
        prefix = ds.tier_dir

        alloc = self._stage(rec, "alloc transfer id",
                            f"alloc-id --batchmeta {self.batchmeta}")
        transfer_id = alloc.get("transfer_id") if not self.host.dry_run else "<id>"
        td = f"{self.batchmeta}/transfer_{transfer_id}"

        # B6 — empty_batch: stage a 0-record batch so retry_whole_batch returns (0,0,0).
        empty_flag = "--empty " if case.fault == "empty_batch" else ""
        staged = self._stage(
            rec, "stage batch",
            f"stage-batch {empty_flag}--src {src} --transfer-dir {td} "
            f"--tier {tier} --name {BATCH_NAME}")
        batch_file = staged.get("batch_file", f"{td}/batches/inprogress/{tier}/{BATCH_NAME}")
        count = staged.get("count")  # 0 for empty_batch

        if case.fault == "missing":
            self.host.run(rec, "inject fault: delete sources",
                          f"find {src} -type f -delete", check=False)

        # Resolve effective bucket / fs_prefix per fault type.
        run_bucket = "nonexistent-bucket-xyz" if case.fault == "bad_bucket" else self.bucket
        # B8 — bad_fsp: wrong prefix → compose_s3_key uses basename fallback.
        run_fsp = "/nonexistent/wrong/fsp" if case.fault == "bad_fsp" else src

        result = self._stage(
            rec, "run retry_whole_batch",
            f"run-mp --transfer-id {transfer_id} --batch {batch_file} "
            f"--bucket {run_bucket} --prefix {prefix} --fs-prefix {run_fsp} "
            f"--endpoint {self.endpoint} --region {self.region} "
            f"--transfer-type {case.transfer_type}",
            timeout=self.args.poll_timeout)

        # B9 — resume: second identical call must return (0,0,0) via load_completed dedup.
        result2 = None
        if case.fault == "resume":
            result2 = self._stage(
                rec, "run retry_whole_batch (2nd \u2014 B9 resume idempotency)",
                f"run-mp --transfer-id {transfer_id} --batch {batch_file} "
                f"--bucket {self.bucket} --prefix {prefix} --fs-prefix {src} "
                f"--endpoint {self.endpoint} --region {self.region} "
                f"--transfer-type {case.transfer_type}",
                timeout=self.args.poll_timeout)

        if self.host.dry_run:
            return "PLANNED", ["dry-run: no execution"], {}, transfer_id

        verify = self._stage(rec, "verify report",
                             f"verify --transfer-id {transfer_id}")
        objs = self._s3_object_count(rec, prefix)
        by_status = verify.get("by_status", {})
        mp_ok = by_status.get("MP_OK", 0)
        ok_n = result.get("ok")
        failed_n = result.get("failed")
        capture = {"file_count": count, "ok": ok_n, "failed": failed_n,
                   "ok_bytes": result.get("ok_bytes"), "by_status": by_status,
                   "s3_objects": objs}

        reasons = []
        if case.fault == "bad_bucket":
            # B7: all uploads fail with NoSuchBucket after max_attempts retries.
            good = (ok_n == 0 and failed_n == count)
            if ok_n != 0:
                reasons.append(f"ok={ok_n} (expected 0 — nonexistent bucket)")
            if failed_n != count:
                reasons.append(f"failed={failed_n} != count={count}")
            verdict = "PASS" if good else "FAIL"
            if good:
                reasons = [f"all {count} record(s) failed after retries "
                           f"(nonexistent bucket); no hang/crash"]

        elif case.fault == "empty_batch":
            # B6: (0,0,0) silently; batch_count==0.
            good = (ok_n == 0 and failed_n == 0 and count == 0)
            if ok_n != 0 or failed_n != 0:
                reasons.append(f"expected (ok=0,failed=0) for empty batch; "
                               f"got ok={ok_n} failed={failed_n}")
            if count != 0:
                reasons.append(f"staged batch had {count} records (expected 0 with --empty)")
            verdict = "PASS" if good else "FAIL"
            if good:
                reasons = ["empty batch \u2192 retry_whole_batch returned (0,0,0) silently (expected)"]

        elif case.fault == "bad_fsp":
            # B8: uploads succeed but keys are basename-only; collisions possible.
            good = (ok_n == count and failed_n in (0, None))
            if ok_n != count:
                reasons.append(f"ok={ok_n} != count={count}")
            if failed_n not in (0, None):
                reasons.append(f"failed={failed_n} (expected 0)")
            verdict = "PASS" if good else "FAIL"
            if good:
                reasons = [f"{ok_n} upload(s) succeeded with basename-only keys "
                           f"(wrong fs_prefix); MP_OK rows={mp_ok}"]
            if objs is not None and count is not None and objs < count:
                # Not a FAIL — just confirms the key-collision vulnerability.
                reasons.append(f"NOTE s3 objects={objs} < count={count}: "
                               f"key collisions from basename-only keys confirmed (B8)")

        elif case.fault == "resume":
            # B9: first run uploads all; second run returns (0,0,0) via dedup.
            ok2 = result2.get("ok") if result2 else None
            failed2 = result2.get("failed") if result2 else None
            capture["resume_ok"] = ok2
            capture["resume_failed"] = failed2
            good = (ok_n == count and ok2 == 0 and failed2 == 0)
            if ok_n != count:
                reasons.append(f"first run: ok={ok_n} != count={count}")
            if ok2 != 0 or failed2 != 0:
                reasons.append(f"second run (resume): expected (0,0), "
                               f"got ok={ok2} failed={failed2}")
            verdict = "PASS" if good else "FAIL"
            if good:
                reasons = [f"first run: {ok_n} upload(s); second run: (0,0,0) "
                           f"via load_completed dedup (idempotency confirmed)"]

        elif case.expect == "ok":
            good = True
            if count is not None and ok_n != count:
                good = False
                reasons.append(f"ok={ok_n} != file count={count}")
            if failed_n not in (0, None) and failed_n != 0:
                good = False
                reasons.append(f"failed={failed_n} (expected 0)")
            if count is not None and mp_ok != count:
                good = False
                reasons.append(f"MP_OK rows={mp_ok} != file count={count}")
            if objs is not None and count is not None and objs < count:
                good = False
                reasons.append(f"s3 objects={objs} < file count={count}")
            verdict = "PASS" if good else "FAIL"
            if good:
                reasons = [f"retry_whole_batch uploaded {ok_n} file(s) (MP_OK)"]
        else:
            # Negative: no successful uploads, clean (non-crash) return.
            good = (ok_n == 0)
            if case.fault == "download":
                if failed_n != count:
                    good = False
                reasons.append(f"download not handled: ok={ok_n} failed={failed_n} "
                               f"(expected 0/{count})")
            else:
                if failed_n != count:
                    good = False
                reasons.append(f"all records failed: ok={ok_n} failed={failed_n} "
                               f"(expected 0/{count})")
            verdict = "PASS" if good else "FAIL"
        return verdict, reasons, capture, transfer_id

    # -- cleanup ------------------------------------------------------------
    def _cleanup(self, rec: Recorder, case: CompCase, transfer_id) -> None:
        if getattr(self.args, "skip_cleanup", False):
            rec.add("skip cleanup", "local", "", detail="--skip-cleanup")
            return
        ds = case.ds
        prefix = ds.tier_dir
        src = f"{self.src_base}/{ds.tier_dir}"
        td = f"{self.batchmeta}/transfer_{transfer_id}"
        log = f"{self.logs_dir}/cloud_transfer_{transfer_id}"
        if self.host.dry_run:
            rec.add("cleanup", "plan",
                    f"aws s3 rm --recursive s3://{self.bucket}/{prefix}; "
                    f"rm -rf {src} {td} {log}", detail="(dry-run)")
            return
        self.host.run(rec, "cleanup bucket prefix",
                      f"aws s3 rm --recursive s3://{self.bucket}/{prefix} "
                      f"--endpoint-url {self.endpoint}", check=False)
        # Guard: never rm a bare base path.
        if ds.tier_dir:
            self.host.run(rec, "cleanup /bryck source", f"rm -rf {src}", check=False)
        self.host.run(rec, "cleanup batch-meta transfer dir", f"rm -rf {td}", check=False)
        self.host.run(rec, "cleanup transfer log dir", f"rm -rf {log}", check=False)


# =============================================================================
# Reporting
# =============================================================================

def _render_html(results: list[dict], meta: dict) -> str:
    import html

    def esc(x):
        return html.escape(str(x))

    counts: dict[str, int] = {}
    for r in results:
        counts[r["verdict"]] = counts.get(r["verdict"], 0) + 1
    badges = " ".join(
        f'<span class="badge" style="background:{VERDICT_COLORS.get(k, "#57606a")}">'
        f'{esc(k)}: {v}</span>' for k, v in sorted(counts.items()))

    rows = []
    for r in results:
        color = VERDICT_COLORS.get(r["verdict"], "#57606a")
        cap = r.get("capture", {})
        rows.append(
            f'<tr><td><a href="#{esc(r["case_id"])}">{esc(r["case_id"])}</a></td>'
            f'<td>{esc(r["mechanism"])}</td><td>{esc(r["group"])}</td>'
            f'<td>{esc(r["dataset"])}</td><td>{esc(r["transfer_type"])}</td>'
            f'<td>{esc(r.get("fault") or "-")}</td>'
            f'<td>{esc(cap.get("file_count"))}</td>'
            f'<td><span class="badge" style="background:{color}">{esc(r["verdict"])}</span></td></tr>')

    sections = []
    for r in results:
        color = VERDICT_COLORS.get(r["verdict"], "#57606a")
        cap = r.get("capture", {})
        step_rows = []
        for s in r["steps"]:
            ok = s.get("ok")
            ok_txt = "" if ok is None else ("&#10003;" if ok else "&#10007;")
            ok_col = "#1a7f37" if ok else ("#cf222e" if ok is False else "#57606a")
            out = ""
            if s.get("stdout") or s.get("stderr"):
                se = ("<br>[stderr] " + esc(s["stderr"])) if s.get("stderr") else ""
                out = f'<pre class="io">{esc(s.get("stdout", ""))}{se}</pre>'
            cmd = f'<code>{esc(s["command"])}</code>' if s.get("command") else ""
            detail = ("<div class=detail>" + esc(s["detail"]) + "</div>") if s.get("detail") else ""
            rc_txt = "" if s.get("rc") is None else ("rc=" + esc(s["rc"]))
            step_rows.append(
                f'<tr><td>{s["seq"]}</td><td>{esc(s["kind"])}</td>'
                f'<td>{esc(s["name"])}<br>{cmd}{detail}{out}</td>'
                f'<td style="color:{ok_col};font-weight:700">{ok_txt} {rc_txt}</td></tr>')
        reasons = "".join(f"<li>{esc(x)}</li>" for x in r.get("reasons", []))
        metrics = (f'transfer_id={esc(r.get("transfer_id"))} · '
                   f'files={esc(cap.get("file_count"))} · '
                   f'by_status={esc(cap.get("by_status"))} · '
                   f'ok={esc(cap.get("ok"))} · failed={esc(cap.get("failed"))} · '
                   f'batch_completed={esc(cap.get("batch_completed"))} · '
                   f'lst_retired={esc(cap.get("lst_retired"))} · '
                   f's3_objects={esc(cap.get("s3_objects"))}')
        sections.append(
            f'<section id="{esc(r["case_id"])}" class="case">'
            f'<h3>{esc(r["case_id"])} <span class="badge" style="background:{color}">'
            f'{esc(r["verdict"])}</span></h3>'
            f'<p class="desc">{esc(r["description"])}</p>'
            f'<p class="meta">{metrics}</p>'
            f'{("<ul class=reasons>" + reasons + "</ul>") if reasons else ""}'
            f'<details open><summary>Steps &amp; commands ({len(r["steps"])})</summary>'
            f'<table class="steps"><thead><tr><th>#</th><th>kind</th>'
            f'<th>step / command</th><th>result</th></tr></thead>'
            f'<tbody>{"".join(step_rows)}</tbody></table></details></section>')

    style = """
    body{font-family:-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;margin:0;background:#f6f8fa;color:#1f2328}
    header{background:#24292f;color:#fff;padding:18px 28px}
    header h1{margin:0;font-size:20px}
    .wrap{padding:20px 28px}
    .badge{color:#fff;padding:2px 8px;border-radius:10px;font-size:12px;font-weight:700}
    table{border-collapse:collapse;width:100%;background:#fff;margin:12px 0;font-size:13px}
    th,td{border:1px solid #d0d7de;padding:6px 8px;text-align:left;vertical-align:top}
    th{background:#eaeef2}
    .case{background:#fff;border:1px solid #d0d7de;border-radius:8px;padding:14px 18px;margin:16px 0}
    .case h3{margin:0 0 6px}
    .desc{color:#57606a;margin:4px 0}
    .meta{font-family:ui-monospace,Consolas,monospace;font-size:12px;background:#f6f8fa;padding:6px 8px;border-radius:6px}
    .reasons{margin:8px 0;color:#9a6700}
    code{font-family:ui-monospace,Consolas,monospace;font-size:12px;color:#0550ae;word-break:break-all}
    .detail{color:#57606a;font-size:12px;margin-top:2px}
    pre.io{background:#0d1117;color:#c9d1d9;padding:8px;border-radius:6px;overflow:auto;max-height:220px;font-size:11px;margin:6px 0 0}
    table.steps td:first-child{width:32px;text-align:right;color:#57606a}
    summary{cursor:pointer;font-weight:600;margin:6px 0}
    """
    head = ('<tr><th>case</th><th>mech</th><th>group</th><th>dataset</th>'
            '<th>type</th><th>fault</th><th>files</th><th>verdict</th></tr>')
    return (f'<!doctype html><html><head><meta charset="utf-8">'
            f'<title>CloudCp Component Fallback Report</title><style>{style}</style>'
            f'</head><body><header><h1>CloudCp Component Fallback Report</h1>'
            f'<div>{esc(meta.get("generated"))} · host {esc(meta.get("host"))} · '
            f'{len(results)} case(s)</div></header><div class="wrap">'
            f'<p>{badges}</p><table class="summary"><thead>{head}</thead>'
            f'<tbody>{"".join(rows)}</tbody></table>{"".join(sections)}</div></body></html>')


# =============================================================================
# Entry point (called from cloudcp_fallback_test.main)
# =============================================================================

def run_component_suite(args, host: RemoteHost, session=None) -> int:
    catalog = build_component_catalog()
    selected = select_component_cases(args, catalog)
    if not selected:
        LOG.error("No component cases selected. Use --component, --component-one, "
                  "--component-negative, or --component-list.")
        return 2

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    runner = ComponentRunner(args, host)
    results: list[dict] = []
    interrupted = False
    case = None
    try:
        for case in selected:
            results.append(runner.run_case(case))
    except KeyboardInterrupt:
        interrupted = True
        if case is not None:
            last = out_dir / case.cid / "report.json"
            if last.is_file():
                try:
                    results.append(json.loads(last.read_text(encoding="utf-8")))
                except Exception:  # noqa: BLE001
                    pass
        LOG.warning("Interrupted (Ctrl+C) — writing report for %d case(s).", len(results))

    host_ip = getattr(session, "host", "(dry-run)") if session else "(dry-run)"
    meta = {"generated": _now(), "host": host_ip, "dry_run": host.dry_run,
            "interrupted": interrupted}
    _write_json(out_dir / "component_report.json", {"meta": meta, "results": results})
    (out_dir / "component_report.html").write_text(
        _render_html(results, meta), encoding="utf-8")

    tally: dict[str, int] = {}
    for r in results:
        tally[r["verdict"]] = tally.get(r["verdict"], 0) + 1
    LOG.info("Component run done. %s", " ".join(f"{k}={v}" for k, v in sorted(tally.items())))
    LOG.info("Reports: %s , %s", out_dir / "component_report.json",
             out_dir / "component_report.html")
    bad = tally.get("FAIL", 0) + tally.get("ERROR", 0)
    return 1 if bad or interrupted else 0
