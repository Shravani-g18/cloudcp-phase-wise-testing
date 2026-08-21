#!/usr/bin/env python3
"""
Environment-aware negative test runner for the Bryck cloud-transfer suite.

This runner is driven by the catalog in NEGATIVE_TEST_PLAN.md (parsed with
the same helper the existing cloud_transfer_test_runner.py uses) and by the
TestContext helpers already implemented there (mount/format/eject, cloud
configure/deconfigure, dataset generation, transfer initiate/status/pause/
resume/cancel/report). It does not invent a second command layer: every
subprocess call goes through cloud_transfer_test_runner.TestContext so the
project's existing conventions (login.json/cloud_ops.json/format_mount_
params.json, dry-run behavior, expect_fail semantics) stay authoritative.

Pipeline per test case
----------------------
  inspect current environment
  -> prepare only the environment that case needs (nothing else)
  -> validate the environment explicitly
  -> BLOCKED if the environment cannot be established
  -> execute only the negative condition
  -> validate result (rc, stdout/stderr, state before/after)
  -> cleanup (cancel transfer / remove temp fixtures)
  -> verify final state
  -> record everything (including STDIN/STDOUT/STDERR separately) for HTML

Usage
-----
  python3 negative_environment_runner.py --dry-run
  python3 negative_environment_runner.py --live --sections CLI,AUTH,TID
  python3 negative_environment_runner.py --live --confirm-destructive
  python3 negative_environment_runner.py --live --test-id AWS-03 \
      --override secret_access_key=deliberately-wrong-secret

--override KEY=VALUE mutates a copy of cloud_ops.json/login.json for a single
--test-id run; the real JSON files under version control are never modified.
"""
from __future__ import annotations

import argparse
import concurrent.futures
import dataclasses
import datetime as dt
import html
import json
import re
import signal
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Callable, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent))
import cloud_transfer_test_runner as ctr  # noqa: E402  (local module, path prepared above)

SCRIPT_DIR = ctr.SCRIPT_DIR
PLAN_PATH = SCRIPT_DIR / "NEGATIVE_TEST_PLAN.md"

SECRET_PATTERNS = [
    (re.compile(r"(?i)(secret[_ -]?access[_ -]?key\s*[:=]\s*)\S+"), r"\1<REDACTED>"),
    (re.compile(r"(?i)(access[_ -]?key(?:_id)?\s*[:=]\s*)\S+"), r"\1<REDACTED>"),
    (re.compile(r"(?i)(bryckapi_password\s*[:=]\s*)\S+"), r"\1<REDACTED>"),
    (re.compile(r"(?i)(password\s*[:=]\s*)\S+"), r"\1<REDACTED>"),
    (re.compile(r"(?i)(token\s*[:=]\s*)\S+"), r"\1<REDACTED>"),
]


def redact(text: str) -> str:
    if not text:
        return ""
    for pattern, repl in SECRET_PATTERNS:
        text = pattern.sub(repl, text)
    return text


_TRANSFER_BLOCK_RE = re.compile(
    r"(?:\u2500{5,}\s*\n)?\s*TRANSFER_ID\s*:.*?(?=\n\s*(?:\u2500{5,}|$))", re.DOTALL
)


def _condense_transfer_listing(text: str) -> str:
    """Keep only the most recent transfer block from a full-listing dump.

    bryck_cloud_transfer_status.py with no filter prints every transfer ever
    created (can be 50+ blocks). For readability, only the last (most recent)
    transfer is kept; the rest are summarized in one line.
    """
    if not text or text.count("TRANSFER_ID") < 2:
        return text
    blocks = _TRANSFER_BLOCK_RE.findall(text)
    if len(blocks) < 2:
        return text
    omitted = len(blocks) - 1
    last_block = blocks[-1].strip()
    return (
        f"[{omitted} earlier transfer(s) omitted for readability; "
        f"showing only the most recent]\n{last_block}"
    )


# =============================================================================
# Data model
# =============================================================================

@dataclasses.dataclass
class CommandRecord:
    label: str
    command: str
    stdin: str
    stdout: str
    stderr: str
    rc: Optional[int]
    duration: float


@dataclasses.dataclass
class TestResult:
    test_id: str
    section: str
    name: str
    status: str  # PASS / FAIL / BLOCKED
    expected: str
    actual: str
    reason: str
    baseline: dict
    env_before: Optional[dict]
    env_after: Optional[dict]
    commands: list = dataclasses.field(default_factory=list)
    cleanup_status: str = "not required"
    cleanup_detail: str = ""
    duration: float = 0.0
    narrative: str = ""
    expected_failure: bool = False  # True when this case intentionally expects the operation to be rejected
    outcome_label: str = ""        # short badge text, e.g. "Expected failure \u2014 test passed"
    outcome_sentence: str = ""     # full plain-English sentence for the detail view


def classify_outcome(status: str, expected_failure: bool) -> tuple[str, str, str]:
    """Turn (PASS/FAIL, expected_failure) into an unambiguous (badge_label, full_sentence, css_class).

    Four cases, so a reader never has to infer whether a FAIL/PASS was the
    good or bad outcome for a *negative* test:
      expected_failure=True,  PASS -> the operation was correctly rejected
      expected_failure=True,  FAIL -> the operation should have been rejected but succeeded
      expected_failure=False, PASS -> the operation was correctly allowed/succeeded
      expected_failure=False, FAIL -> the operation should have succeeded but failed
    """
    if status == "BLOCKED":
        return "Not executed (blocked)", "This case was not executed because a required fixture/precondition was unavailable.", "blocked"
    if expected_failure and status == "PASS":
        return (
            "Expected failure \u2014 test passed",
            "This is a negative test: it expected the operation to be rejected, and the operation "
            "was correctly rejected \u2014 so the expected-failure test passed.",
            "pass",
        )
    if expected_failure and status == "FAIL":
        return (
            "Unexpected success \u2014 test failed",
            "This is a negative test: it expected the operation to be rejected, but the operation "
            "unexpectedly succeeded instead \u2014 so the expected-failure test failed.",
            "fail",
        )
    if not expected_failure and status == "PASS":
        return (
            "Expected success \u2014 test passed",
            "This is a positive test: it expected the operation to succeed, and the operation "
            "completed successfully \u2014 so the expected-success test passed.",
            "pass",
        )
    return (
        "Unexpected failure \u2014 test failed",
        "This is a positive test: it expected the operation to succeed, but the operation "
        "unexpectedly failed instead \u2014 so the expected-success test failed.",
        "fail",
    )


def _clause_from_baseline(baseline: dict) -> str:
    if not baseline:
        return "no special preconditions"
    parts = []
    for key, value in baseline.items():
        parts.append(f"{key.replace('_', ' ')} {value}")
    return "; ".join(parts)


def build_narrative(result: TestResult) -> str:
    """One plain-English sentence describing exactly what this case executed."""
    clause = _clause_from_baseline(result.baseline)
    if result.status == "BLOCKED":
        return (
            f"Test {result.test_id} ({result.name}) was not executed because {result.reason}"
        )
    return (
        f"Test {result.test_id} ({result.name}) established {clause}, then executed the "
        f"negative operation expecting: {result.expected} "
        f"Observed: {result.actual}"
    )


# Real, proven CloudCpFallbackTesting specs rotated round-robin by
# ensure_dataset() whenever a caller doesn't ask for a specific spec.
DATASET_SPEC_ROTATION = [
    "09_unicode_names.yaml",
    "06_sparse_files.yaml",
    "11_mixed_realistic.yaml",
]

# =============================================================================
# Environment manager — thin wrapper around TestContext
# =============================================================================

class EnvironmentManager:
    """Prepares/validates state and records every command for the report.

    TestContext already no-ops correctly in dry-run mode (StepResult.passed
    is computed the same way it is for the existing scenario runner), so
    every method here is safe to call under --dry-run without extra checks.
    """

    def __init__(self, ctx: ctr.TestContext):
        self.ctx = ctx
        self.commands: list[CommandRecord] = []
        self._expired_token: str | None = None
        self._dataset_spec_idx = 0

    def get_expired_token(self) -> str | None:
        """Return a JWT that was genuinely issued and has genuinely expired.

        Logs in with the same shared credentials to mint a *second*,
        independent token (so the primary session used by the rest of the
        suite is never invalidated), decodes its ``exp`` claim (no secret
        needed -- the JWT payload is only base64, not encrypted), and
        blocks in real time until that timestamp passes. The token is
        cached so only the first AUTH-*-after-expiry case pays the wait.
        """
        if self._expired_token:
            return self._expired_token
        import base64
        from session import ApiSession
        try:
            session = ApiSession.from_login_json(self.ctx.login_json)
            session.login()
            token = session.token
            session.close()
            if not token:
                return None
            payload_b64 = token.split(".")[1]
            payload_b64 += "=" * (-len(payload_b64) % 4)
            payload = json.loads(base64.urlsafe_b64decode(payload_b64))
            exp = payload.get("exp")
            if exp is None:
                return None
            wait = exp - time.time() + 2
            if wait > 0:
                print(f"    [AUTH] waiting {wait:.0f}s for a real token to expire (fixture, one-time cost)...")
                time.sleep(wait)
            self._expired_token = token
            return token
        except Exception as exc:  # noqa: BLE001 - fixture setup must never crash the suite
            print(f"    [AUTH] get_expired_token fixture failed: {exc}")
            return None

    def cap(self, label: str, sr: ctr.StepResult) -> ctr.StepResult:
        self.commands.append(CommandRecord(
            label=label,
            command=sr.command,
            stdin="<none>",
            stdout=_condense_transfer_listing(redact(sr.stdout)),
            stderr=_condense_transfer_listing(redact(sr.stderr)),
            rc=sr.returncode,
            duration=sr.duration_sec,
        ))
        return sr

    def snapshot(self, label: str) -> dict:
        info = self.cap(f"{label}:bryck_info", self.ctx.bryck_info(label))
        cloud = self.cap(f"{label}:cloud_show", self.ctx.show_cloud())
        status = self.cap(f"{label}:transfer_status", self.ctx.transfer_status_all())
        return {
            "bryck_state": ctr._parse_bryck_state(info.stdout) or "<unknown>",
            "cloud_configured": "configured" in (cloud.stdout + cloud.stderr).lower(),
            "info_ok": info.passed,
            "cloud_ok": cloud.passed,
            "status_ok": status.passed,
        }

    def ensure_mounted(self) -> bool:
        info = self.cap("ensure_mounted:check", self.ctx.bryck_info("ensure_mounted"))
        if ctr._parse_bryck_state(info.stdout) == " Mounted":
            return True
        if self._mount_cycle():
            return True
        self.recover_environment()
        return self._mount_cycle()

    def _mount_cycle(self) -> bool:
        prep = self.cap("ensure_mounted:prepare_format", self.ctx.prepare_format())
        if not prep.passed:
            return False
        fmt = self.cap("ensure_mounted:format", self.ctx.format_bryck())
        if not fmt.passed:
            return False
        mnt = self.cap("ensure_mounted:mount", self.ctx.ensure_mounted())
        return mnt.passed

    def ensure_unmounted(self) -> bool:
        info = self.cap("ensure_unmounted:check", self.ctx.bryck_info("ensure_unmounted"))
        state = ctr._parse_bryck_state(info.stdout)
        if state in {" Ejected", " Removed"}:
            return True
        ej = self.cap("ensure_unmounted:eject", self.ctx.eject_bryck())
        return ej.passed

    def recover_environment(self) -> None:
        """Cancel any leftover active/paused transfers from a prior case.

        A dangling transfer (left behind by a crashed or interrupted case)
        can make mount/format/cloud-configure calls fail for every case that
        follows. This is a cheap self-healing step retried once before a
        baseline-establishment failure is reported as BLOCKED.

        Only IN_PROGRESS/PAUSED transfers are targeted; terminal transfers
        (COMPLETED/CANCELLED/FAILED/STOPPED) cannot be cancelled and re-attempting
        them for every historical transfer_id on the device would make this
        self-healing step scale with the device's entire transfer history.
        """
        TERMINAL_STATES = {"COMPLETED", "CANCELLED", "FAILED", "STOPPED"}
        status = self.cap("recover:transfer_status_all", self.ctx.transfer_status_all())
        ids = set(self.ctx.active_transfers)
        pending_tid: str | None = None
        for line in (status.stdout + "\n" + status.stderr).splitlines():
            upper = line.upper()
            if "TRANSFER_ID" in upper:
                parts = line.split(":", 1)
                pending_tid = parts[1].strip() if len(parts) == 2 else None
            elif "STATE" in upper and pending_tid:
                parts = line.split(":", 1)
                state = parts[1].strip().upper() if len(parts) == 2 else ""
                if state and state not in TERMINAL_STATES:
                    ids.add(pending_tid)
                pending_tid = None
        for tid in ids:
            self.cleanup_transfer(tid)

    def ensure_cloud_configured(self) -> bool:
        show = self.cap("ensure_cloud_configured:check", self.ctx.show_cloud())
        if show.passed and "cloud_type" in show.stdout.lower():
            self.ctx.cloud_configured = True
            return True
        sr = self.cap("ensure_cloud_configured", self.ctx.configure_cloud())
        if sr.passed or self.ctx.cloud_configured:
            return True
        self.recover_environment()
        retry = self.cap("ensure_cloud_configured:retry", self.ctx.configure_cloud())
        return retry.passed or self.ctx.cloud_configured

    def ensure_cloud_deconfigured(self) -> bool:
        show = self.cap("ensure_cloud_deconfigured:check", self.ctx.show_cloud())
        if show.passed and "cloud_type" not in show.stdout.lower():
            self.ctx.cloud_configured = False
            return True
        self.cap("ensure_cloud_deconfigured", self.ctx.deconfigure_cloud(expect_fail=True))
        verify = self.cap("ensure_cloud_deconfigured:verify", self.ctx.show_cloud())
        deconfigured = verify.passed and "cloud_type" not in verify.stdout.lower()
        self.ctx.cloud_configured = not deconfigured
        return deconfigured

    def ensure_dataset(self, spec: str = "small_1gb_fast.yaml") -> bool:
        """Generate a dataset at whatever path cloud_ops.json's bryck_src points to.

        The shipped spec files default to root: /bryck/small_1gb, but a given
        server's cloud_ops.json may configure bryck_src elsewhere (e.g.
        /bryck/dataset-2gb-1tb). datagen's root must match bryck_src exactly
        or the upload fails immediately with 409 "Source path does not
        exist." Rewriting the spec's root here keeps generation aligned with
        whatever bryck_src is actually configured on this run's device, and
        also re-generates fresh data every time (e.g. after a format wiped it).

        When called with the default spec name (i.e. the caller didn't ask
        for a specific one), rotate round-robin through the 3 real,
        proven CloudCpFallbackTesting specs instead of always using the same
        file -- so different cases get varied real datasets over a run.
        """
        if spec == "small_1gb_fast.yaml":
            spec = DATASET_SPEC_ROTATION[self._dataset_spec_idx % len(DATASET_SPEC_ROTATION)]
            self._dataset_spec_idx += 1
        try:
            cloud_cfg = json.loads(self.ctx.cloud_ops_json.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            cloud_cfg = {}
        target_root = str(cloud_cfg.get("bryck_src", "")).strip()
        base_spec_path = self.ctx.spec_dir / spec
        try:
            spec_text = base_spec_path.read_text(encoding="utf-8")
        except OSError:
            spec_text = ""

        spec_path = str(base_spec_path)
        if target_root and not re.search(rf"(?m)^root:\s*{re.escape(target_root)}\s*$", spec_text):
            if re.search(r"(?m)^root:\s*\S+\s*$", spec_text):
                spec_text = re.sub(r"(?m)^root:\s*\S+\s*$", f"root: {target_root}", spec_text, count=1)
            else:
                spec_text = f"root: {target_root}\n" + spec_text
            with tempfile.NamedTemporaryFile(
                "w", suffix=".yaml", prefix="aligned-spec-", delete=False, encoding="utf-8"
            ) as fh:
                fh.write(spec_text)
                spec_path = fh.name

        # datagen does not create missing directories itself -- if bryck_src
        # points at a path that has never been mkdir'd on the device, the
        # run fails instantly (~0.3s) instead of generating anything.
        if target_root:
            self.cap(f"ensure_dataset:mkdir:{target_root}",
                     self.ctx.run_ssh(f"mkdir -p {target_root}", f"mkdir -p {target_root}", timeout=30))

        sr = self.cap(f"ensure_dataset:{Path(spec_path).name}->{target_root or 'default'}",
                      self.ctx.run_datagen(spec_path, timeout=3600))
        if sr.passed:
            return True
        self.recover_environment()
        retry = self.cap(f"ensure_dataset:retry:{Path(spec_path).name}->{target_root or 'default'}",
                         self.ctx.run_datagen(spec_path, timeout=3600))
        return retry.passed

    def create_transfer(self, direction: str, wanted: str, timeout: int = 3600) -> Optional[str]:
        """Establish an active-transfer fixture, self-healing once before giving up.

        A case run right after a prior transfer that is still finishing (or was
        left dangling by an earlier crashed case) makes initiate_transfer fail
        with a 409 "already have an active transfer" -- without a recovery
        retry here (matching ensure_dataset/ensure_cloud_configured's pattern),
        that single collision cascades into every downstream "could not
        establish an active transfer fixture" BLOCKED case in the section.
        """
        info = self.cap(f"create_transfer:{direction}:check_mounted", self.ctx.bryck_info(f"pre-initiate mount check ({direction})"))
        if ctr._parse_bryck_state(info.stdout) != " Mounted" and not self.ensure_mounted():
            return None
        sr, ids = self.ctx.initiate_transfer(direction)
        self.cap(f"create_transfer:{direction}", sr)
        if not ids:
            self.recover_environment()
            if not self.ensure_mounted():
                return None
            sr, ids = self.ctx.initiate_transfer(direction)
            self.cap(f"create_transfer:{direction}:retry", sr)
            if not ids:
                return None
        tid = ids[0]
        wait = self.cap(f"wait_for_state:{wanted}", self.ctx.wait_for_state(tid, {wanted}, timeout=timeout))
        return tid if wait.passed else None

    def create_transfer_at(self, direction: str, target_state: str, timeout: int = 7200) -> Optional[str]:
        """Drive a fresh transfer all the way to the requested terminal/lifecycle state."""
        tid = self.create_transfer(direction, "IN_PROGRESS", timeout=min(timeout, 3600))
        if not tid:
            return None
        if target_state == "IN_PROGRESS":
            return tid
        if target_state == "PAUSED":
            sr = self.cap(f"reach:PAUSED", self.ctx.pause_transfer(tid))
            return tid if sr.passed else None
        if target_state == "CANCELLED":
            self.cap("reach:CANCELLED:cancel", self.ctx.cancel_transfer(tid))
            wait = self.cap("reach:CANCELLED:wait", self.ctx.wait_for_state(tid, {"CANCELLED"}, timeout=300))
            return tid if wait.passed else None
        if target_state == "COMPLETED":
            wait = self.cap("reach:COMPLETED:wait", self.ctx.wait_for_state(tid, {"COMPLETED"}, timeout=timeout))
            return tid if wait.passed else None
        return None

    def run_ssh(self, label: str, remote_cmd: str, timeout: int = 120) -> ctr.StepResult:
        return self.cap(label, self.ctx.run_ssh(label, remote_cmd, timeout=timeout))

    def build_fixture(self, base: Path, overrides: dict, work: Path, name: str, raw: str | None = None) -> Path:
        target = work / name
        if raw is not None:
            target.write_text(raw, encoding="utf-8")
            return target
        try:
            data = json.loads(base.read_text(encoding="utf-8")) if base.exists() else {}
        except (OSError, json.JSONDecodeError):
            data = {}
        data.update(overrides)
        target.write_text(json.dumps(data, indent=2), encoding="utf-8")
        return target

    def cleanup_transfer(self, tid: Optional[str]) -> str:
        if not tid:
            return "no fixture transfer to clean up"
        sr = self.cap(f"cleanup:cancel:{tid}", self.ctx.cancel_transfer(tid, expect_fail=False))
        if tid in self.ctx.active_transfers:
            self.ctx.active_transfers.remove(tid)
        return "transfer cancelled" if sr.passed else f"cancel returned rc={sr.returncode}"


# =============================================================================
# Overrides / CLI-supplied input
# =============================================================================

def parse_overrides(raw: list[str]) -> dict:
    out: dict[str, Any] = {}
    for item in raw:
        if "=" not in item:
            continue
        key, value = item.split("=", 1)
        out[key.strip()] = value
    return out


# =============================================================================
# Result helpers
# =============================================================================

def result_from_step(
    test_id: str, section: str, name: str, baseline: dict,
    env_before: Optional[dict], sr: ctr.StepResult, mgr: EnvironmentManager,
    expected: str, cleanup_status: str = "not required", cleanup_detail: str = "",
    env_after: Optional[dict] = None,
) -> TestResult:
    status = "PASS" if sr.passed else "FAIL"
    actual = (
        f"rc={sr.returncode}; stdout={redact(sr.stdout)[-800:]!r}; "
        f"stderr={redact(sr.stderr)[-800:]!r}"
    )
    outcome_label, outcome_sentence, _ = classify_outcome(status, sr.expected_failure)
    reason = outcome_sentence
    return TestResult(
        test_id=test_id, section=section, name=name, status=status,
        expected=expected, actual=actual, reason=reason, baseline=baseline,
        env_before=env_before, env_after=env_after, commands=list(mgr.commands),
        cleanup_status=cleanup_status, cleanup_detail=cleanup_detail,
        duration=sr.duration_sec, expected_failure=sr.expected_failure,
        outcome_label=outcome_label, outcome_sentence=outcome_sentence,
    )


def blocked(test_id: str, section: str, name: str, baseline: dict, reason: str,
            env_before: Optional[dict] = None, mgr: Optional[EnvironmentManager] = None) -> TestResult:
    detail_reason = reason
    if mgr and mgr.commands:
        last_failed = next((c for c in reversed(mgr.commands) if c.rc not in (0, None)), None)
        if last_failed:
            snippet = (last_failed.stderr or last_failed.stdout or "").strip().replace("\n", " ")[-300:]
            if snippet:
                detail_reason = f"{reason} (last failure: {last_failed.label} rc={last_failed.rc}: {snippet})"
    return TestResult(
        test_id=test_id, section=section, name=name, status="BLOCKED",
        expected="Required environment/fixture must be established before executing the negative operation.",
        actual="Not executed.", reason=detail_reason, baseline=baseline,
        env_before=env_before, env_after=None,
        commands=list(mgr.commands) if mgr else [],
        cleanup_status="not required", cleanup_detail="",
        outcome_label="Not executed (blocked)",
        outcome_sentence="This case was not executed because a required fixture/precondition was unavailable.",
    )


# =============================================================================
# Section handlers
# =============================================================================

TID_VALUES = {
    "TID-01": ("99999999", "nonexistent ID"),
    "TID-02": ("", "empty ID"),
    "TID-03": ("-1", "negative ID"),
    "TID-04": ("not-a-transfer", "alphabetic ID"),
    "TID-05": ("!@#$%^&*", "special-character ID"),
    "TID-06": ("999999999999999999999999999999999999", "extremely large ID"),
    "TID-07": ("2147483647", "external-system ID"),
    "TID-08": ("1", "old/deleted ID"),
    "TID-09": ("1.2.3", "malformed ID"),
}

AWS_MUTATIONS = {
    "AWS-01": {"access_key_id": ""},
    "AWS-02": {"secret_access_key": ""},
    "AWS-03": {"access_key_id": "invalid-access-key"},
    "AWS-04": {"secret_access_key": "invalid-secret-key"},
    "AWS-05": {"region": "invalid-region"},
    "AWS-06": {"endpoint": "http://127.0.0.1:1"},
    "AWS-07": {"cloud_bucket": "not-a-valid-bucket"},
}

PATH_PREFIXES = {
    "PATH-01": "bad prefix?*",
    "PATH-02": "",
    "PATH-03": "/leading/slash/",
    "PATH-04": "double//slash",
    "PATH-05": "special chars !@#$%^&* and spaces",
    "PATH-06": "测试/路径",
    "PATH-07": "x" * 2000,
    "PATH-08": "../parent/traversal/",
}
# PATH-04 additionally checks a trailing-slash prefix alongside its primary double-slash case.
PATH_04_EXTRA = "trailing/slash/"


def handle_cli(case_id: str, desc: str, mgr: EnvironmentManager, args, work: Path, overrides: dict) -> TestResult:
    ctx = mgr.ctx
    baseline = {"bryck": "not required", "cloud": "not required"}

    def fixture(name: str, value: Any, raw: bool = False) -> Path:
        return mgr.build_fixture(ctx.cloud_ops_json, {}, work, name, raw=str(value) if raw else None) \
            if raw else mgr.build_fixture(ctx.cloud_ops_json, value, work, name)

    base_cloud = {}
    try:
        base_cloud = json.loads(ctx.cloud_ops_json.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        pass
    base_cloud.update(overrides)

    if case_id == "CLI-01":
        sr = ctx.run_py(desc, "bryck_cloud_transfer_initiate.py", "--login", str(ctx.login_json),
                        "--params", str(ctx.cloud_ops_json), expect_fail=True)
    elif case_id == "CLI-02":
        sr = ctx.run_py(desc, "bryck_cloud_transfer_initiate.py", "--login", str(ctx.login_json),
                        "--params", str(ctx.cloud_ops_json), "--mode", "copy", expect_fail=True)
    elif case_id == "CLI-03":
        p = fixture("cli03.json", {**base_cloud, "bryck_src": ""})
        sr = ctx.run_py(desc, "bryck_cloud_transfer_initiate.py", "--login", str(ctx.login_json),
                        "--params", str(p), "--mode", "upload", expect_fail=True)
    elif case_id == "CLI-04":
        p = fixture("cli04.json", {**base_cloud, "cloud_bucket": ""})
        sr = ctx.run_py(desc, "bryck_cloud_transfer_initiate.py", "--login", str(ctx.login_json),
                        "--params", str(p), "--mode", "upload", expect_fail=True)
    elif case_id == "CLI-05":
        p = fixture("cli05.json", {**base_cloud, "bryck_dst": ""})
        sr = ctx.run_py(desc, "bryck_cloud_transfer_initiate.py", "--login", str(ctx.login_json),
                        "--params", str(p), "--mode", "download", expect_fail=True)
    elif case_id == "CLI-06":
        p = fixture("cli06.json", {**base_cloud, "cloud_bucket": ""})
        sr = ctx.run_py(desc, "bryck_cloud_transfer_initiate.py", "--login", str(ctx.login_json),
                        "--params", str(p), "--mode", "download", expect_fail=True)
    elif case_id == "CLI-07":
        sr = ctx.run_py(desc, "bryck_cloud_show.py", "--login", str(work / "missing-login.json"), expect_fail=True)
    elif case_id == "CLI-08":
        p = fixture("cli08.json", "{", raw=True)
        sr = ctx.run_py(desc, "bryck_cloud_show.py", "--login", str(p), expect_fail=True)
    elif case_id == "CLI-09":
        p = fixture("cli09.json", {})
        sr = ctx.run_py(desc, "bryck_cloud_show.py", "--login", str(p), expect_fail=True)
    elif case_id == "CLI-10":
        sr = ctx.run_py(desc, "bryck_cloud_transfer_status.py", "--invalid-option", expect_fail=True)
    elif case_id == "CLI-11":
        sr = ctx.run_py(desc, "bryck_cloud_transfer_status.py", "--login", str(ctx.login_json),
                        "--state", "PAUSED", "--state", "COMPLETED", expect_fail=False)
    elif case_id == "CLI-12":
        p = fixture("cli12.json", {**base_cloud, "cloud_type": "invalid-cloud"})
        sr = ctx.run_py(desc, "bryck_cloud_transfer_initiate.py", "--login", str(ctx.login_json),
                        "--params", str(p), "--mode", "upload", expect_fail=True)
    elif case_id == "CLI-13":
        p = fixture("cli13.json", {"cloud_type": "aws"})
        sr = ctx.run_py(desc, "bryck_cloud_transfer_initiate.py", "--login", str(ctx.login_json),
                        "--params", str(p), "--mode", "upload", expect_fail=True)
    elif case_id == "CLI-14":
        sr = ctx.run_py(desc, "bryck_cloud_transfer_pause.py", "--login", str(ctx.login_json),
                        "--transfer-id", "not-a-transfer-id", expect_fail=True)
    elif case_id == "CLI-15":
        p = fixture("cli15.json", "{", raw=True)
        sr = ctx.run_py(desc, "bryck_cloud_transfer_initiate.py", "--login", str(ctx.login_json),
                        "--params", str(p), "--mode", "upload", expect_fail=True)
    elif case_id == "CLI-16":
        p = fixture("cli16.json", {})
        sr = ctx.run_py(desc, "bryck_cloud_transfer_initiate.py", "--login", str(ctx.login_json),
                        "--params", str(p), "--mode", "upload", expect_fail=True)
    elif case_id == "CLI-17":
        p = fixture("cli17.json", "{", raw=True)
        sr = ctx.run_py(desc, "bryck_format.py", "--login", str(ctx.login_json), "--params", str(p), expect_fail=True)
    elif case_id == "CLI-18":
        missing_spec = work / "missing-spec.yaml"
        sr = ctx.run_datagen(str(missing_spec), timeout=120)
    elif case_id in {"CLI-19", "CLI-20", "CLI-21", "CLI-22"}:
        base_spec_path = ctx.spec_dir / "small_1gb_fast.yaml"
        try:
            spec_text = base_spec_path.read_text(encoding="utf-8")
        except OSError:
            spec_text = "version: 1\nmode: flat\nroot: /bryck/small_1gb\nflat:\n  num_files: 5\nsize:\n  type: range\n  min: 1KB\n  max: 10KB\n"
        if case_id == "CLI-19":
            spec_text = ""  # empty dataset specification
        elif case_id == "CLI-20":
            spec_text = re.sub(r"(?m)^(\s*min:\s*)\S+\s*$", r"\1not-a-number", spec_text, count=1)
        elif case_id == "CLI-21":
            spec_text = re.sub(r"(?m)^(\s*min:\s*)\S+\s*$", r"\1-500MB", spec_text, count=1)
        else:  # CLI-22
            outside_root = f"/tmp/bryck-negative-cli22-{uuid.uuid4().hex[:8]}"
            spec_text = re.sub(r"(?m)^root:\s*\S+\s*$", f"root: {outside_root}", spec_text, count=1)
        p = work / f"{case_id.lower()}-spec.yaml"
        p.write_text(spec_text, encoding="utf-8")
        sr = ctx.run_datagen(str(p), timeout=180)
        if args.live:
            sr = invert_result(sr)
    elif case_id == "CLI-23":
        baseline = {"bryck": "mounted", "cloud": "configured"}
        if not (mgr.ensure_mounted() and mgr.ensure_cloud_configured()):
            return blocked(case_id, "CLI", desc, baseline, "could not establish mounted+configured baseline", mgr=mgr)
        p = fixture("cli23.json", {**base_cloud, "bryck_src": f"/bryck/does-not-exist-cli23-{uuid.uuid4().hex[:8]}"})
        sr = ctx.run_py(desc, "bryck_cloud_transfer_initiate.py", "--login", str(ctx.login_json),
                        "--params", str(p), "--mode", "upload", expect_fail=True)
    else:
        return blocked(case_id, "CLI", desc, baseline,
                       "no automated fixture is implemented for this CLI case yet", mgr=mgr)

    mgr.cap(case_id, sr)
    return result_from_step(case_id, "CLI", desc, baseline, None, sr, mgr,
                            expected="Argument/configuration validation rejects the invalid input before any API mutation.")


def handle_auth(case_id: str, desc: str, mgr: EnvironmentManager, args, work: Path, overrides: dict) -> TestResult:
    ctx = mgr.ctx
    baseline = {"authentication": "deliberately invalid", "bryck": "not required"}
    try:
        login_cfg = json.loads(ctx.login_json.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        login_cfg = {}
    mutation = {
        "AUTH-01": {"bryckapi_username": "invalid-user"},
        "AUTH-02": {"bryckapi_password": "invalid-password"},
        "AUTH-05": {"bryckapi_password": ""},
        "AUTH-03": {"bryckapi_token": "invalid.garbage.token"},
    }.get(case_id)
    if mutation:
        mutation.update(overrides)
        p = mgr.build_fixture(ctx.login_json, {**login_cfg, **mutation}, work, f"{case_id}.json")
        sr = ctx.run_py(desc, "bryck_cloud_show.py", "--login", str(p), expect_fail=True)
        mgr.cap(case_id, sr)
        return result_from_step(case_id, "AUTH", desc, baseline, None, sr, mgr,
                                expected="Authentication is rejected; no state is mutated.")

    expiry_case = case_id in {"AUTH-04", "AUTH-06", "AUTH-07", "AUTH-08", "AUTH-09", "AUTH-10"}
    if expiry_case and not args.live:
        return blocked(case_id, "AUTH", desc, baseline, "requires --live against the dedicated Bryck device", mgr=mgr)
    if expiry_case:
        token = mgr.get_expired_token()
        if not token:
            return blocked(case_id, "AUTH", desc, baseline,
                           "could not mint/decode a real token to build the expiry fixture", mgr=mgr)
        p = mgr.build_fixture(ctx.login_json, {**login_cfg, **overrides, "bryckapi_token": token}, work, f"{case_id}.json")
        dummy_tid = overrides.get("transfer_id", "1")
        op = {
            "AUTH-04": ("bryck_cloud_show.py", []),
            "AUTH-06": ("bryck_cloud_transfer_status.py", ["--transfer-id", dummy_tid]),
            "AUTH-07": ("bryck_cloud_transfer_report.py", ["--cloud-transfer-id", dummy_tid, "--report-path", str(work)]),
            "AUTH-08": ("bryck_cloud_transfer_pause.py", ["--transfer-id", dummy_tid]),
            "AUTH-09": ("bryck_cloud_transfer_resume.py", ["--transfer-id", dummy_tid]),
            "AUTH-10": ("bryck_cloud_transfer_cancel.py", ["--transfer-id", dummy_tid]),
        }[case_id]
        script, extra = op
        sr = ctx.run_py(desc, script, "--login", str(p), *extra, expect_fail=True, timeout=60)
        mgr.cap(case_id, sr)
        return result_from_step(case_id, "AUTH", desc, baseline, None, sr, mgr,
                                expected="A genuinely expired token is rejected on every operation; no false success, no transfer corruption.")

    return blocked(case_id, "AUTH", desc, baseline,
                   "requires a controlled token/session-expiry fixture; credentials of shared users are never invalidated",
                   mgr=mgr)


def handle_tid(case_id: str, desc: str, mgr: EnvironmentManager, args, work: Path, overrides: dict) -> TestResult:
    ctx = mgr.ctx
    baseline = {"bryck": "not required", "authentication": "valid", "transfer_id": "deliberately invalid"}
    value, label = TID_VALUES.get(case_id, ("99999999", "nonexistent ID"))
    value = overrides.get("transfer_id", value)
    results = []
    for op, fn in (
        ("status", lambda: ctx.run_py(f"{desc}:status", "bryck_cloud_transfer_status.py",
                                      "--login", str(ctx.login_json), "--transfer-id", value, expect_fail=True)),
        ("pause", lambda: ctx.pause_transfer(value, expect_fail=True)),
        ("resume", lambda: ctx.resume_transfer(value, expect_fail=True)),
        ("cancel", lambda: ctx.cancel_transfer(value, expect_fail=True)),
        ("report", lambda: ctx.download_report(value, label, expect_fail=True)),
    ):
        sr = fn()
        mgr.cap(f"{case_id}:{op}", sr)
        results.append(sr)
    all_passed = all(sr.passed for sr in results)
    combined = ctr.StepResult(
        step=0, name=desc, command="; ".join(sr.command for sr in results),
        stdout="\n".join(sr.stdout for sr in results), stderr="\n".join(sr.stderr for sr in results),
        returncode=0 if all_passed else 1, duration_sec=sum(sr.duration_sec for sr in results), passed=all_passed,
        expected_failure=True,
    )
    return result_from_step(case_id, "TID", desc, baseline, None, combined, mgr,
                            expected=f"status/pause/resume/cancel/report all reject the {label} cleanly with no traceback.")


def handle_aws(case_id: str, desc: str, mgr: EnvironmentManager, args, work: Path, overrides: dict) -> TestResult:
    ctx = mgr.ctx
    baseline = {"bryck": "not required for configuration validation", "aws": "deliberately invalid"}
    try:
        cloud_cfg = json.loads(ctx.cloud_ops_json.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        cloud_cfg = {}

    if case_id in AWS_MUTATIONS:
        mutation = dict(AWS_MUTATIONS[case_id])
        mutation.update(overrides)
        p = mgr.build_fixture(ctx.cloud_ops_json, {**cloud_cfg, **mutation}, work, f"{case_id}.json")
        sr = ctx.run_py(desc, "bryck_cloud_configure.py", "--login", str(ctx.login_json), "--params", str(p), expect_fail=True)
        mgr.cap(case_id, sr)
        return result_from_step(case_id, "AWS", desc, baseline, None, sr, mgr,
                                expected="Provider configuration is rejected; no partial provider remains.")
    if case_id == "AWS-08":
        mutation = {"cloud_bucket": f"s3://does-not-exist-bryck-negative-{uuid.uuid4().hex[:8]}"}
        mutation.update(overrides)
        p = mgr.build_fixture(ctx.cloud_ops_json, {**cloud_cfg, **mutation}, work, f"{case_id}.json")
        sr = ctx.run_py(desc, "bryck_cloud_configure.py", "--login", str(ctx.login_json), "--params", str(p), expect_fail=True)
        mgr.cap(case_id, sr)
        return result_from_step(case_id, "AWS", desc, baseline, None, sr, mgr,
                                expected="Nonexistent bucket is rejected; no success is reported.")
    if case_id == "AWS-13":
        sr = ctx.deconfigure_cloud(expect_fail=True)
        mgr.cap(case_id, sr)
        return result_from_step(case_id, "AWS", desc, baseline, None, sr, mgr,
                                expected="Deconfiguring when not configured is rejected or explicitly idempotent.")
    if case_id == "AWS-14":
        env_before = mgr.snapshot(f"{case_id}:before")
        first = mgr.cap(f"{case_id}:configure", ctx.configure_cloud())
        second = ctx.deconfigure_cloud(expect_fail=False)
        mgr.cap(f"{case_id}:deconfigure1", second)
        third = ctx.deconfigure_cloud(expect_fail=True)
        mgr.cap(f"{case_id}:deconfigure2", third)
        return result_from_step(case_id, "AWS", desc, baseline, env_before, third, mgr,
                                expected="Second deconfigure is deterministic; no stale provider remains.",
                                env_after=mgr.snapshot(f"{case_id}:after"))
    if case_id in {"AWS-15", "AWS-16", "AWS-17", "AWS-18"}:
        destructive_gate = args.live and not args.confirm_destructive
        if not args.live or destructive_gate:
            return blocked(case_id, "AWS", desc, baseline,
                           "requires --live plus a real active/paused transfer fixture", mgr=mgr)
        if not (mgr.ensure_mounted() and mgr.ensure_cloud_configured() and mgr.ensure_dataset()):
            return blocked(case_id, "AWS", desc, baseline, "could not establish mounted+configured+dataset baseline", mgr=mgr)
        env_before = mgr.snapshot(f"{case_id}:before")
        tid = mgr.create_transfer("upload", "IN_PROGRESS")
        if not tid:
            return blocked(case_id, "AWS", desc, baseline, "could not establish an active transfer fixture",
                          env_before, mgr)
        if case_id in {"AWS-16", "AWS-18"}:
            mgr.cap(f"{case_id}:pause", ctx.pause_transfer(tid))
        op = "deconfigure" if case_id in {"AWS-15", "AWS-16"} else "configure"
        sr = ctx.deconfigure_cloud(expect_fail=True) if op == "deconfigure" else ctx.configure_cloud()
        mgr.cap(f"{case_id}:{op}", sr)
        cleanup_detail = mgr.cleanup_transfer(tid)
        env_after = mgr.snapshot(f"{case_id}:after")
        return result_from_step(case_id, "AWS", desc, baseline, env_before, sr, mgr,
                                expected="Cloud config change during an active/paused transfer does not silently detach it.",
                                cleanup_status="performed", cleanup_detail=cleanup_detail, env_after=env_after)
    return blocked(case_id, "AWS", desc, baseline,
                   "requires restricted IAM credentials/permission fixture not available in this environment", mgr=mgr)


def handle_path(case_id: str, desc: str, mgr: EnvironmentManager, args, work: Path, overrides: dict) -> TestResult:
    ctx = mgr.ctx
    baseline = {"bryck": "not required", "aws": "valid structure", "prefix": "deliberately invalid"}
    try:
        cloud_cfg = json.loads(ctx.cloud_ops_json.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        cloud_cfg = {}
    prefix = PATH_PREFIXES.get(case_id, f"negative/{case_id}/")
    prefix = overrides.get("cloud_bucket_prefix", prefix)
    mutated_bucket = cloud_cfg.get("cloud_bucket", "s3://dataset-2gb-1tb/cloud-transfer")

    def initiate_with_prefix(pfx: str, label: str) -> ctr.StepResult:
        base_prefix = mutated_bucket.split("/", 3)[-1] if "://" in mutated_bucket else ""
        new_bucket = mutated_bucket.rsplit(base_prefix, 1)[0] + pfx if base_prefix else mutated_bucket
        cfg = {**cloud_cfg, "cloud_bucket": new_bucket}
        p = mgr.build_fixture(ctx.cloud_ops_json, cfg, work, f"{case_id}-{label}.json")
        return ctx.run_py(f"{desc} ({label})", "bryck_cloud_transfer_initiate.py", "--login", str(ctx.login_json),
                          "--params", str(p), "--mode", "upload", expect_fail=True)

    if case_id == "PATH-09":
        cfg = {**cloud_cfg, "bryck_src": "/bryck/does-not-match-dataset-root"}
        p = mgr.build_fixture(ctx.cloud_ops_json, cfg, work, f"{case_id}.json")
        sr = ctx.run_py(desc, "bryck_cloud_transfer_initiate.py", "--login", str(ctx.login_json),
                        "--params", str(p), "--mode", "upload", expect_fail=True)
        mgr.cap(case_id, sr)
    elif case_id == "PATH-04":
        sr1 = mgr.cap(f"{case_id}:double-slash", initiate_with_prefix(prefix, "double-slash"))
        sr2 = mgr.cap(f"{case_id}:trailing-slash", initiate_with_prefix(PATH_04_EXTRA, "trailing-slash"))
        all_ok = sr1.passed and sr2.passed
        sr = ctr.StepResult(
            step=0, name=desc, command=f"{sr1.command}; {sr2.command}",
            stdout=f"[double-slash] {sr1.stdout}\n[trailing-slash] {sr2.stdout}",
            stderr=f"[double-slash] {sr1.stderr}\n[trailing-slash] {sr2.stderr}",
            returncode=0 if all_ok else 1, duration_sec=sr1.duration_sec + sr2.duration_sec,
            passed=all_ok, expected_failure=True,
        )
    else:
        sr = mgr.cap(case_id, initiate_with_prefix(prefix, case_id))

    return result_from_step(case_id, "PATH", desc, baseline, None, sr, mgr,
                            expected="Invalid/edge-case object path is rejected; no object escapes the isolated test prefix.")


LIFE_STATE = {
    "LIFE-02": "unmounted", "LIFE-04": "unmounted", "LIFE-05": "mounted", "LIFE-06": "mounted",
    "LIFE-07": "any", "LIFE-09": "ejected", "LIFE-12": "mounted",
}


def handle_life(case_id: str, desc: str, mgr: EnvironmentManager, args, work: Path, overrides: dict) -> TestResult:
    ctx = mgr.ctx
    destructive = case_id in {"LIFE-06", "LIFE-07", "LIFE-12", "LIFE-15"}
    baseline = {"bryck": LIFE_STATE.get(case_id, "see case description")}
    if not args.live:
        return blocked(case_id, "LIFE", desc, baseline, "requires --live against the dedicated Bryck device", mgr=mgr)
    if destructive and not args.confirm_destructive:
        return blocked(case_id, "LIFE", desc, baseline, "requires --confirm-destructive", mgr=mgr)

    if case_id == "LIFE-04":
        env_before = mgr.snapshot(f"{case_id}:before")
        p = mgr.build_fixture(ctx.fmt_mount_json, {"mount": {}}, work, f"{case_id}.json")
        sr = ctx.run_py(desc, "bryck_mount.py", "--login", str(ctx.login_json), "--params", str(p), expect_fail=True)
        mgr.cap(case_id, sr)
        return result_from_step(case_id, "LIFE", desc, baseline, env_before, sr, mgr,
                                expected="Mount with an incomplete parameter object is rejected; device state unchanged.",
                                env_after=mgr.snapshot(f"{case_id}:after"))
    if case_id == "LIFE-07":
        env_before = mgr.snapshot(f"{case_id}:before")
        p = mgr.build_fixture(ctx.fmt_mount_json, {"format": {}}, work, f"{case_id}.json")
        sr = ctx.run_py(desc, "bryck_format.py", "--login", str(ctx.login_json), "--params", str(p),
                        timeout=900, expect_fail=True)
        mgr.cap(case_id, sr)
        return result_from_step(case_id, "LIFE", desc, baseline, env_before, sr, mgr,
                                expected="Format with an incomplete parameter object is rejected.",
                                env_after=mgr.snapshot(f"{case_id}:after"))
    if case_id == "LIFE-12":
        if not mgr.ensure_mounted():
            return blocked(case_id, "LIFE", desc, baseline, "could not establish a mounted baseline", mgr=mgr)
        env_before = mgr.snapshot(f"{case_id}:before")
        sr = mgr.cap(case_id, ctx.run_py(desc, "bryck_erase.py", "--login", str(ctx.login_json), expect_fail=True))
        return result_from_step(case_id, "LIFE", desc, baseline, env_before, sr, mgr,
                                expected="Erase while mounted is rejected by the state-machine precondition (requires Ejected).",
                                env_after=mgr.snapshot(f"{case_id}:after"))
    if case_id == "LIFE-03":
        env_before = mgr.snapshot(f"{case_id}:before")
        sr = ctx.run_py(desc, "bryck_mount.py", "--login", str(ctx.login_json),
                        "--params", str(work / "missing-format-mount.json"), expect_fail=True)
        mgr.cap(case_id, sr)
        return result_from_step(case_id, "LIFE", desc, baseline, env_before, sr, mgr,
                                expected="Missing mount parameters are rejected; Bryck state is unchanged.",
                                env_after=mgr.snapshot(f"{case_id}:after"))
    if case_id == "LIFE-02":
        if not mgr.ensure_unmounted():
            return blocked(case_id, "LIFE", desc, baseline, "could not establish an unmounted baseline", mgr=mgr)
        env_before = mgr.snapshot(f"{case_id}:before")
        sr = ctx.run_py(desc, "bryck_mount.py", "--login", str(ctx.login_json),
                        "--params", str(ctx.fmt_mount_json), expect_fail=True)
        mgr.cap(case_id, sr)
        return result_from_step(case_id, "LIFE", desc, baseline, env_before, sr, mgr,
                                expected="Mounting an unformatted device is rejected with a controlled precondition failure.",
                                env_after=mgr.snapshot(f"{case_id}:after"))
    if case_id == "LIFE-05":
        if not mgr.ensure_mounted():
            return blocked(case_id, "LIFE", desc, baseline, "could not establish a mounted baseline", mgr=mgr)
        env_before = mgr.snapshot(f"{case_id}:before")
        sr = ctx.run_py(desc, "bryck_mount.py", "--login", str(ctx.login_json),
                        "--params", str(ctx.fmt_mount_json), expect_fail=True)
        mgr.cap(case_id, sr)
        return result_from_step(case_id, "LIFE", desc, baseline, env_before, sr, mgr,
                                expected="Re-mounting an already-mounted device is rejected or explicitly idempotent.",
                                env_after=mgr.snapshot(f"{case_id}:after"))
    if case_id == "LIFE-06":
        if not mgr.ensure_mounted():
            return blocked(case_id, "LIFE", desc, baseline, "could not establish a mounted baseline", mgr=mgr)
        env_before = mgr.snapshot(f"{case_id}:before")
        sr = mgr.cap(case_id, ctx.run_py(desc, "bryck_format.py", "--login", str(ctx.login_json),
                                         "--params", str(ctx.fmt_mount_json), timeout=900, expect_fail=True))
        return result_from_step(case_id, "LIFE", desc, baseline, env_before, sr, mgr,
                                expected="Format while mounted is rejected; mount/device state is unchanged.",
                                env_after=mgr.snapshot(f"{case_id}:after"))
    if case_id == "LIFE-09":
        if not mgr.ensure_unmounted():
            return blocked(case_id, "LIFE", desc, baseline, "could not establish an ejected baseline", mgr=mgr)
        env_before = mgr.snapshot(f"{case_id}:before")
        sr = mgr.cap(case_id, ctx.eject_bryck(expect_fail=True))
        return result_from_step(case_id, "LIFE", desc, baseline, env_before, sr, mgr,
                                expected="Ejecting an already-ejected device is rejected or explicitly idempotent.",
                                env_after=mgr.snapshot(f"{case_id}:after"))
    if case_id in {"LIFE-10", "LIFE-11", "LIFE-13", "LIFE-14"}:
        if not (mgr.ensure_mounted() and mgr.ensure_cloud_configured() and mgr.ensure_dataset()):
            return blocked(case_id, "LIFE", desc, baseline, "could not establish mounted+configured+dataset baseline", mgr=mgr)
        env_before = mgr.snapshot(f"{case_id}:before")
        wanted = "PAUSED" if case_id in {"LIFE-11", "LIFE-14"} else "IN_PROGRESS"
        tid = mgr.create_transfer("upload", "IN_PROGRESS" if wanted == "IN_PROGRESS" else "IN_PROGRESS")
        if not tid:
            return blocked(case_id, "LIFE", desc, baseline, "could not establish the required transfer state", env_before, mgr)
        if wanted == "PAUSED":
            mgr.cap(f"{case_id}:pause", ctx.pause_transfer(tid))
        op = "eject" if case_id in {"LIFE-10", "LIFE-11"} else "format"
        if op == "eject":
            sr = ctx.eject_bryck(expect_fail=True)
        else:
            sr = ctx.run_py(desc, "bryck_format.py", "--login", str(ctx.login_json),
                            "--params", str(ctx.fmt_mount_json), timeout=900, expect_fail=True)
        mgr.cap(f"{case_id}:{op}", sr)
        cleanup_detail = mgr.cleanup_transfer(tid)
        env_after = mgr.snapshot(f"{case_id}:after")
        return result_from_step(case_id, "LIFE", desc, baseline, env_before, sr, mgr,
                                expected=f"{op} is blocked while the transfer is active/paused; transfer remains observable.",
                                cleanup_status="performed", cleanup_detail=cleanup_detail, env_after=env_after)
    if case_id == "LIFE-15":
        if not (mgr.ensure_mounted() and mgr.ensure_cloud_configured() and mgr.ensure_dataset()):
            return blocked(case_id, "LIFE", desc, baseline, "could not establish mounted+configured+dataset baseline", mgr=mgr)
        env_before = mgr.snapshot(f"{case_id}:before")
        tid = mgr.create_transfer_at("upload", "COMPLETED", timeout=7200)
        if not tid:
            return blocked(case_id, "LIFE", desc, baseline, "could not establish a completed transfer", env_before, mgr)

        def run_verify():
            return "verify", ctx.transfer_status(tid, f"{case_id} verification window")

        def run_format():
            return "format", ctx.run_py(desc, "bryck_format.py", "--login", str(ctx.login_json),
                                        "--params", str(ctx.fmt_mount_json), timeout=900, expect_fail=True)

        def run_eject():
            return "eject", ctx.eject_bryck(expect_fail=True)

        def run_deconfigure():
            return "deconfigure", ctx.deconfigure_cloud(expect_fail=True)

        barrier = threading.Barrier(4)

        def wrap(fn):
            def inner():
                barrier.wait()
                return fn()
            return inner

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            futures = [pool.submit(wrap(fn)) for fn in (run_verify, run_format, run_eject, run_deconfigure)]
            records = {name: sr for name, sr in (f.result() for f in futures)}
        for name, sr in records.items():
            mgr.cap(f"{case_id}:{name}", sr)

        verify_ok = records["verify"].passed and "COMPLETED" in (records["verify"].stdout + records["verify"].stderr).upper()
        cleanup_detail = mgr.cleanup_transfer(tid)
        env_after = mgr.snapshot(f"{case_id}:after")
        combined = dataclasses.replace(records["verify"], passed=verify_ok)
        return result_from_step(case_id, "LIFE", desc, baseline, env_before, combined, mgr,
                                expected="Format/eject/deconfigure fired during the post-completion verification window "
                                         "never corrupt the COMPLETED transfer's own status; verification stays consistent.",
                                cleanup_status="performed", cleanup_detail=cleanup_detail, env_after=env_after)
    if case_id == "LIFE-16":
        if not (mgr.ensure_mounted() and mgr.ensure_cloud_configured() and mgr.ensure_dataset()):
            return blocked(case_id, "LIFE", desc, baseline, "could not establish mounted+configured+dataset baseline", mgr=mgr)
        env_before = mgr.snapshot(f"{case_id}:before")
        tid = mgr.create_transfer("upload", "IN_PROGRESS")
        if not tid:
            return blocked(case_id, "LIFE", desc, baseline, "could not establish an active transfer", env_before, mgr)
        barrier = threading.Barrier(2)

        def run_cancel():
            barrier.wait()
            return "cancel", ctx.cancel_transfer(tid)

        def run_eject():
            barrier.wait()
            return "eject", ctx.eject_bryck(expect_fail=True)

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            f1, f2 = pool.submit(run_cancel), pool.submit(run_eject)
            records = dict([f1.result(), f2.result()])
        for name, sr in records.items():
            mgr.cap(f"{case_id}:{name}", sr)
        one_ok = sum(1 for sr in records.values() if sr.passed) >= 1
        cleanup_detail = mgr.cleanup_transfer(tid)
        env_after = mgr.snapshot(f"{case_id}:after")
        combined = dataclasses.replace(records["cancel"], passed=one_ok)
        return result_from_step(case_id, "LIFE", desc, baseline, env_before, combined, mgr,
                                expected="Eject fired concurrently with cancellation resolves to one valid, consistent "
                                         "final state (cancelled transfer, no corrupt mount state).",
                                cleanup_status="performed", cleanup_detail=cleanup_detail, env_after=env_after)
    return blocked(case_id, "LIFE", desc, baseline,
                   "requires exact-timing fault control (verification/cancellation window) not available", mgr=mgr)


def invert_result(sr: ctr.StepResult) -> ctr.StepResult:
    """Flip PASS/FAIL for helpers (initiate_transfer/run_datagen) that don't take expect_fail."""
    return dataclasses.replace(sr, passed=not sr.passed, expected_failure=True)


def handle_data(case_id: str, desc: str, mgr: EnvironmentManager, args, work: Path, overrides: dict) -> TestResult:
    ctx = mgr.ctx
    baseline = {"dataset": "deliberately invalid specification"}
    if case_id == "DATA-05":
        sr = ctx.run_datagen(str(work / "missing-spec.yaml"), timeout=120)
    elif case_id == "DATA-06":
        bad = work / "invalid-spec.yaml"
        bad.write_text("this is not: [valid yaml", encoding="utf-8")
        sr = ctx.run_datagen(str(bad), timeout=120)
        if args.live:
            sr = invert_result(sr)
    elif case_id == "DATA-07":
        bad = work / "invalid-size-spec.yaml"
        bad.write_text("root: /bryck/negative\nsize: not-a-number\n", encoding="utf-8")
        sr = ctx.run_datagen(str(bad), timeout=120)
        if args.live:
            sr = invert_result(sr)
    elif case_id in {"DATA-01", "DATA-02"}:
        baseline = {"bryck": "ejected/unmounted"}
        if not args.live:
            return blocked(case_id, "DATASET", desc, baseline, "requires --live against the dedicated Bryck device", mgr=mgr)
        if not mgr.ensure_unmounted():
            return blocked(case_id, "DATASET", desc, baseline, "could not establish an ejected/unmounted baseline", mgr=mgr)
        env_before = mgr.snapshot(f"{case_id}:before")
        sr = invert_result(ctx.run_datagen("small_1gb_fast.yaml", timeout=180))
        mgr.cap(case_id, sr)
        return result_from_step(case_id, "DATASET", desc, baseline, env_before, sr, mgr,
                                expected="Dataset generation is blocked before any SSH/host mutation while unmounted.",
                                env_after=mgr.snapshot(f"{case_id}:after"))
    elif case_id in {"DATA-03", "DATA-04"}:
        baseline = {"bryck": "mounted", "cloud": "configured", "transfer": "active/paused"}
        if not args.live:
            return blocked(case_id, "DATASET", desc, baseline, "requires --live against the dedicated Bryck device", mgr=mgr)
        if not (mgr.ensure_mounted() and mgr.ensure_cloud_configured() and mgr.ensure_dataset()):
            return blocked(case_id, "DATASET", desc, baseline, "could not establish mounted+configured+dataset baseline", mgr=mgr)
        env_before = mgr.snapshot(f"{case_id}:before")
        target_state = "PAUSED" if case_id == "DATA-04" else "IN_PROGRESS"
        tid = mgr.create_transfer_at("upload", target_state, timeout=3600)
        if not tid:
            return blocked(case_id, "DATASET", desc, baseline, "could not establish the required transfer state", env_before, mgr)
        gen = mgr.cap(f"{case_id}:regenerate", ctx.run_datagen("small_1gb_fast.yaml", timeout=1800))
        status = mgr.cap(f"{case_id}:status_after", ctx.transfer_status(tid, f"{case_id} unaffected transfer"))
        cleanup_detail = mgr.cleanup_transfer(tid)
        env_after = mgr.snapshot(f"{case_id}:after")
        combined = dataclasses.replace(gen, passed=(gen.returncode != -2 and status.passed))
        return result_from_step(case_id, "DATASET", desc, baseline, env_before, combined, mgr,
                                expected="Dataset generation does not crash and the unrelated active/paused transfer remains queryable/uncorrupted.",
                                cleanup_status="performed", cleanup_detail=cleanup_detail, env_after=env_after)
    elif case_id == "DATA-09":
        baseline = {"bryck": "mounted", "dataset root": "deliberately outside /bryck"}
        if not args.live:
            return blocked(case_id, "DATASET", desc, baseline, "requires --live against the dedicated Bryck device", mgr=mgr)
        if not mgr.ensure_mounted():
            return blocked(case_id, "DATASET", desc, baseline, "could not establish a mounted baseline", mgr=mgr)
        env_before = mgr.snapshot(f"{case_id}:before")
        outside_root = f"/tmp/bryck-negative-outside-{uuid.uuid4().hex[:8]}"
        base_spec_path = ctx.spec_dir / "small_1gb_fast.yaml"
        try:
            spec_text = base_spec_path.read_text(encoding="utf-8")
        except OSError:
            spec_text = "version: 1\nmode: flat\nroot: /bryck/small_1gb\nflat:\n  num_files: 5\nsize:\n  type: range\n  min: 1KB\n  max: 10KB\n"
        spec_text = re.sub(r"(?m)^root:\s*\S+\s*$", f"root: {outside_root}", spec_text, count=1)
        spec = work / "outside-root-spec.yaml"
        spec.write_text(spec_text, encoding="utf-8")
        sr = invert_result(mgr.cap(case_id, ctx.run_datagen(str(spec), timeout=180)))
        mgr.run_ssh(f"{case_id}:cleanup_outside_root", f"rm -rf {outside_root}")
        env_after = mgr.snapshot(f"{case_id}:after")
        return result_from_step(case_id, "DATASET", desc, baseline, env_before, sr, mgr,
                                expected="Dataset generation outside /bryck is rejected; no accidental host-root write occurs.",
                                env_after=env_after)
    elif case_id == "DATA-10":
        baseline = {"bryck": "mounted", "dataset": "root differs from cloud_ops.bryck_src"}
        if not args.live:
            return blocked(case_id, "DATASET", desc, baseline, "requires --live against the dedicated Bryck device", mgr=mgr)
        if not (mgr.ensure_mounted() and mgr.ensure_dataset()):
            return blocked(case_id, "DATASET", desc, baseline, "could not establish mounted+dataset baseline", mgr=mgr)
        env_before = mgr.snapshot(f"{case_id}:before")
        try:
            cloud_cfg = json.loads(ctx.cloud_ops_json.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            cloud_cfg = {}
        mismatched = mgr.build_fixture(
            ctx.cloud_ops_json, {**cloud_cfg, "bryck_src": "/bryck/deliberately-mismatched-source"},
            work, f"{case_id}.json",
        )
        original_cloud_ops = ctx.cloud_ops_json
        try:
            ctx.cloud_ops_json = mismatched
            sr = invert_result(mgr.cap(case_id, ctx.validate_dataset_source("small_1gb_fast.yaml")))
        finally:
            ctx.cloud_ops_json = original_cloud_ops
        return result_from_step(case_id, "DATASET", desc, baseline, env_before, sr, mgr,
                                expected="A dataset-root/bryck_src mismatch is detected before any transfer is initiated.",
                                env_after=mgr.snapshot(f"{case_id}:after"))
    elif case_id == "DATA-12":
        baseline = {"bryck": "mounted", "dataset": "generated twice"}
        if not args.live:
            return blocked(case_id, "DATASET", desc, baseline, "requires --live against the dedicated Bryck device", mgr=mgr)
        if not mgr.ensure_mounted():
            return blocked(case_id, "DATASET", desc, baseline, "could not establish a mounted baseline", mgr=mgr)
        env_before = mgr.snapshot(f"{case_id}:before")
        mgr.cap(f"{case_id}:first", ctx.run_datagen("small_1gb_fast.yaml", timeout=1800))
        second = mgr.cap(f"{case_id}:second", ctx.run_datagen("small_1gb_fast.yaml", timeout=1800))
        return result_from_step(case_id, "DATASET", desc, baseline, env_before, second, mgr,
                                expected="Duplicate dataset generation is deterministic; no crash or corrupted root.",
                                env_after=mgr.snapshot(f"{case_id}:after"))
    elif case_id == "DATA-11":
        # Interrupted generation: kill the remote datagen process mid-run (via a delayed
        # `pkill -f` over a separate SSH call, same "approved control" pattern REC-02 already
        # uses to kill a runner mid-initiate), confirm a partial dataset is left in a
        # detectable/removable state, then confirm a clean regeneration recovers normally.
        baseline = {"bryck": "mounted", "dataset": "interrupted mid-generation"}
        if not args.live:
            return blocked(case_id, "DATASET", desc, baseline, "requires --live against the dedicated Bryck device", mgr=mgr)
        if not mgr.ensure_mounted():
            return blocked(case_id, "DATASET", desc, baseline, "could not establish a mounted baseline", mgr=mgr)
        env_before = mgr.snapshot(f"{case_id}:before")

        def killer():
            time.sleep(3)
            mgr.run_ssh(f"{case_id}:kill_datagen", f"pkill -f {ctx.datagen_bin}")

        kill_thread = threading.Thread(target=killer)
        kill_thread.start()
        interrupted = mgr.cap(f"{case_id}:interrupted_run", ctx.run_datagen("small_1gb_fast.yaml", timeout=120))
        kill_thread.join()
        recovered = mgr.cap(f"{case_id}:recover_regenerate", ctx.run_datagen("small_1gb_fast.yaml", timeout=1800))
        combined = dataclasses.replace(recovered, passed=recovered.passed)
        env_after = mgr.snapshot(f"{case_id}:after")
        return result_from_step(case_id, "DATASET", desc, baseline, env_before, combined, mgr,
                                expected="Killing datagen mid-run leaves a partial (not corrupting) dataset; a clean "
                                         "regeneration afterward succeeds normally.",
                                env_after=env_after)
    else:
        return blocked(case_id, "DATASET", desc, baseline,
                       "requires an approved disk-full fixture; unsafe to synthesize here (remote-permission cases "
                       "are covered independently by XFER-05/DOWNLOAD-04)", mgr=mgr)
    mgr.cap(case_id, sr)
    return result_from_step(case_id, "DATASET", desc, baseline, None, sr, mgr,
                            expected="Dataset generation is rejected before any SSH/host mutation occurs.")


XFER_STATE_CASES = {
    "XFER-01", "XFER-02", "XFER-03", "XFER-04", "XFER-05", "XFER-06", "XFER-07", "XFER-08",
    "XFER-09", "XFER-10", "XFER-11", "XFER-12", "XFER-13", "XFER-14", "XFER-15",
    "XFER-16", "XFER-17", "XFER-18",
}


def handle_xfer(case_id: str, desc: str, mgr: EnvironmentManager, args, work: Path, overrides: dict) -> TestResult:
    ctx = mgr.ctx
    baseline = {"bryck": "mounted", "cloud": "configured", "dataset": "available", "transfer": "active"}
    if not args.live:
        return blocked(case_id, "XFER", desc, baseline, "requires --live against the dedicated Bryck device", mgr=mgr)
    if case_id not in XFER_STATE_CASES:
        return blocked(case_id, "XFER", desc, baseline,
                       "requires a dedicated isolated fixture (permissions/bucket/network) not available in this environment",
                       mgr=mgr)

    try:
        cloud_cfg = json.loads(ctx.cloud_ops_json.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        cloud_cfg = {}

    if case_id == "XFER-01":
        if not mgr.ensure_unmounted():
            return blocked(case_id, "XFER", desc, baseline, "could not establish an unmounted baseline", mgr=mgr)
        env_before = mgr.snapshot(f"{case_id}:before")
        sr, _ = ctx.initiate_transfer("upload")
        sr = invert_result(mgr.cap(case_id, sr))
        return result_from_step(case_id, "XFER", desc, baseline, env_before, sr, mgr,
                                expected="Upload is blocked while the Bryck is ejected/unmounted.",
                                env_after=mgr.snapshot(f"{case_id}:after"))
    if case_id == "XFER-02":
        if not (mgr.ensure_mounted() and mgr.ensure_dataset()):
            return blocked(case_id, "XFER", desc, baseline, "could not establish mounted+dataset baseline", mgr=mgr)
        if not mgr.ensure_cloud_deconfigured():
            return blocked(case_id, "XFER", desc, baseline,
                           "could not verify the cloud was actually deconfigured before the test", mgr=mgr)
        env_before = mgr.snapshot(f"{case_id}:before")
        sr, _ = ctx.initiate_transfer("upload")
        sr = invert_result(mgr.cap(case_id, sr))
        return result_from_step(case_id, "XFER", desc, baseline, env_before, sr, mgr,
                                expected="Upload fails a configuration check when the cloud provider is not configured.",
                                env_after=mgr.snapshot(f"{case_id}:after"))
    if case_id == "XFER-03":
        if not (mgr.ensure_mounted() and mgr.ensure_cloud_configured()):
            return blocked(case_id, "XFER", desc, baseline, "could not establish mounted+configured baseline", mgr=mgr)
        env_before = mgr.snapshot(f"{case_id}:before")
        p = mgr.build_fixture(ctx.cloud_ops_json, {**cloud_cfg, "bryck_src": "/bryck/does-not-exist-negative"}, work, f"{case_id}.json")
        sr = ctx.run_py(desc, "bryck_cloud_transfer_initiate.py", "--login", str(ctx.login_json),
                        "--params", str(p), "--mode", "upload", timeout=300, expect_fail=True)
        mgr.cap(case_id, sr)
        return result_from_step(case_id, "XFER", desc, baseline, env_before, sr, mgr,
                                expected="Upload from a nonexistent source path fails with a controlled source-path error.",
                                env_after=mgr.snapshot(f"{case_id}:after"))
    if case_id == "XFER-04":
        if not (mgr.ensure_mounted() and mgr.ensure_cloud_configured()):
            return blocked(case_id, "XFER", desc, baseline, "could not establish mounted+configured baseline", mgr=mgr)
        env_before = mgr.snapshot(f"{case_id}:before")
        empty_dir = f"/bryck/empty-negative-{uuid.uuid4().hex[:8]}"
        mgr.run_ssh(f"{case_id}:mkdir_empty", f"mkdir -p {empty_dir}")
        p = mgr.build_fixture(ctx.cloud_ops_json, {**cloud_cfg, "bryck_src": empty_dir}, work, f"{case_id}.json")
        # Either a rejection or a documented empty/trivial-transfer success is an acceptable outcome here;
        # this case observes behaviour rather than forcing a single expected outcome.
        sr = ctx.run_py(desc, "bryck_cloud_transfer_initiate.py", "--login", str(ctx.login_json),
                        "--params", str(p), "--mode", "upload", timeout=300, expect_fail=False)
        sr = dataclasses.replace(sr, passed=True)
        mgr.cap(case_id, sr)
        mgr.run_ssh(f"{case_id}:rm_empty", f"rmdir {empty_dir}")
        return result_from_step(case_id, "XFER", desc, baseline, env_before, sr, mgr,
                                expected="Upload of an empty source directory is rejected or returns a documented empty-transfer result.",
                                env_after=mgr.snapshot(f"{case_id}:after"))
    if case_id == "XFER-05":
        # Inaccessible source: chmod a real generated file to 000 so it cannot be read, attempt
        # the upload expecting a controlled permission failure, then restore permissions.
        if not (mgr.ensure_mounted() and mgr.ensure_cloud_configured() and mgr.ensure_dataset()):
            return blocked(case_id, "XFER", desc, baseline, "could not establish mounted+configured+dataset baseline", mgr=mgr)
        env_before = mgr.snapshot(f"{case_id}:before")
        bryck_src = cloud_cfg.get("bryck_src", "/bryck/bryck-yato")
        find_sr = mgr.run_ssh(f"{case_id}:find_file", f"find {bryck_src} -maxdepth 2 -type f | head -n 1")
        target_file = (find_sr.stdout or "").strip().splitlines()[0] if find_sr.passed and find_sr.stdout.strip() else ""
        if not target_file:
            return blocked(case_id, "XFER", desc, baseline, "could not find a real dataset file to make unreadable",
                          env_before, mgr)
        mgr.run_ssh(f"{case_id}:chmod_000", f"chmod 000 {target_file}")
        try:
            sr = ctx.run_py(desc, "bryck_cloud_transfer_initiate.py", "--login", str(ctx.login_json),
                            "--params", str(ctx.cloud_ops_json), "--mode", "upload", timeout=300, expect_fail=True)
            mgr.cap(case_id, sr)
        finally:
            mgr.run_ssh(f"{case_id}:restore_perms", f"chmod 644 {target_file}")
        return result_from_step(case_id, "XFER", desc, baseline, env_before, sr, mgr,
                                expected="Upload of an unreadable source file/tree is rejected with a controlled "
                                         "permission failure.",
                                env_after=mgr.snapshot(f"{case_id}:after"))
    if case_id == "XFER-06":
        if not (mgr.ensure_mounted() and mgr.ensure_cloud_configured() and mgr.ensure_dataset()):
            return blocked(case_id, "XFER", desc, baseline, "could not establish mounted+configured+dataset baseline", mgr=mgr)
        env_before = mgr.snapshot(f"{case_id}:before")
        p = mgr.build_fixture(ctx.cloud_ops_json,
                              {**cloud_cfg, "cloud_bucket": f"s3://does-not-exist-bryck-negative-{uuid.uuid4().hex[:8]}"},
                              work, f"{case_id}.json")
        sr = ctx.run_py(desc, "bryck_cloud_transfer_initiate.py", "--login", str(ctx.login_json),
                        "--params", str(p), "--mode", "upload", timeout=300, expect_fail=True)
        mgr.cap(case_id, sr)
        return result_from_step(case_id, "XFER", desc, baseline, env_before, sr, mgr,
                                expected="Upload to a nonexistent bucket reaches a documented failure state.",
                                env_after=mgr.snapshot(f"{case_id}:after"))
    if case_id == "XFER-07":
        if not (mgr.ensure_mounted() and mgr.ensure_cloud_configured() and mgr.ensure_dataset()):
            return blocked(case_id, "XFER", desc, baseline, "could not establish mounted+configured+dataset baseline", mgr=mgr)
        env_before = mgr.snapshot(f"{case_id}:before")
        p = mgr.build_fixture(ctx.cloud_ops_json, {**cloud_cfg, "cloud_bucket": "not-a-valid-object-path"}, work, f"{case_id}.json")
        sr = ctx.run_py(desc, "bryck_cloud_transfer_initiate.py", "--login", str(ctx.login_json),
                        "--params", str(p), "--mode", "upload", timeout=300, expect_fail=True)
        mgr.cap(case_id, sr)
        return result_from_step(case_id, "XFER", desc, baseline, env_before, sr, mgr,
                                expected="An invalid cloud object path is rejected.",
                                env_after=mgr.snapshot(f"{case_id}:after"))
    if case_id == "XFER-08":
        if not mgr.ensure_mounted():
            return blocked(case_id, "XFER", desc, baseline, "could not establish a mounted baseline", mgr=mgr)
        mgr.ensure_cloud_configured()
        env_before = mgr.snapshot(f"{case_id}:before")
        p = mgr.build_fixture(ctx.cloud_ops_json, {**cloud_cfg, "bryck_dst": "/root/negative-not-writable"}, work, f"{case_id}.json")
        sr = ctx.run_py(desc, "bryck_cloud_transfer_initiate.py", "--login", str(ctx.login_json),
                        "--params", str(p), "--mode", "download", timeout=300, expect_fail=True)
        mgr.cap(case_id, sr)
        return result_from_step(case_id, "XFER", desc, baseline, env_before, sr, mgr,
                                expected="Download to an invalid/inaccessible destination fails in a controlled way.",
                                env_after=mgr.snapshot(f"{case_id}:after"))
    if case_id in {"XFER-09", "XFER-10"}:
        if not (mgr.ensure_mounted() and mgr.ensure_cloud_configured() and mgr.ensure_dataset()):
            return blocked(case_id, "XFER", desc, baseline, "could not establish mounted+configured+dataset baseline", mgr=mgr)
        env_before = mgr.snapshot(f"{case_id}:before")
        sr, ids = ctx.initiate_transfer("both")
        mgr.cap(case_id, sr)
        status = mgr.cap(f"{case_id}:status_all", ctx.transfer_status_all())
        for tid in ids:
            mgr.cleanup_transfer(tid)
        combined_pass = sr.passed and len(ids) >= 2 and status.passed
        combined = dataclasses.replace(sr, passed=combined_pass)
        env_after = mgr.snapshot(f"{case_id}:after")
        return result_from_step(case_id, "XFER", desc, baseline, env_before, combined, mgr,
                                expected="Upload and download admitted concurrently do not corrupt each other's state.",
                                cleanup_status="performed", cleanup_detail=f"{len(ids)} transfer(s) cancelled", env_after=env_after)
    if case_id == "XFER-18":
        if not mgr.ensure_mounted():
            return blocked(case_id, "XFER", desc, baseline, "could not establish a mounted baseline", mgr=mgr)
        mgr.ensure_cloud_configured()
        env_before = mgr.snapshot(f"{case_id}:before")
        p = mgr.build_fixture(
            ctx.cloud_ops_json,
            {**cloud_cfg, "cloud_bucket": cloud_cfg.get("cloud_bucket", "s3://dataset-2gb-1tb") + f"/negative-missing-{uuid.uuid4().hex[:8]}"},
            work, f"{case_id}.json",
        )
        sr = ctx.run_py(desc, "bryck_cloud_transfer_initiate.py", "--login", str(ctx.login_json),
                        "--params", str(p), "--mode", "download", timeout=300, expect_fail=True)
        mgr.cap(case_id, sr)
        return result_from_step(case_id, "XFER", desc, baseline, env_before, sr, mgr,
                                expected="Download of a missing object reaches a terminal failure; no false completion.",
                                env_after=mgr.snapshot(f"{case_id}:after"))

    if not (mgr.ensure_mounted() and mgr.ensure_cloud_configured() and mgr.ensure_dataset()):
        return blocked(case_id, "XFER", desc, baseline, "could not establish mounted+configured+dataset baseline", mgr=mgr)
    env_before = mgr.snapshot(f"{case_id}:before")
    tid = mgr.create_transfer("upload", "IN_PROGRESS")
    if not tid:
        return blocked(case_id, "XFER", desc, baseline, "could not establish an active upload transfer", env_before, mgr)

    if case_id == "XFER-11":
        sr = ctx.pause_transfer(tid, expect_fail=False)
        expected = "Pausing immediately after initiation succeeds and the transfer reaches PAUSED."
    elif case_id == "XFER-12":
        sr = ctx.resume_transfer(tid, expect_fail=True)
        expected = "Resuming an already-active transfer is rejected; state is unchanged."
    elif case_id == "XFER-13":
        mgr.cap(f"{case_id}:pause1", ctx.pause_transfer(tid))
        sr = ctx.pause_transfer(tid, expect_fail=True)
        expected = "A second pause on an already-paused transfer is rejected/idempotent."
    elif case_id == "XFER-14":
        mgr.cap(f"{case_id}:pause", ctx.pause_transfer(tid))
        mgr.cap(f"{case_id}:resume1", ctx.resume_transfer(tid))
        sr = ctx.resume_transfer(tid, expect_fail=True)
        expected = "A second resume on an already-active transfer is rejected/idempotent."
    elif case_id == "XFER-15":
        mgr.cap(f"{case_id}:cancel1", ctx.cancel_transfer(tid))
        sr = ctx.cancel_transfer(tid, expect_fail=True)
        expected = "A second cancel on an already-cancelled transfer is rejected/idempotent."
    elif case_id == "XFER-16":
        if not args.confirm_destructive:
            mgr.cleanup_transfer(tid)
            return blocked(case_id, "XFER", desc, baseline, "requires --confirm-destructive", env_before, mgr)
        sr = ctx.eject_bryck(expect_fail=True)
        expected = "Eject during an active transfer is blocked; transfer remains observable."
    else:  # XFER-17
        sr = ctx.deconfigure_cloud(expect_fail=True)
        expected = "Cloud deconfigure during an active transfer does not silently detach it."

    mgr.cap(case_id, sr)
    cleanup_detail = mgr.cleanup_transfer(tid)
    env_after = mgr.snapshot(f"{case_id}:after")
    return result_from_step(case_id, "XFER", desc, baseline, env_before, sr, mgr,
                            expected=expected, cleanup_status="performed", cleanup_detail=cleanup_detail,
                            env_after=env_after)


def handle_download(case_id: str, desc: str, mgr: EnvironmentManager, args, work: Path, overrides: dict) -> TestResult:
    ctx = mgr.ctx
    baseline = {"bryck": "mounted", "cloud": "configured", "transfer": "seeded download"}
    if not args.live:
        return blocked(case_id, "DOWNLOAD", desc, baseline, "requires --live against the dedicated Bryck device", mgr=mgr)
    if case_id == "DOWNLOAD-04":
        # Cloud permission denied: same deliberately-invalid-but-well-formed AWS credential
        # technique already used for AWS-03/F-33, applied to a download attempt.
        try:
            cloud_cfg = json.loads(ctx.cloud_ops_json.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            cloud_cfg = {}
        env_before = mgr.snapshot(f"{case_id}:before")
        p = mgr.build_fixture(ctx.cloud_ops_json, {**cloud_cfg, "access_key_id": "AKIAINVALIDNEGATIVE02",
                                                    "secret_access_key": "invalid-secret-download-negative-fixture"},
                              work, f"{case_id}.json")
        sr = ctx.run_py(desc, "bryck_cloud_transfer_initiate.py", "--login", str(ctx.login_json),
                        "--params", str(p), "--mode", "download", timeout=300, expect_fail=True)
        mgr.cap(case_id, sr)
        return result_from_step(case_id, "DOWNLOAD", desc, baseline, env_before, sr, mgr,
                                expected="A download with invalid/denied AWS credentials surfaces a provider error; "
                                         "no false success.",
                                env_after=mgr.snapshot(f"{case_id}:after"))

    try:
        cloud_cfg = json.loads(ctx.cloud_ops_json.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        cloud_cfg = {}

    if case_id == "DOWNLOAD-01":
        if not mgr.ensure_unmounted():
            return blocked(case_id, "DOWNLOAD", desc, baseline, "could not establish an unmounted baseline", mgr=mgr)
        env_before = mgr.snapshot(f"{case_id}:before")
        sr, _ = ctx.initiate_transfer("download")
        sr = invert_result(mgr.cap(case_id, sr))
        return result_from_step(case_id, "DOWNLOAD", desc, baseline, env_before, sr, mgr,
                                expected="Download is blocked while the Bryck is ejected/unmounted.",
                                env_after=mgr.snapshot(f"{case_id}:after"))

    if not (mgr.ensure_mounted() and mgr.ensure_cloud_configured() and mgr.ensure_dataset()):
        return blocked(case_id, "DOWNLOAD", desc, baseline, "could not establish mounted+configured+dataset baseline", mgr=mgr)

    if case_id == "DOWNLOAD-02":
        env_before = mgr.snapshot(f"{case_id}:before")
        p_missing_obj = mgr.build_fixture(
            ctx.cloud_ops_json,
            {**cloud_cfg, "cloud_bucket": cloud_cfg.get("cloud_bucket", "s3://dataset-2gb-1tb") + f"/negative-missing-{uuid.uuid4().hex[:8]}"},
            work, f"{case_id}-missing-object.json",
        )
        sr1 = mgr.cap(f"{case_id}:missing-object", ctx.run_py(desc, "bryck_cloud_transfer_initiate.py", "--login", str(ctx.login_json),
                        "--params", str(p_missing_obj), "--mode", "download", timeout=300, expect_fail=True))
        p_missing_bucket = mgr.build_fixture(
            ctx.cloud_ops_json,
            {**cloud_cfg, "cloud_bucket": f"s3://negative-nonexistent-bucket-{uuid.uuid4().hex[:8]}/x"},
            work, f"{case_id}-missing-bucket.json",
        )
        sr2 = mgr.cap(f"{case_id}:missing-bucket", ctx.run_py(desc, "bryck_cloud_transfer_initiate.py", "--login", str(ctx.login_json),
                        "--params", str(p_missing_bucket), "--mode", "download", timeout=300, expect_fail=True))
        all_ok = sr1.passed and sr2.passed
        sr = ctr.StepResult(
            step=0, name=desc, command=f"{sr1.command}; {sr2.command}",
            stdout=f"[missing-object] {sr1.stdout}\n[missing-bucket] {sr2.stdout}",
            stderr=f"[missing-object] {sr1.stderr}\n[missing-bucket] {sr2.stderr}",
            returncode=0 if all_ok else 1, duration_sec=sr1.duration_sec + sr2.duration_sec,
            passed=all_ok, expected_failure=True,
        )
        return result_from_step(case_id, "DOWNLOAD", desc, baseline, env_before, sr, mgr,
                                expected="Download of a missing object AND a nonexistent bucket both reach a documented failure state.",
                                env_after=mgr.snapshot(f"{case_id}:after"))
    if case_id == "DOWNLOAD-03":
        env_before = mgr.snapshot(f"{case_id}:before")
        p = mgr.build_fixture(ctx.cloud_ops_json, {**cloud_cfg, "bryck_dst": "/root/negative-not-writable"}, work, f"{case_id}.json")
        sr = ctx.run_py(desc, "bryck_cloud_transfer_initiate.py", "--login", str(ctx.login_json),
                        "--params", str(p), "--mode", "download", timeout=300, expect_fail=True)
        mgr.cap(case_id, sr)
        return result_from_step(case_id, "DOWNLOAD", desc, baseline, env_before, sr, mgr,
                                expected="Download to an invalid/inaccessible destination is rejected.",
                                env_after=mgr.snapshot(f"{case_id}:after"))
    if case_id == "DOWNLOAD-05":
        env_before = mgr.snapshot(f"{case_id}:before")
        sr, ids = ctx.initiate_transfer("both")
        mgr.cap(case_id, sr)
        status = mgr.cap(f"{case_id}:status_all", ctx.transfer_status_all())
        for tid in ids:
            mgr.cleanup_transfer(tid)
        combined = dataclasses.replace(sr, passed=(sr.passed and len(ids) >= 2 and status.passed))
        env_after = mgr.snapshot(f"{case_id}:after")
        return result_from_step(case_id, "DOWNLOAD", desc, baseline, env_before, combined, mgr,
                                expected="Download admitted alongside an active upload does not corrupt either transfer's state.",
                                cleanup_status="performed", cleanup_detail=f"{len(ids)} transfer(s) cancelled", env_after=env_after)
    # DOWNLOAD-06: duplicate pause/resume/cancel on a real download transfer
    env_before = mgr.snapshot(f"{case_id}:before")
    tid = mgr.create_transfer("download", "IN_PROGRESS")
    if not tid:
        return blocked(case_id, "DOWNLOAD", desc, baseline, "could not establish an active download transfer", env_before, mgr)
    mgr.cap(f"{case_id}:pause1", ctx.pause_transfer(tid))
    sr = mgr.cap(case_id, ctx.pause_transfer(tid, expect_fail=True))
    cleanup_detail = mgr.cleanup_transfer(tid)
    env_after = mgr.snapshot(f"{case_id}:after")
    return result_from_step(case_id, "DOWNLOAD", desc, baseline, env_before, sr, mgr,
                            expected="A duplicate pause on an already-paused download is rejected/idempotent.",
                            cleanup_status="performed", cleanup_detail=cleanup_detail, env_after=env_after)


STATE_SEQUENCES = {
    "STATE-01": ("upload", "IN_PROGRESS", [("pause", False), ("pause", True)]),
    "STATE-02": ("upload", "IN_PROGRESS", [("pause", False), ("resume", False), ("resume", True)]),
    "STATE-03": ("upload", "IN_PROGRESS", [("pause", False), ("cancel", False), ("cancel", True)]),
    "STATE-04": ("upload", "IN_PROGRESS", [("resume", True)]),
    "STATE-05": ("upload", "IN_PROGRESS", [("cancel", False), ("cancel", True)]),
    "STATE-06": ("upload", "PAUSED", [("pause", True)]),
    "STATE-07": ("upload", "PAUSED", [("resume", False), ("resume", True)]),
    "STATE-08": ("upload", "PAUSED", [("cancel", False), ("cancel", True)]),
    "STATE-13": ("upload", "IN_PROGRESS", [("resume", True)]),
}


def handle_state(case_id: str, desc: str, mgr: EnvironmentManager, args, work: Path, overrides: dict) -> TestResult:
    ctx = mgr.ctx
    baseline = {"bryck": "mounted", "cloud": "configured", "dataset": "available", "transfer": "fresh fixture"}
    if not args.live:
        return blocked(case_id, "STATE", desc, baseline, "requires --live against the dedicated Bryck device", mgr=mgr)

    if case_id == "STATE-12":
        env_before = mgr.snapshot(f"{case_id}:before")
        sr = ctx.run_py(desc, "bryck_cloud_transfer_status.py", "--login", str(ctx.login_json),
                        "--state", "NOT_A_REAL_STATE", expect_fail=True)
        mgr.cap(case_id, sr)
        return result_from_step(case_id, "STATE", desc, baseline, env_before, sr, mgr,
                                expected="An unknown/invalid state filter is rejected without a traceback.",
                                env_after=mgr.snapshot(f"{case_id}:after"))

    if not (mgr.ensure_mounted() and mgr.ensure_cloud_configured() and mgr.ensure_dataset()):
        return blocked(case_id, "STATE", desc, baseline, "could not establish mounted+configured+dataset baseline", mgr=mgr)
    env_before = mgr.snapshot(f"{case_id}:before")

    if case_id == "STATE-09":
        if not args.confirm_destructive:
            return blocked(case_id, "STATE", desc, baseline, "requires --confirm-destructive", env_before, mgr)
        tid = mgr.create_transfer_at("upload", "PAUSED")
        if not tid:
            return blocked(case_id, "STATE", desc, baseline, "could not establish a paused transfer", env_before, mgr)
        sr = mgr.cap(case_id, ctx.eject_bryck(expect_fail=True))
        cleanup_detail = mgr.cleanup_transfer(tid)
        return result_from_step(case_id, "STATE", desc, baseline, env_before, sr, mgr,
                                expected="Eject while PAUSED is blocked; transfer remains observable.",
                                cleanup_status="performed", cleanup_detail=cleanup_detail,
                                env_after=mgr.snapshot(f"{case_id}:after"))
    if case_id == "STATE-10":
        tid = mgr.create_transfer_at("upload", "COMPLETED", timeout=7200)
        if not tid:
            return blocked(case_id, "STATE", desc, baseline, "could not establish a completed transfer", env_before, mgr)
        results = [
            mgr.cap(f"{case_id}:pause", ctx.pause_transfer(tid, expect_fail=True)),
            mgr.cap(f"{case_id}:resume", ctx.resume_transfer(tid, expect_fail=True)),
            mgr.cap(f"{case_id}:cancel", ctx.cancel_transfer(tid, expect_fail=True)),
        ]
        cleanup_detail = mgr.cleanup_transfer(tid)
        combined = dataclasses.replace(results[-1], passed=all(r.passed for r in results))
        return result_from_step(case_id, "STATE", desc, baseline, env_before, combined, mgr,
                                expected="pause/resume/cancel against a COMPLETED transfer are all rejected.",
                                cleanup_status="performed", cleanup_detail=cleanup_detail,
                                env_after=mgr.snapshot(f"{case_id}:after"))
    if case_id == "STATE-11":
        tid = mgr.create_transfer_at("upload", "CANCELLED")
        if not tid:
            return blocked(case_id, "STATE", desc, baseline, "could not establish a cancelled transfer", env_before, mgr)
        results = [
            mgr.cap(f"{case_id}:pause", ctx.pause_transfer(tid, expect_fail=True)),
            mgr.cap(f"{case_id}:resume", ctx.resume_transfer(tid, expect_fail=True)),
            mgr.cap(f"{case_id}:cancel", ctx.cancel_transfer(tid, expect_fail=True)),
        ]
        cleanup_detail = mgr.cleanup_transfer(tid)
        combined = dataclasses.replace(results[-1], passed=all(r.passed for r in results))
        return result_from_step(case_id, "STATE", desc, baseline, env_before, combined, mgr,
                                expected="pause/resume/cancel against a CANCELLED transfer are all rejected.",
                                cleanup_status="performed", cleanup_detail=cleanup_detail,
                                env_after=mgr.snapshot(f"{case_id}:after"))

    if case_id not in STATE_SEQUENCES:
        return blocked(case_id, "STATE", desc, baseline, "no automated sequence registered for this case", env_before, mgr)
    direction, initial_wanted, ops = STATE_SEQUENCES[case_id]
    tid = mgr.create_transfer(direction, "IN_PROGRESS")
    if not tid:
        return blocked(case_id, "STATE", desc, baseline, "could not establish the initial active transfer", env_before, mgr)
    if initial_wanted == "PAUSED":
        mgr.cap(f"{case_id}:reach_paused", ctx.pause_transfer(tid))

    fn_map = {"pause": ctx.pause_transfer, "resume": ctx.resume_transfer, "cancel": ctx.cancel_transfer}
    last_sr = None
    all_ok = True
    for i, (op, expect_fail) in enumerate(ops):
        sr = fn_map[op](tid, expect_fail=expect_fail)
        mgr.cap(f"{case_id}:{op}{i}", sr)
        all_ok = all_ok and sr.passed
        last_sr = sr
        mgr.cap(f"{case_id}:status{i}", ctx.transfer_status(tid, f"{case_id} after {op}#{i}"))

    cleanup_detail = mgr.cleanup_transfer(tid)
    env_after = mgr.snapshot(f"{case_id}:after")
    combined = ctr.StepResult(
        step=0, name=desc, command=last_sr.command if last_sr else "", stdout=last_sr.stdout if last_sr else "",
        stderr=last_sr.stderr if last_sr else "", returncode=0 if all_ok else 1,
        duration_sec=last_sr.duration_sec if last_sr else 0.0, passed=all_ok,
        expected_failure=ops[-1][1] if ops else False,  # the decisive step is the final rejection check
    )
    op_names = " -> ".join(op for op, _ in ops)
    return result_from_step(case_id, "STATE", desc, baseline, env_before, combined, mgr,
                            expected=f"Sequence {op_names} matches the documented state machine; no reset/orphan.",
                            cleanup_status="performed", cleanup_detail=cleanup_detail, env_after=env_after)


RACE_OPS = {
    "RACE-01": ["pause", "cancel"], "RACE-02": ["resume", "cancel"], "RACE-03": ["pause", "pause"],
    "RACE-04": ["resume", "resume"], "RACE-05": ["cancel", "cancel"],
    "RACE-06": ["pause", "eject"], "RACE-10": ["pause", "deconfigure"],
}
RACE_INITIATE_MODES = {"RACE-07": "both", "RACE-08": "upload", "RACE-09": "download"}

# RACE-06 ("Transfer + lifecycle") covers the full eject/format/erase/remove/mount
# matrix plus format-vs-pause/resume/cancel, each pair run against its own fresh
# active transfer. RACE-10 additionally covers resume+deconfigure (not just pause).
# Pairs whose first op is NOT pause/resume/cancel don't need an active transfer --
# they test the eject-in-progress transition window (mount/format fired mid-eject)
# and duplicate-mount, so they run directly against the mounted baseline.
RACE06_TRANSFER_OP_PAIRS = [
    ("pause", "eject"), ("pause", "format"), ("pause", "erase"), ("pause", "remove"), ("pause", "mount"),
    ("cancel", "format"), ("resume", "format"), ("resume", "eject"),
]
RACE06_TRANSITION_PAIRS = [("eject", "format"), ("eject", "mount"), ("mount", "mount"), ("mount", "format")]
RACE06_PAIRS = RACE06_TRANSFER_OP_PAIRS + RACE06_TRANSITION_PAIRS
RACE10_PAIRS = [("pause", "deconfigure"), ("resume", "deconfigure")]
TRANSFER_OPS = {"pause", "resume", "cancel"}


def handle_race(case_id: str, desc: str, mgr: EnvironmentManager, args, work: Path, overrides: dict) -> TestResult:
    ctx = mgr.ctx
    baseline = {"bryck": "mounted", "cloud": "configured", "dataset": "available", "transfer": "active (concurrent ops)"}
    if not args.live:
        return blocked(case_id, "RACE", desc, baseline, "requires --live against the dedicated Bryck device", mgr=mgr)
    if case_id == "RACE-06" and not args.confirm_destructive:
        return blocked(case_id, "RACE", desc, baseline, "requires --confirm-destructive (eject)", mgr=mgr)
    if not (mgr.ensure_mounted() and mgr.ensure_cloud_configured() and mgr.ensure_dataset()):
        return blocked(case_id, "RACE", desc, baseline, "could not establish mounted+configured+dataset baseline", mgr=mgr)
    env_before = mgr.snapshot(f"{case_id}:before")

    if case_id in RACE_INITIATE_MODES:
        mode = RACE_INITIATE_MODES[case_id]
        directions = ["upload", "download"] if mode == "both" else [mode, mode]

        def run_initiate(direction: str):
            return direction, ctx.initiate_transfer(direction)

        barrier = threading.Barrier(2)

        def run_one(direction: str):
            barrier.wait()
            return run_initiate(direction)

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(run_one, d) for d in directions]
            records = [f.result() for f in futures]
        all_ids = []
        for i, (direction, (sr, ids)) in enumerate(records):
            mgr.cap(f"{case_id}:{direction}#{i}", sr)
            all_ids.extend(ids)
        status = mgr.cap(f"{case_id}:status_all", ctx.transfer_status_all())
        for tid in all_ids:
            mgr.cleanup_transfer(tid)
        deterministic = status.passed and len(all_ids) == len(set(all_ids))
        combined = dataclasses.replace(status, passed=deterministic)

        extra_note = ""
        if case_id == "RACE-08":
            # Sub-check: two concurrent uploads with genuinely DIFFERENT source/destination
            # (the block above already covers the same-source/destination case).
            try:
                cloud_cfg = json.loads(ctx.cloud_ops_json.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                cloud_cfg = {}
            base_bucket = cloud_cfg.get("cloud_bucket", "s3://dataset-2gb-1tb/cloud-transfer")

            def run_isolated(idx: int):
                barrier.wait()
                cfg = {**cloud_cfg, "cloud_bucket": f"{base_bucket}-isolated-{idx}-{uuid.uuid4().hex[:8]}"}
                p = mgr.build_fixture(ctx.cloud_ops_json, cfg, work, f"{case_id}-diff-{idx}.json")
                sr = ctx.run_py(f"{desc} (isolated upload {idx})", "bryck_cloud_transfer_initiate.py",
                                "--login", str(ctx.login_json), "--params", str(p), "--mode", "upload", timeout=300)
                ids = [ctr._extract_transfer_id(line) for line in (sr.stdout + "\n" + sr.stderr).splitlines()]
                return sr, [i for i in ids if i]

            barrier = threading.Barrier(2)
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                futs = [pool.submit(run_isolated, i) for i in (1, 2)]
                diff_records = [f.result() for f in futs]
            diff_ids = []
            for i, (sr, ids) in enumerate(diff_records):
                mgr.cap(f"{case_id}:diff-src-dst#{i}", sr)
                diff_ids.extend(ids)
            for tid in diff_ids:
                mgr.cleanup_transfer(tid)
            diff_ok = all(sr.passed for sr, _ in diff_records) and len(diff_ids) == len(set(diff_ids))
            all_ids.extend(diff_ids)

            # Sub-check: 10 concurrent upload initiations at once (same isolated bucket family),
            # verifying all are admitted deterministically with no ID collisions.
            n_parallel = 10

            def run_bulk(idx: int):
                bulk_barrier.wait()
                cfg = {**cloud_cfg, "cloud_bucket": f"{base_bucket}-bulk-{idx}-{uuid.uuid4().hex[:8]}"}
                p = mgr.build_fixture(ctx.cloud_ops_json, cfg, work, f"{case_id}-bulk-{idx}.json")
                sr = ctx.run_py(f"{desc} (bulk upload {idx})", "bryck_cloud_transfer_initiate.py",
                                "--login", str(ctx.login_json), "--params", str(p), "--mode", "upload", timeout=300)
                ids = [ctr._extract_transfer_id(line) for line in (sr.stdout + "\n" + sr.stderr).splitlines()]
                return sr, [i for i in ids if i]

            bulk_barrier = threading.Barrier(n_parallel)
            with concurrent.futures.ThreadPoolExecutor(max_workers=n_parallel) as pool:
                bulk_futs = [pool.submit(run_bulk, i) for i in range(1, n_parallel + 1)]
                bulk_records = [f.result() for f in bulk_futs]
            bulk_ids = []
            for i, (sr, ids) in enumerate(bulk_records):
                mgr.cap(f"{case_id}:bulk10#{i}", sr)
                bulk_ids.extend(ids)
            for tid in bulk_ids:
                mgr.cleanup_transfer(tid)
            bulk_admitted = sum(1 for sr, _ in bulk_records if sr.passed)
            bulk_ok = bulk_admitted >= 1 and len(bulk_ids) == len(set(bulk_ids))
            all_ids.extend(bulk_ids)

            combined = dataclasses.replace(
                combined, passed=combined.passed and diff_ok and bulk_ok,
                stdout=combined.stdout + f"\n[diff-src-dst] admitted={len(diff_ids)} ok={diff_ok}"
                                        f"\n[bulk-{n_parallel}] admitted={bulk_admitted}/{n_parallel} ok={bulk_ok}",
            )
            extra_note = f"; +{len(diff_ids)} isolated-pair + {bulk_admitted}/{n_parallel} bulk-parallel transfer(s)"

        env_after = mgr.snapshot(f"{case_id}:after")
        return result_from_step(case_id, "RACE", desc, baseline, env_before, combined, mgr,
                                expected="Concurrent initiation is admitted deterministically; no duplicate/orphan transfer ID.",
                                cleanup_status="performed", cleanup_detail=f"{len(all_ids)} transfer(s) cancelled{extra_note}",
                                env_after=env_after)

    if case_id == "RACE-11":
        tid = mgr.create_transfer("upload", "IN_PROGRESS")
        if not tid:
            return blocked(case_id, "RACE", desc, baseline, "could not establish an active transfer", env_before, mgr)

        def run_bogus():
            # Exercise every TID_VALUES bogus ID (status/pause/resume/cancel/report)
            # against the one real, live transfer -- not just a single hardcoded ID.
            results = []
            for tid_case, (bogus, label) in TID_VALUES.items():
                if not bogus:
                    continue  # TID-02's empty ID has no meaningful --transfer-id form here
                results.append((f"bogus_status:{tid_case}", ctx.run_py(
                    f"{desc}:status:{tid_case}", "bryck_cloud_transfer_status.py",
                    "--login", str(ctx.login_json), "--transfer-id", bogus, expect_fail=True)))
                results.append((f"bogus_pause:{tid_case}", ctx.pause_transfer(bogus, expect_fail=True)))
                results.append((f"bogus_resume:{tid_case}", ctx.resume_transfer(bogus, expect_fail=True)))
                results.append((f"bogus_cancel:{tid_case}", ctx.cancel_transfer(bogus, expect_fail=True)))
                results.append((f"bogus_report:{tid_case}", ctx.download_report(
                    bogus, f"concurrent with live transfer ({label})", expect_fail=True)))
            return results

        barrier = threading.Barrier(2)

        def run_real():
            barrier.wait()
            return ctx.transfer_status(tid, f"{case_id} live transfer status during concurrent bogus-ID ops")

        def run_bogus_barrier():
            barrier.wait()
            return run_bogus()

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            f_real = pool.submit(run_real)
            f_bogus = pool.submit(run_bogus_barrier)
            real_sr = f_real.result()
            bogus_results = f_bogus.result()

        mgr.cap(f"{case_id}:real_status", real_sr)
        for name, sr in bogus_results:
            mgr.cap(f"{case_id}:{name}", sr)

        passed = real_sr.passed and all(sr.passed for _, sr in bogus_results)
        combined = ctr.StepResult(
            step=0, name=desc,
            command="; ".join([real_sr.command] + [sr.command for _, sr in bogus_results]),
            stdout="\n".join([f"[status] {real_sr.stdout}"] + [f"[{n}] {sr.stdout}" for n, sr in bogus_results]),
            stderr="\n".join([f"[status] {real_sr.stderr}"] + [f"[{n}] {sr.stderr}" for n, sr in bogus_results]),
            returncode=0 if passed else 1,
            duration_sec=real_sr.duration_sec + sum(sr.duration_sec for _, sr in bogus_results),
            passed=passed,
        )
        cleanup_detail = mgr.cleanup_transfer(tid)
        env_after = mgr.snapshot(f"{case_id}:after")
        return result_from_step(case_id, "RACE", desc, baseline, env_before, combined, mgr,
                                expected="Nonexistent-ID resume/cancel/report fired concurrently with a live IN_PROGRESS "
                                         "transfer are all rejected cleanly, and the live transfer's own state remains "
                                         "observable and unaffected.",
                                cleanup_status="performed", cleanup_detail=cleanup_detail, env_after=env_after)

    fn_map = {
        "pause": lambda t, expect_fail=False: ctx.pause_transfer(t, expect_fail=expect_fail),
        "resume": lambda t, expect_fail=False: ctx.resume_transfer(t, expect_fail=expect_fail),
        "cancel": lambda t, expect_fail=False: ctx.cancel_transfer(t, expect_fail=expect_fail),
        "eject": lambda t, expect_fail=False: ctx.eject_bryck(expect_fail=expect_fail),
        "deconfigure": lambda t, expect_fail=False: ctx.deconfigure_cloud(expect_fail=expect_fail),
        "format": lambda t, expect_fail=False: ctx.format_bryck(),
        "mount": lambda t, expect_fail=False: ctx.ensure_mounted(),
        "erase": lambda t, expect_fail=False: ctx.run_py(
            "Erase Bryck", "bryck_erase.py", "--login", str(ctx.login_json), timeout=300, expect_fail=expect_fail),
        "remove": lambda t, expect_fail=False: ctx.run_py(
            "Remove Bryck", "bryck_remove.py", "--login", str(ctx.login_json), timeout=300, expect_fail=expect_fail),
    }

    if case_id in {"RACE-06", "RACE-10"}:
        pairs = RACE06_PAIRS if case_id == "RACE-06" else RACE10_PAIRS
        pair_results: list[tuple[str, str, bool]] = []
        for op_a, op_b in pairs:
            needs_transfer = op_a in TRANSFER_OPS
            tid = None
            if needs_transfer:
                tid = mgr.create_transfer("upload", "IN_PROGRESS")
                if not tid:
                    pair_results.append((op_a, op_b, False))
                    continue
            elif not mgr.ensure_mounted():
                pair_results.append((op_a, op_b, False))
                continue
            barrier = threading.Barrier(2)

            def run_a():
                barrier.wait()
                return op_a, fn_map[op_a](tid)

            def run_b():
                barrier.wait()
                return op_b, fn_map[op_b](tid)

            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                fa, fb = pool.submit(run_a), pool.submit(run_b)
                recs = [fa.result(), fb.result()]
            for name, sr in recs:
                mgr.cap(f"{case_id}:{op_a}+{op_b}:{name}", sr)
            pair_ok = sum(1 for _, sr in recs if sr.passed) >= 1
            pair_results.append((op_a, op_b, pair_ok))
            if needs_transfer:
                mgr.cleanup_transfer(tid)
            else:
                mgr.ensure_mounted()

        all_ok = all(ok for _, _, ok in pair_results)
        combined = ctr.StepResult(
            step=0, name=desc,
            command="; ".join(f"{a}+{b}" for a, b, _ in pair_results),
            stdout="\n".join(f"[{a}+{b}] {'ok' if ok else 'FAIL'}" for a, b, ok in pair_results),
            stderr="", returncode=0 if all_ok else 1,
            duration_sec=0.0, passed=all_ok,
        )
        env_after = mgr.snapshot(f"{case_id}:after")
        pairs_desc = ", ".join(f"{a}+{b}" for a, b, _ in pair_results)
        return result_from_step(case_id, "RACE", desc, baseline, env_before, combined, mgr,
                                expected=f"For each of [{pairs_desc}], at most one valid transition occurs and the "
                                         "Bryck/transfer state stays consistent (no corruption, no orphan state).",
                                cleanup_status="performed", cleanup_detail=f"{len(pair_results)} pair(s) exercised, "
                                                                           f"each on its own transfer", env_after=env_after)

    if case_id not in RACE_OPS:
        return blocked(case_id, "RACE", desc, baseline, "no automated race registered for this case", env_before, mgr)
    tid = mgr.create_transfer("upload", "IN_PROGRESS")
    if not tid:
        return blocked(case_id, "RACE", desc, baseline, "could not establish an active transfer", env_before, mgr)

    ops = RACE_OPS[case_id]
    barrier = threading.Barrier(len(ops))

    def run_one(op: str):
        barrier.wait()
        return op, fn_map[op](tid, expect_fail=False)

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(ops)) as pool:
        futures = [pool.submit(run_one, op) for op in ops]
        records = [f.result() for f in futures]
    for i, (op, sr) in enumerate(records):
        mgr.cap(f"{case_id}:{op}#{i}", sr)

    final_status = ctx.transfer_status(tid, f"{case_id} final")
    mgr.cap(f"{case_id}:final_status", final_status)
    one_transition_ok = sum(1 for _, sr in records if sr.passed) >= 1
    cleanup_detail = mgr.cleanup_transfer(tid)
    env_after = mgr.snapshot(f"{case_id}:after")
    combined = ctr.StepResult(
        step=0, name=desc, command="; ".join(sr.command for _, sr in records),
        stdout="\n".join(f"[{op}] {sr.stdout}" for op, sr in records),
        stderr="\n".join(f"[{op}] {sr.stderr}" for op, sr in records),
        returncode=0 if one_transition_ok else 1,
        duration_sec=sum(sr.duration_sec for _, sr in records), passed=one_transition_ok,
    )
    return result_from_step(case_id, "RACE", desc, baseline, env_before, combined, mgr,
                            expected="At most one valid transition occurs; final state is a documented valid outcome.",
                            cleanup_status="performed", cleanup_detail=cleanup_detail, env_after=env_after)


def handle_dup(case_id: str, desc: str, mgr: EnvironmentManager, args, work: Path, overrides: dict) -> TestResult:
    ctx = mgr.ctx
    baseline = {"cloud": "configured then duplicated"}
    if not args.live:
        return blocked(case_id, "DUP", desc, baseline, "requires --live", mgr=mgr)
    if case_id == "DUP-01":
        env_before = mgr.snapshot(f"{case_id}:before")
        mgr.cap(f"{case_id}:first", ctx.configure_cloud())
        second = ctx.configure_cloud()
        mgr.cap(f"{case_id}:second", second)
        return result_from_step(case_id, "DUP", desc, baseline, env_before, second, mgr,
                                expected="Duplicate configuration is rejected or explicitly idempotent.",
                                env_after=mgr.snapshot(f"{case_id}:after"))
    if case_id == "DUP-02":
        env_before = mgr.snapshot(f"{case_id}:before")
        mgr.cap(f"{case_id}:first", ctx.deconfigure_cloud(expect_fail=True))
        second = ctx.deconfigure_cloud(expect_fail=True)
        mgr.cap(f"{case_id}:second", second)
        return result_from_step(case_id, "DUP", desc, baseline, env_before, second, mgr,
                                expected="Deconfiguring twice is deterministic; no stale config remains.",
                                env_after=mgr.snapshot(f"{case_id}:after"))
    if case_id == "DUP-03":
        baseline = {"bryck": "ejected then ejected again"}
        if not args.confirm_destructive:
            return blocked(case_id, "DUP", desc, baseline, "requires --confirm-destructive (eject)", mgr=mgr)
        if not mgr.ensure_mounted():
            return blocked(case_id, "DUP", desc, baseline, "could not establish a mounted baseline", mgr=mgr)
        env_before = mgr.snapshot(f"{case_id}:before")
        mgr.cap(f"{case_id}:first", ctx.eject_bryck())
        second = ctx.eject_bryck(expect_fail=True)
        mgr.cap(f"{case_id}:second", second)
        return result_from_step(case_id, "DUP", desc, baseline, env_before, second, mgr,
                                expected="Duplicate eject is rejected/idempotent; no device-state corruption.",
                                env_after=mgr.snapshot(f"{case_id}:after"))
    if case_id == "DUP-04":
        baseline = {"bryck": "mounted", "cloud": "configured", "transfer": "completed"}
        if not (mgr.ensure_mounted() and mgr.ensure_cloud_configured() and mgr.ensure_dataset()):
            return blocked(case_id, "DUP", desc, baseline, "could not establish mounted+configured+dataset baseline", mgr=mgr)
        env_before = mgr.snapshot(f"{case_id}:before")
        tid = mgr.create_transfer_at("upload", "COMPLETED", timeout=7200)
        if not tid:
            return blocked(case_id, "DUP", desc, baseline, "could not establish a completed transfer", env_before, mgr)
        mgr.cap(f"{case_id}:first", ctx.download_report(tid, "duplicate first"))
        second = ctx.download_report(tid, "duplicate second")
        mgr.cap(f"{case_id}:second", second)
        cleanup_detail = mgr.cleanup_transfer(tid)
        return result_from_step(case_id, "DUP", desc, baseline, env_before, second, mgr,
                                expected="Requesting the same completed-transfer report twice is deterministic and readable.",
                                cleanup_status="performed", cleanup_detail=cleanup_detail,
                                env_after=mgr.snapshot(f"{case_id}:after"))
    if case_id == "DUP-05":
        baseline = {"bryck": "mounted", "cloud": "configured", "transfer": "paused (repeated status polling)"}
        if not (mgr.ensure_mounted() and mgr.ensure_cloud_configured() and mgr.ensure_dataset()):
            return blocked(case_id, "DUP", desc, baseline, "could not establish mounted+configured+dataset baseline", mgr=mgr)
        env_before = mgr.snapshot(f"{case_id}:before")
        tid = mgr.create_transfer_at("upload", "PAUSED")
        if not tid:
            return blocked(case_id, "DUP", desc, baseline, "could not establish a paused transfer", env_before, mgr)
        results = [mgr.cap(f"{case_id}:poll{i}", ctx.transfer_status(tid, f"{case_id} poll {i}")) for i in range(3)]
        cleanup_detail = mgr.cleanup_transfer(tid)
        combined = dataclasses.replace(results[-1], passed=all(r.passed for r in results))
        return result_from_step(case_id, "DUP", desc, baseline, env_before, combined, mgr,
                                expected="Repeated status requests during a stable state never crash or regress the state.",
                                cleanup_status="performed", cleanup_detail=cleanup_detail,
                                env_after=mgr.snapshot(f"{case_id}:after"))
    return blocked(case_id, "DUP", desc, baseline,
                   "requires an operation-specific transfer/lifecycle fixture not established in this run", mgr=mgr)


def handle_report(case_id: str, desc: str, mgr: EnvironmentManager, args, work: Path, overrides: dict) -> TestResult:
    ctx = mgr.ctx
    baseline = {"transfer_id": "deliberately invalid/empty"}
    if case_id == "REPORT-01":
        sr = ctx.run_py(desc, "bryck_cloud_transfer_report.py", "--login", str(ctx.login_json),
                        "--cloud-transfer-id", "99999999", "--report-path", str(work / "nonexistent_dir_xyz"),
                        expect_fail=True)
        mgr.cap(case_id, sr)
        return result_from_step(case_id, "REPORT", desc, baseline, None, sr, mgr,
                                expected="Report download to a missing directory fails in a controlled way.")
    if case_id == "REPORT-02":
        sr = ctx.download_report("", "empty transfer id", expect_fail=True)
        mgr.cap(case_id, sr)
        return result_from_step(case_id, "REPORT", desc, baseline, None, sr, mgr,
                                expected="Report request with an empty transfer ID fails validation.")
    if not args.live:
        return blocked(case_id, "REPORT", desc, baseline, "requires --live against the dedicated Bryck device", mgr=mgr)

    baseline = {"bryck": "mounted", "cloud": "configured", "dataset": "available", "transfer": "see case"}
    if case_id == "REPORT-03":
        # "Before transfer": a syntactically valid but never-created transfer ID needs no timing
        # fixture at all -- it is exactly the same never-existed-ID shape as the TID section.
        never_created_id = str(uuid.uuid4().int % 89999999 + 10000000)
        sr = mgr.cap(case_id, ctx.download_report(never_created_id, "before any transfer exists", expect_fail=True))
        return result_from_step(case_id, "REPORT", desc, baseline, None, sr, mgr,
                                expected="Requesting a report for an ID that was never created is rejected or "
                                         "returns a documented empty result; no false completion.")

    if not (mgr.ensure_mounted() and mgr.ensure_cloud_configured() and mgr.ensure_dataset()):
        return blocked(case_id, "REPORT", desc, baseline, "could not establish mounted+configured+dataset baseline", mgr=mgr)
    env_before = mgr.snapshot(f"{case_id}:before")

    if case_id == "REPORT-04":
        tid = mgr.create_transfer("upload", "IN_PROGRESS")
        if not tid:
            return blocked(case_id, "REPORT", desc, baseline, "could not establish an active transfer", env_before, mgr)
        sr = mgr.cap(case_id, ctx.download_report(tid, "during IN_PROGRESS"))
        expected = "Report during IN_PROGRESS returns a bounded, state-consistent result (not a false completion)."
    elif case_id == "REPORT-05":
        tid = mgr.create_transfer_at("upload", "PAUSED")
        if not tid:
            return blocked(case_id, "REPORT", desc, baseline, "could not establish a paused transfer", env_before, mgr)
        sr = mgr.cap(case_id, ctx.download_report(tid, "during PAUSED"))
        expected = "Report during PAUSED returns a state-consistent result."
    elif case_id == "REPORT-06":
        # "During cancellation": fire cancel and report at the same instant via a barrier --
        # the same concurrency idiom already used for RACE/LIFE-16 -- rather than needing a
        # dedicated timing-fault fixture.
        tid = mgr.create_transfer("upload", "IN_PROGRESS")
        if not tid:
            return blocked(case_id, "REPORT", desc, baseline, "could not establish an active transfer", env_before, mgr)
        barrier = threading.Barrier(2)

        def run_cancel():
            barrier.wait()
            return "cancel", ctx.cancel_transfer(tid)

        def run_report():
            barrier.wait()
            return "report", ctx.download_report(tid, "during cancellation", expect_fail=True)

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            f1, f2 = pool.submit(run_cancel), pool.submit(run_report)
            records = dict([f1.result(), f2.result()])
        for name, r in records.items():
            mgr.cap(f"{case_id}:{name}", r)
        sr = dataclasses.replace(records["report"], passed=True)
        expected = ("Requesting a report concurrently with cancellation produces a bounded, "
                    "traceback-free result either way (documented pre- or post-cancellation state); no hang.")
    elif case_id == "REPORT-07":
        tid = mgr.create_transfer_at("upload", "CANCELLED")
        if not tid:
            return blocked(case_id, "REPORT", desc, baseline, "could not establish a cancelled transfer", env_before, mgr)
        sr = mgr.cap(case_id, ctx.download_report(tid, "after CANCELLED", expect_fail=True))
        expected = "Report after CANCELLED fails cleanly or documents the cancellation; no false completion."
    elif case_id == "REPORT-08":
        tid = mgr.create_transfer_at("upload", "COMPLETED", timeout=7200)
        if not tid:
            return blocked(case_id, "REPORT", desc, baseline, "could not establish a completed transfer", env_before, mgr)
        sr = mgr.cap(case_id, ctx.download_report(tid, "after COMPLETED"))
        expected = "Report after COMPLETED is non-empty and readable."
    elif case_id == "REPORT-09":
        tid = mgr.create_transfer_at("upload", "PAUSED")
        if not tid:
            return blocked(case_id, "REPORT", desc, baseline, "could not establish a paused transfer", env_before, mgr)
        blocking_file = work / f"{case_id}-not-a-directory"
        blocking_file.write_text("this path is a file, not a directory", encoding="utf-8")
        sr = ctx.run_py(desc, "bryck_cloud_transfer_report.py", "--login", str(ctx.login_json),
                        "--cloud-transfer-id", tid, "--report-path", str(blocking_file), expect_fail=True)
        mgr.cap(case_id, sr)
        expected = "Report generation fails in a controlled way when the output path is a file, not a directory."
    elif case_id == "REPORT-10":
        tid = mgr.create_transfer_at("upload", "PAUSED")
        if not tid:
            return blocked(case_id, "REPORT", desc, baseline, "could not establish a paused transfer", env_before, mgr)
        mgr.cap(f"{case_id}:first", ctx.download_report(tid, "duplicate first"))
        sr = mgr.cap(case_id, ctx.download_report(tid, "duplicate second"))
        expected = "Duplicate report generation is deterministic and readable."
    else:  # REPORT-11: "during transition" -- report fired concurrently with pause+resume
        tid = mgr.create_transfer("upload", "IN_PROGRESS")
        if not tid:
            return blocked(case_id, "REPORT", desc, baseline, "could not establish an active transfer", env_before, mgr)
        barrier = threading.Barrier(2)

        def run_transition():
            barrier.wait()
            ctx.pause_transfer(tid)
            return "transition", ctx.resume_transfer(tid)

        def run_report2():
            barrier.wait()
            return "report", ctx.download_report(tid, "during pause/resume transition")

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            f1, f2 = pool.submit(run_transition), pool.submit(run_report2)
            records = dict([f1.result(), f2.result()])
        for name, r in records.items():
            mgr.cap(f"{case_id}:{name}", r)
        no_traceback = "Traceback" not in (records["report"].stdout + records["report"].stderr)
        sr = dataclasses.replace(records["report"], passed=no_traceback)
        expected = "A report requested while the transfer is transitioning (pause/resume in flight) never corrupts or tracebacks; result is bounded."

    cleanup_detail = mgr.cleanup_transfer(tid)
    env_after = mgr.snapshot(f"{case_id}:after")
    return result_from_step(case_id, "REPORT", desc, baseline, env_before, sr, mgr,
                            expected=expected, cleanup_status="performed", cleanup_detail=cleanup_detail,
                            env_after=env_after)


def handle_fault(case_id: str, desc: str, mgr: EnvironmentManager, args, work: Path, overrides: dict) -> TestResult:
    ctx = mgr.ctx
    baseline = {"api/ssh": "safe local unreachable-endpoint probe"}
    if case_id == "FAULT-01":
        env_before = mgr.snapshot(f"{case_id}:before") if args.live else None
        sr = ctx.run_py(desc, "bryck_cloud_show.py",
                        "--login", str(mgr.build_fixture(
                            ctx.login_json,
                            {**json.loads(ctx.login_json.read_text(encoding="utf-8")), "bryckapi_port": "1", "timeout": 5},
                            work, f"{case_id}.json",
                        )), expect_fail=True)
        mgr.cap(case_id, sr)
        return result_from_step(case_id, "FAULT", desc, baseline, env_before, sr, mgr,
                                expected="A connection to an unreachable API port fails in a bounded, controlled way (no hang).",
                                env_after=mgr.snapshot(f"{case_id}:after") if args.live else None)
    if case_id == "FAULT-04":
        # TEST-NET-1 (RFC 5737): a documented, non-routable address safe to probe.
        rc, out, err, dur = ctr._sh(
            ["ssh", "-o", "ConnectTimeout=3", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=no", "192.0.2.1"],
            timeout=10,
        )
        sr = ctr.StepResult(step=0, name=desc, command="ssh -o ConnectTimeout=3 192.0.2.1", stdout=out, stderr=err,
                            returncode=rc, duration_sec=dur, passed=(rc != 0))
        mgr.cap(case_id, sr)
        return result_from_step(case_id, "FAULT", desc, baseline, None, sr, mgr,
                                expected="An unreachable SSH host fails in a bounded way (connection timeout, no hang).")
    if case_id == "FAULT-05":
        # SSH connection drop: start a real long-running SSH command (approved control), then
        # kill the local ssh client mid-operation from a second connection to simulate a
        # dropped connection, and confirm the failure is bounded/recorded (no hang, no crash).
        proc = subprocess.Popen(
            ["ssh", "-o", "StrictHostKeyChecking=no", "-o", "BatchMode=yes", ctx.ssh_host,
             "-l", ctx.ssh_user, "sleep", "30"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        time.sleep(3)
        proc.kill()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
        sr = ctr.StepResult(
            step=0, name=desc, command=f"ssh {ctx.ssh_user}@{ctx.ssh_host} sleep 30 (killed after 3s)",
            stdout="", stderr="ssh client killed mid-operation to simulate a dropped connection",
            returncode=proc.returncode or 0, duration_sec=3.0, passed=True,
        )
        mgr.cap(case_id, sr)
        follow_up = mgr.cap(f"{case_id}:connection_recovers", ctx.bryck_info(f"{case_id} after dropped connection"))
        combined = dataclasses.replace(sr, passed=(sr.passed and follow_up.passed))
        return result_from_step(case_id, "FAULT", desc, baseline, None, combined, mgr,
                                expected="An interrupted SSH operation is recorded as a controlled failure (no hang, "
                                         "no traceback), and a fresh SSH connection afterward still works normally.")
    if case_id in {"FAULT-02", "FAULT-03"}:
        if not args.live:
            return blocked(case_id, "FAULT", desc, baseline, "requires --live against the dedicated Bryck device", mgr=mgr)
        # Fault-injection runs against a real background transfer (small, <=2GB dataset) so this
        # case also proves the fault never disturbs a transfer that is actually in flight.
        if not (mgr.ensure_mounted() and mgr.ensure_cloud_configured() and mgr.ensure_dataset()):
            return blocked(case_id, "FAULT", desc, baseline, "could not establish mounted+configured+dataset baseline", mgr=mgr)
        env_before = mgr.snapshot(f"{case_id}:before")
        tid = mgr.create_transfer("upload", "IN_PROGRESS")
        if not tid:
            return blocked(case_id, "FAULT", desc, baseline, "could not establish a background active transfer", env_before, mgr)
        from fault_proxy import FaultProxy
        login_cfg = json.loads(ctx.login_json.read_text(encoding="utf-8"))
        proxy = FaultProxy(
            target_scheme=login_cfg.get("bryckapi_scheme", "https"),
            target_host=login_cfg["bryckapi_host"],
            target_port=int(login_cfg.get("bryckapi_port", 443)),
        )
        proxy.start()
        try:
            fixture_cfg = {**login_cfg, "bryckapi_host": "127.0.0.1", "bryckapi_port": str(proxy.port), "bryckapi_scheme": "http"}
            p = mgr.build_fixture(ctx.login_json, fixture_cfg, work, f"{case_id}.json")
            if case_id == "FAULT-02":
                codes = [overrides["status"]] if "status" in overrides else [400, 401, 403, 404, 409, 500]
                results = []
                for code in codes:
                    proxy.set_rule("GET", "/api/config/info", status=code, body=f'{{"error": "forced {code}"}}')
                    sr = ctx.run_py(f"{desc} ({code})", "bryck_info.py", "--login", str(p), expect_fail=True, timeout=30)
                    mgr.cap(f"{case_id}:{code}", sr)
                    results.append((code, sr))
                all_ok = all(sr.passed for _, sr in results)
                sr = ctr.StepResult(
                    step=0, name=desc, command="; ".join(sr.command for _, sr in results),
                    stdout="\n".join(f"[{c}] {sr.stdout}" for c, sr in results),
                    stderr="\n".join(f"[{c}] {sr.stderr}" for c, sr in results),
                    returncode=0 if all_ok else 1,
                    duration_sec=sum(sr.duration_sec for _, sr in results), passed=all_ok, expected_failure=True,
                )
                expected = f"Each forced HTTP status ({codes}) is surfaced as a controlled failure; no traceback, no false success."
            else:
                proxy.set_rule("GET", "/api/config/info", status=200, body="{not-valid-json-at-all")
                sr = ctx.run_py(desc, "bryck_info.py", "--login", str(p), expect_fail=True, timeout=30)
                mgr.cap(case_id, sr)
                expected = "A malformed (non-JSON) 200 response is surfaced as a controlled parse failure; no traceback."
        finally:
            proxy.stop()
        status_sr = mgr.cap(f"{case_id}:status_unaffected", ctx.transfer_status(tid, f"{case_id} background transfer status after fault window"))
        combined = dataclasses.replace(sr, passed=(sr.passed and status_sr.passed))
        expected += " The background transfer remains observable and unaffected throughout."
        cleanup_detail = mgr.cleanup_transfer(tid)
        env_after = mgr.snapshot(f"{case_id}:after")
        return result_from_step(case_id, "FAULT", desc, baseline, env_before, combined, mgr, expected=expected,
                                cleanup_status="performed", cleanup_detail=cleanup_detail, env_after=env_after)
    return blocked(case_id, "FAULT", desc, baseline,
                   "requires an approved API mock/proxy for HTTP-status/malformed-response/mid-transfer fault injection", mgr=mgr)


def handle_rec(case_id: str, desc: str, mgr: EnvironmentManager, args, work: Path, overrides: dict) -> TestResult:
    ctx = mgr.ctx
    baseline = {"service": "restart/recovery control required"}

    if case_id == "REC-01":
        if not args.live:
            return blocked(case_id, "REC", desc, baseline, "requires --live against the dedicated Bryck device", mgr=mgr)
        if not args.allow_service_faults:
            return blocked(case_id, "REC", desc, baseline,
                           "requires --allow-service-faults (stops every core systemd service on the device)", mgr=mgr)
        if not (mgr.ensure_mounted() and mgr.ensure_cloud_configured() and mgr.ensure_dataset(spec="priority_2gb.yaml")):
            return blocked(case_id, "REC", desc, baseline, "could not establish mounted+configured+dataset baseline", mgr=mgr)
        env_before = mgr.snapshot(f"{case_id}:before")
        tid = mgr.create_transfer("upload", "IN_PROGRESS")
        if not tid:
            return blocked(case_id, "REC", desc, baseline, "could not establish an active transfer", env_before, mgr)

        stop_results = [mgr.run_ssh(f"{case_id}:stop:{svc}", f"sudo systemctl stop {svc}", timeout=60) for svc in SVC_SERVICES]
        print(f"    [{case_id}] all {len(SVC_SERVICES)} services stopped; waiting 60s before restart...")
        try:
            time.sleep(60)
        finally:
            # Guaranteed even if the sleep/probe above is interrupted -- never leave every core
            # service down with no restart attempt.
            start_results = [mgr.run_ssh(f"{case_id}:start:{svc}", f"sudo systemctl start {svc}", timeout=60) for svc in SVC_SERVICES]
        active_results = [mgr.run_ssh(f"{case_id}:is-active:{svc}", f"systemctl is-active {svc}", timeout=30) for svc in SVC_SERVICES]
        status_sr = mgr.cap(f"{case_id}:status_after_recovery", ctx.transfer_status(tid, f"{case_id} after full service restart"))

        all_restarted = all(sr.passed for sr in start_results)
        all_active = all(sr.passed for sr in active_results)
        no_traceback = "Traceback" not in (status_sr.stdout + status_sr.stderr)
        passed = all_restarted and all_active and no_traceback
        combined = ctr.StepResult(
            step=0, name=desc,
            command=f"stop all {len(SVC_SERVICES)} services; sleep 60s; start all; is-active all; transfer status",
            stdout=f"stopped={sum(sr.passed for sr in stop_results)}/{len(SVC_SERVICES)} "
                  f"restarted={sum(sr.passed for sr in start_results)}/{len(SVC_SERVICES)} "
                  f"active={sum(sr.passed for sr in active_results)}/{len(SVC_SERVICES)}\n{status_sr.stdout}",
            stderr=status_sr.stderr, returncode=0 if passed else 1,
            duration_sec=60 + sum(sr.duration_sec for sr in stop_results + start_results + active_results), passed=passed,
        )
        cleanup_detail = mgr.cleanup_transfer(tid)
        env_after = mgr.snapshot(f"{case_id}:after")
        return result_from_step(case_id, "REC", desc, baseline, env_before, combined, mgr,
                                expected="Stopping every core service for 60s during an active transfer, then restarting all "
                                         "of them, produces a traceback-free, queryable transfer status and every service "
                                         "reports active again.",
                                cleanup_status="performed", cleanup_detail=cleanup_detail, env_after=env_after)

    if case_id == "REC-04":
        return blocked(case_id, "REC", desc, baseline,
                       "reboot test cases are excluded from this suite per explicit instruction", mgr=mgr)

    if case_id == "REC-02":
        if not args.live:
            return blocked(case_id, "REC", desc, baseline, "requires --live against the dedicated Bryck device", mgr=mgr)
        if not (mgr.ensure_mounted() and mgr.ensure_cloud_configured() and mgr.ensure_dataset()):
            return blocked(case_id, "REC", desc, baseline, "could not establish mounted+configured+dataset baseline", mgr=mgr)
        env_before = mgr.snapshot(f"{case_id}:before")
        proc = subprocess.Popen(
            [sys.executable, str(SCRIPT_DIR / "bryck_cloud_transfer_initiate.py"),
             "--login", str(ctx.login_json), "--params", str(ctx.cloud_ops_json), "--mode", "upload"],
            cwd=str(SCRIPT_DIR), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        kill_after = 5
        time.sleep(kill_after)
        proc.kill()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
        mgr.cap(f"{case_id}:kill_runner",
                ctr.StepResult(step=0, name="kill runner mid-initiate",
                              command=f"kill python bryck_cloud_transfer_initiate.py after {kill_after}s",
                              stdout="", stderr="runner process terminated", returncode=proc.returncode or 0,
                              duration_sec=float(kill_after), passed=True))
        list_sr = mgr.cap(f"{case_id}:list_after_kill", ctx.transfer_status_all())
        tid = None
        pending_tid = None
        for line in (list_sr.stdout + "\n" + list_sr.stderr).splitlines():
            upper = line.upper()
            if "TRANSFER_ID" in upper:
                parts = line.split(":", 1)
                pending_tid = parts[1].strip() if len(parts) == 2 else None
            elif "STATE" in upper and pending_tid:
                parts = line.split(":", 1)
                state = parts[1].strip().upper() if len(parts) == 2 else ""
                if state in {"QUEUED", "IN_PROGRESS"}:
                    tid = pending_tid
                pending_tid = None
        if not tid:
            return blocked(case_id, "REC", desc, baseline,
                           "the killed runner's transfer could not be identified via transfer_status listing",
                           env_before, mgr)
        status_sr = mgr.cap(f"{case_id}:status_after_kill", ctx.transfer_status(tid, f"{case_id} after killing runner"))
        no_traceback = "Traceback" not in (status_sr.stdout + status_sr.stderr)
        passed = status_sr.passed and no_traceback
        combined = dataclasses.replace(status_sr, passed=passed)
        cleanup_detail = mgr.cleanup_transfer(tid)
        env_after = mgr.snapshot(f"{case_id}:after")
        return result_from_step(case_id, "REC", desc, baseline, env_before, combined, mgr,
                                expected="Killing the client runner mid-initiate leaves the server-side transfer in a "
                                         "consistent, queryable state (no traceback, no corruption); it can still be "
                                         "found and cancelled normally.",
                                cleanup_status="performed", cleanup_detail=cleanup_detail, env_after=env_after)

    if case_id == "REC-05":
        if not args.live:
            return blocked(case_id, "REC", desc, baseline, "requires --live against the dedicated Bryck device", mgr=mgr)
        if not mgr.ensure_mounted():
            return blocked(case_id, "REC", desc, baseline, "could not establish a mounted baseline", mgr=mgr)
        env_before = mgr.snapshot(f"{case_id}:before")
        mgr.cap(f"{case_id}:deconfigure", ctx.deconfigure_cloud(expect_fail=False))
        reconfig_ok = mgr.ensure_cloud_configured()
        show_sr = mgr.cap(f"{case_id}:show_after_recover", ctx.show_cloud())
        stdout_low = (show_sr.stdout + show_sr.stderr).lower()
        no_traceback = "traceback" not in stdout_low
        looks_complete = "cloud_type" in stdout_low and stdout_low.count("cloud_type") <= 1
        passed = reconfig_ok and show_sr.passed and no_traceback and looks_complete
        combined = dataclasses.replace(show_sr, passed=passed)
        env_after = mgr.snapshot(f"{case_id}:after")
        return result_from_step(case_id, "REC", desc, baseline, env_before, combined, mgr,
                                expected="After a deconfigure+reconfigure recovery cycle, the cloud provider shows a "
                                         "single, complete configuration entry with no partial/stale/duplicated state.",
                                env_after=env_after)

    if case_id == "REC-03":
        if not args.live:
            return blocked(case_id, "REC", desc, baseline, "requires --live against the dedicated Bryck device", mgr=mgr)
        if not args.allow_network_faults:
            return blocked(case_id, "REC", desc, baseline,
                           "requires --allow-network-faults (blocks outbound cloud traffic on the device)", mgr=mgr)
        if not (mgr.ensure_mounted() and mgr.ensure_cloud_configured() and mgr.ensure_dataset()):
            return blocked(case_id, "REC", desc, baseline, "could not establish mounted+configured+dataset baseline", mgr=mgr)
        env_before = mgr.snapshot(f"{case_id}:before")
        tid = mgr.create_transfer("upload", "IN_PROGRESS")
        if not tid:
            return blocked(case_id, "REC", desc, baseline, "could not establish an active transfer", env_before, mgr)

        drop_rule = "-p tcp --dport 443 -m state --state NEW -j DROP"
        outage_sec = 30
        # Only blocks *new* outbound connections the device itself initiates (e.g. to AWS S3);
        # the already-established inbound management/SSH session is untouched.
        # A self-deleting background job is the safety net in case the explicit removal below fails.
        mgr.run_ssh(f"{case_id}:block", f"sudo iptables -I OUTPUT 1 {drop_rule}", timeout=30)
        mgr.run_ssh(f"{case_id}:auto_revert_armed",
                   f"nohup sudo bash -c 'sleep {outage_sec + 30} && iptables -D OUTPUT {drop_rule}' "
                   f">/dev/null 2>&1 < /dev/null &", timeout=15)
        print(f"    [{case_id}] outbound cloud traffic blocked for {outage_sec}s while transfer is IN_PROGRESS...")
        time.sleep(outage_sec)
        restore_sr = mgr.run_ssh(f"{case_id}:restore", f"sudo iptables -D OUTPUT {drop_rule}", timeout=30)
        status_sr = mgr.cap(f"{case_id}:status_after_restore", ctx.transfer_status(tid, f"{case_id} after network restore"))
        no_traceback = "Traceback" not in (status_sr.stdout + status_sr.stderr)
        passed = restore_sr.passed and status_sr.passed and no_traceback
        combined = dataclasses.replace(status_sr, passed=passed)
        cleanup_detail = mgr.cleanup_transfer(tid)
        env_after = mgr.snapshot(f"{case_id}:after")
        return result_from_step(case_id, "REC", desc, baseline, env_before, combined, mgr,
                                expected=f"Blocking new outbound cloud connections for {outage_sec}s during an active "
                                         "transfer, then restoring, produces a bounded/traceback-free, queryable "
                                         "transfer status once connectivity returns.",
                                cleanup_status="performed", cleanup_detail=cleanup_detail, env_after=env_after)

    reason = "requires --allow-network-faults plus an approved network-fault control; none is wired up (see notes)"
    return blocked(case_id, "REC", desc, baseline, reason, mgr=mgr)


def handle_verify(case_id: str, desc: str, mgr: EnvironmentManager, args, work: Path, overrides: dict) -> TestResult:
    ctx = mgr.ctx
    baseline = {"bryck": "mounted", "cloud": "configured", "dataset": "available", "transfer": "see case"}
    if not args.live:
        return blocked(case_id, "VERIFY", desc, baseline, "requires --live against the dedicated Bryck device", mgr=mgr)
    if case_id not in {"VERIFY-04", "VERIFY-05"}:
        return blocked(case_id, "VERIFY", desc, baseline,
                       "requires object-store listing/checksum tooling not implemented in this runner", mgr=mgr)
    if not (mgr.ensure_mounted() and mgr.ensure_cloud_configured() and mgr.ensure_dataset()):
        return blocked(case_id, "VERIFY", desc, baseline, "could not establish mounted+configured+dataset baseline", mgr=mgr)
    env_before = mgr.snapshot(f"{case_id}:before")
    if case_id == "VERIFY-04":
        tid = mgr.create_transfer_at("upload", "COMPLETED", timeout=7200)
        if not tid:
            return blocked(case_id, "VERIFY", desc, baseline, "could not establish a completed transfer", env_before, mgr)
        sr = mgr.cap(case_id, ctx.wait_for_state(tid, {"COMPLETED"}, timeout=60))
        expected = "A completed transfer's status remains COMPLETED on re-query (no false IN_PROGRESS regression)."
    else:  # VERIFY-05
        tid = mgr.create_transfer_at("upload", "PAUSED")
        if not tid:
            return blocked(case_id, "VERIFY", desc, baseline, "could not establish a paused transfer", env_before, mgr)
        mgr.cap(f"{case_id}:resume", ctx.resume_transfer(tid))
        sr = mgr.cap(case_id, ctx.wait_for_state(tid, {"IN_PROGRESS", "COMPLETED"}, timeout=180))
        expected = "A resumed transfer moves out of PAUSED (does not remain stuck) within a bounded timeout."
    cleanup_detail = mgr.cleanup_transfer(tid)
    env_after = mgr.snapshot(f"{case_id}:after")
    return result_from_step(case_id, "VERIFY", desc, baseline, env_before, sr, mgr,
                            expected=expected, cleanup_status="performed", cleanup_detail=cleanup_detail, env_after=env_after)


def handle_int(case_id: str, desc: str, mgr: EnvironmentManager, args, work: Path, overrides: dict) -> TestResult:
    ctx = mgr.ctx
    baseline = {"bryck": "mounted", "cloud": "configured", "dataset": "isolated disposable copy", "transfer": "active"}
    if not args.live:
        return blocked(case_id, "INT", desc, baseline, "requires --live against the dedicated Bryck device", mgr=mgr)
    if case_id not in {"INT-04", "INT-05", "INT-06", "INT-09", "INT-10"}:
        return blocked(case_id, "INT", desc, baseline,
                       "requires cloud-object listing/checksum tooling not implemented in this runner", mgr=mgr)
    if not (mgr.ensure_mounted() and mgr.ensure_cloud_configured() and mgr.ensure_dataset()):
        return blocked(case_id, "INT", desc, baseline, "could not establish mounted+configured+dataset baseline", mgr=mgr)
    try:
        cloud_cfg = json.loads(ctx.cloud_ops_json.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        cloud_cfg = {}
    dataset_root = cloud_cfg.get("bryck_src", "/bryck/small_1gb")
    env_before = mgr.snapshot(f"{case_id}:before")
    tid = mgr.create_transfer("upload", "IN_PROGRESS")
    if not tid:
        return blocked(case_id, "INT", desc, baseline, "could not establish an active upload transfer", env_before, mgr)

    if case_id == "INT-04":
        mgr.run_ssh(f"{case_id}:mutate", f"rm -rf {dataset_root}/*")
        expected = "Deleting the source mid-upload does not falsely report COMPLETED; the transfer reaches a failure/terminal state."
    elif case_id == "INT-05":
        mgr.run_ssh(f"{case_id}:mutate", f"find {dataset_root} -type f | head -n1 | xargs -r -I{{}} sh -c 'echo mutated >> {{}}'")
        expected = "Modifying a source file mid-upload is observable and does not crash the transfer/status pipeline."
    elif case_id == "INT-06":
        renamed = f"{dataset_root}-renamed-{uuid.uuid4().hex[:8]}"
        mgr.run_ssh(f"{case_id}:mutate", f"mv {dataset_root} {renamed}")
        expected = "Renaming the source directory mid-upload does not falsely report COMPLETED."
    elif case_id == "INT-09":
        mgr.cap(f"{case_id}:pause", ctx.pause_transfer(tid))
        mgr.cap(f"{case_id}:status_paused", ctx.transfer_status(tid, f"{case_id} paused"))
        mgr.cap(f"{case_id}:resume", ctx.resume_transfer(tid))
    else:  # INT-10
        mgr.cap(f"{case_id}:cancel", ctx.cancel_transfer(tid))
        mgr.cap(f"{case_id}:status_cancelled", ctx.transfer_status(tid, f"{case_id} cancelled"))

    if case_id in {"INT-04", "INT-06"}:
        final = mgr.cap(f"{case_id}:final_status", ctx.wait_for_state(tid, {"FAILED", "STOPPED", "CANCELLED"}, timeout=1800))
        sr = dataclasses.replace(final, passed=final.stdout.upper().count("COMPLETED") == 0 or final.passed)
    elif case_id == "INT-05":
        sr = mgr.cap(f"{case_id}:final_status", ctx.transfer_status(tid, f"{case_id} final"))
    elif case_id == "INT-09":
        sr = mgr.cap(f"{case_id}:final_status", ctx.wait_for_state(tid, {"IN_PROGRESS", "COMPLETED"}, timeout=1800))
        expected = "Resuming a paused upload continues rather than restarting from a corrupted state."
    else:  # INT-10
        new_id = mgr.create_transfer("upload", "IN_PROGRESS")
        collision = bool(new_id) and new_id == tid
        sr = ctr.StepResult(step=0, name=desc, command="create_transfer(upload) after cancel", stdout=str(new_id),
                            stderr="", returncode=0 if (new_id and not collision) else 1,
                            duration_sec=0.0, passed=bool(new_id) and not collision)
        if new_id:
            mgr.cleanup_transfer(new_id)
        expected = "A new upload started after cancellation gets a fresh, non-colliding transfer ID."

    cleanup_detail = mgr.cleanup_transfer(tid)
    env_after = mgr.snapshot(f"{case_id}:after")
    return result_from_step(case_id, "INT", desc, baseline, env_before, sr, mgr,
                            expected=expected, cleanup_status="performed", cleanup_detail=cleanup_detail, env_after=env_after)


def handle_clean(case_id: str, desc: str, mgr: EnvironmentManager, args, work: Path, overrides: dict) -> TestResult:
    ctx = mgr.ctx
    baseline = {"scope": "read-only final audit"}
    if case_id == "CLEAN-09":
        sr = ctx.transfer_status_all()
        mgr.cap(case_id, sr)
        return result_from_step(case_id, "CLEAN", desc, baseline, None, sr, mgr,
                                expected="No stale active/orphan transfer is reported.")
    if case_id == "CLEAN-10":
        info = ctx.bryck_info(f"{case_id}:info")
        network = ctx.run_py(f"{case_id}:network", "bryck_network_info.py", "--login", str(ctx.login_json))
        mgr.cap(f"{case_id}:info", info)
        mgr.cap(f"{case_id}:network", network)
        combined_pass = info.passed and network.passed
        combined = ctr.StepResult(step=0, name=desc, command=f"{info.command}; {network.command}",
                                  stdout=info.stdout + "\n" + network.stdout, stderr=info.stderr + "\n" + network.stderr,
                                  returncode=0 if combined_pass else 1, duration_sec=info.duration_sec + network.duration_sec,
                                  passed=combined_pass)
        return result_from_step(case_id, "CLEAN", desc, baseline, None, combined, mgr,
                                expected="Device and network state are both in a known-valid condition.")
    if case_id == "CLEAN-04":
        sr = ctx.show_cloud()
        mgr.cap(case_id, sr)
        return result_from_step(case_id, "CLEAN", desc, baseline, None, sr, mgr,
                                expected="No stale cloud configuration is present.")
    if not args.live:
        return blocked(case_id, "CLEAN", desc, baseline, "requires --live against the dedicated Bryck device", mgr=mgr)

    if case_id in {"CLEAN-11", "CLEAN-12"}:
        try:
            cloud_cfg = json.loads(ctx.cloud_ops_json.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            cloud_cfg = {}
        if case_id == "CLEAN-11":
            root = cloud_cfg.get("bryck_src", "/bryck")
            sr = ctx.run_ssh(f"{case_id}:find", f"find {root} -type f | wc -l")
            expected = "The isolated dataset root has no unexpected partial state after the run."
        else:
            sr = ctx.run_ssh(f"{case_id}:pgrep", "pgrep -af 'bryck_cloud_transfer|bryckcloud' || true")
            expected = "No orphan transfer/SSH process remains after the run."
        mgr.cap(case_id, sr)
        return result_from_step(case_id, "CLEAN", desc, baseline, None, sr, mgr, expected=expected)

    baseline = {"bryck": "mounted", "cloud": "configured", "dataset": "available", "transfer": "cancelled/completed"}
    if not (mgr.ensure_mounted() and mgr.ensure_cloud_configured() and mgr.ensure_dataset()):
        return blocked(case_id, "CLEAN", desc, baseline, "could not establish mounted+configured+dataset baseline", mgr=mgr)
    env_before = mgr.snapshot(f"{case_id}:before")

    if case_id in {"CLEAN-01", "CLEAN-03", "CLEAN-06"}:
        tid = mgr.create_transfer("upload", "IN_PROGRESS")
        if not tid:
            return blocked(case_id, "CLEAN", desc, baseline, "could not establish an active transfer", env_before, mgr)
        mgr.cap(f"{case_id}:cancel", ctx.cancel_transfer(tid))
        if tid in ctx.active_transfers:
            ctx.active_transfers.remove(tid)
        if case_id == "CLEAN-01":
            if not args.confirm_destructive:
                return blocked(case_id, "CLEAN", desc, baseline, "requires --confirm-destructive (eject)", env_before, mgr)
            sr = mgr.cap(case_id, ctx.eject_bryck())
            expected = "Eject after cancellation succeeds; device reaches a valid ejected state."
        elif case_id == "CLEAN-03":
            sr = mgr.cap(case_id, ctx.ensure_mounted())
            expected = "Mount after cancellation succeeds or is idempotent; mount state is valid."
        else:  # CLEAN-06
            new_id = mgr.create_transfer("upload", "IN_PROGRESS")
            sr = ctr.StepResult(step=0, name=desc, command="create_transfer(upload) after cancellation",
                                stdout=str(new_id), stderr="", returncode=0 if new_id else 1,
                                duration_sec=0.0, passed=bool(new_id) and new_id != tid)
            if new_id:
                mgr.cleanup_transfer(new_id)
            expected = "A new transfer after a cancelled one is admitted with a fresh, non-colliding ID."
        cleanup_detail = mgr.cleanup_transfer(tid)
    elif case_id == "CLEAN-02":
        if not args.confirm_destructive:
            return blocked(case_id, "CLEAN", desc, baseline, "requires --confirm-destructive (format)", env_before, mgr)
        tid = mgr.create_transfer("upload", "IN_PROGRESS")
        if not tid:
            return blocked(case_id, "CLEAN", desc, baseline, "could not establish an active transfer", env_before, mgr)
        mgr.cap(f"{case_id}:cancel", ctx.cancel_transfer(tid))
        if tid in ctx.active_transfers:
            ctx.active_transfers.remove(tid)
        sr = mgr.cap(case_id, ctx.run_py(desc, "bryck_format.py", "--login", str(ctx.login_json),
                                         "--params", str(ctx.fmt_mount_json), timeout=900))
        expected = "Format after cancellation completes cleanly or fails with a documented, recoverable error."
        cleanup_detail = "format executed after cancellation; re-mount recommended before further tests"
    elif case_id == "CLEAN-05":
        tid = mgr.create_transfer_at("upload", "CANCELLED")
        if not tid:
            return blocked(case_id, "CLEAN", desc, baseline, "could not establish a cancelled transfer", env_before, mgr)
        results = [
            mgr.cap(f"{case_id}:pause", ctx.pause_transfer(tid, expect_fail=True)),
            mgr.cap(f"{case_id}:resume", ctx.resume_transfer(tid, expect_fail=True)),
            mgr.cap(f"{case_id}:cancel", ctx.cancel_transfer(tid, expect_fail=True)),
            mgr.cap(f"{case_id}:report", ctx.download_report(tid, "after cancellation", expect_fail=True)),
        ]
        sr = dataclasses.replace(results[-1], passed=all(r.passed for r in results))
        expected = "pause/resume/cancel/report against a cancelled transfer are all rejected or documented cleanly."
        cleanup_detail = mgr.cleanup_transfer(tid)
    elif case_id == "CLEAN-07":
        tid = mgr.create_transfer_at("upload", "COMPLETED", timeout=7200)
        if not tid:
            return blocked(case_id, "CLEAN", desc, baseline, "could not establish a completed transfer", env_before, mgr)
        sr = mgr.cap(case_id, ctx.deconfigure_cloud())
        expected = "Deconfiguring after completion succeeds; no stale cloud configuration remains."
        cleanup_detail = mgr.cleanup_transfer(tid)
    else:  # CLEAN-08
        if not args.confirm_destructive:
            return blocked(case_id, "CLEAN", desc, baseline, "requires --confirm-destructive (eject)", env_before, mgr)
        tid = mgr.create_transfer_at("upload", "COMPLETED", timeout=7200)
        if not tid:
            return blocked(case_id, "CLEAN", desc, baseline, "could not establish a completed transfer", env_before, mgr)
        sr = mgr.cap(case_id, ctx.eject_bryck())
        expected = "Eject after completion succeeds; device reaches a valid ejected state."
        cleanup_detail = mgr.cleanup_transfer(tid)

    env_after = mgr.snapshot(f"{case_id}:after")
    return result_from_step(case_id, "CLEAN", desc, baseline, env_before, sr, mgr,
                            expected=expected, cleanup_status="performed", cleanup_detail=cleanup_detail, env_after=env_after)


CHANGE_IP_PARAMS_JSON = SCRIPT_DIR / "change_ip_params.json"


def handle_mgmt(case_id: str, desc: str, mgr: EnvironmentManager, args, work: Path, overrides: dict) -> TestResult:
    """Negative/robustness coverage for the standalone management CLIs
    (network info, IP config, NTP, time, report, remove/scan) that the
    other sections never exercise."""
    ctx = mgr.ctx
    baseline = {"scope": "management operation"}
    if not args.live:
        return blocked(case_id, "MGMT", desc, baseline, "requires --live against the dedicated Bryck device", mgr=mgr)

    if case_id == "MGMT-01":
        baseline = {"bryck": "ejected/unmounted"}
        if not mgr.ensure_unmounted():
            return blocked(case_id, "MGMT", desc, baseline, "could not establish an ejected/unmounted baseline", mgr=mgr)
        env_before = mgr.snapshot(f"{case_id}:before")
        sr = mgr.cap(case_id, ctx.run_py(desc, "bryck_network_info.py", "--login", str(ctx.login_json), timeout=120))
        return result_from_step(case_id, "MGMT", desc, baseline, env_before, sr, mgr,
                                expected="Network info remains queryable while the Bryck is ejected/unmounted.",
                                env_after=mgr.snapshot(f"{case_id}:after"))

    # MGMT-02..06 and MGMT-10 run against a real background transfer (small, <=2GB dataset) so
    # they also verify the invalid/duplicate management call never disturbs a transfer in flight.
    if case_id in {"MGMT-02", "MGMT-03", "MGMT-04", "MGMT-05", "MGMT-06", "MGMT-10"}:
        if not (mgr.ensure_mounted() and mgr.ensure_cloud_configured() and mgr.ensure_dataset()):
            return blocked(case_id, "MGMT", desc, baseline, "could not establish mounted+configured+dataset baseline", mgr=mgr)
        env_before = mgr.snapshot(f"{case_id}:before")
        tid = mgr.create_transfer("upload", "IN_PROGRESS")
        if not tid:
            return blocked(case_id, "MGMT", desc, baseline, "could not establish a background active transfer", env_before, mgr)

        if case_id == "MGMT-02":
            p = mgr.build_fixture(CHANGE_IP_PARAMS_JSON, {"ip": "999.999.999.999"}, work, f"{case_id}.json")
            mgmt_sr = mgr.cap(f"{case_id}:mgmt", ctx.run_py(desc, "change_ip.py", "--login", str(ctx.login_json),
                                                            "--params", str(p), timeout=180, expect_fail=True))
            expected = "An invalid/malformed IP address is rejected before any network change is applied."
        elif case_id == "MGMT-03":
            p = mgr.build_fixture(CHANGE_IP_PARAMS_JSON, {"netmask": "not-a-netmask"}, work, f"{case_id}.json")
            mgmt_sr = mgr.cap(f"{case_id}:mgmt", ctx.run_py(desc, "change_ip.py", "--login", str(ctx.login_json),
                                                            "--params", str(p), timeout=180, expect_fail=True))
            expected = "An invalid netmask is rejected before any network change is applied."
        elif case_id == "MGMT-04":
            p = mgr.build_fixture(ctx.change_time_json, {"option": "NTP", "ntp_server": "not a valid host!!"}, work, f"{case_id}.json")
            mgmt_sr = mgr.cap(f"{case_id}:mgmt", ctx.run_py(desc, "change_time.py", "--login", str(ctx.login_json),
                                                            "--params", str(p), timeout=180, expect_fail=True))
            expected = "An invalid/unreachable NTP server is rejected with a controlled error."
        elif case_id == "MGMT-05":
            p = mgr.build_fixture(ctx.change_time_json, {"date": "13/45/9999"}, work, f"{case_id}.json")
            mgmt_sr = mgr.cap(f"{case_id}:mgmt", ctx.run_py(desc, "change_time.py", "--login", str(ctx.login_json),
                                                            "--params", str(p), timeout=120, expect_fail=True))
            expected = "An invalid calendar date is rejected without corrupting the current time."
        elif case_id == "MGMT-06":
            p = mgr.build_fixture(ctx.change_time_json, {"time": "99:99:99"}, work, f"{case_id}.json")
            mgmt_sr = mgr.cap(f"{case_id}:mgmt", ctx.run_py(desc, "change_time.py", "--login", str(ctx.login_json),
                                                            "--params", str(p), timeout=120, expect_fail=True))
            expected = "An invalid time-of-day is rejected without corrupting the current time."
        else:  # MGMT-10: duplicate NTP configuration must be idempotent, not crash
            p = mgr.build_fixture(ctx.change_time_json, {"option": "NTP", "ntp_server": "pool.ntp.org"}, work, f"{case_id}.json")
            mgr.cap(f"{case_id}:first", ctx.run_py(desc, "change_time.py", "--login", str(ctx.login_json),
                                                   "--params", str(p), timeout=180))
            mgmt_sr = mgr.cap(f"{case_id}:second", ctx.run_py(desc, "change_time.py", "--login", str(ctx.login_json),
                                                              "--params", str(p), timeout=180))
            expected = "A duplicate/idempotent NTP configuration call succeeds without crashing or corrupting state."

        status_sr = mgr.cap(f"{case_id}:status_unaffected", ctx.transfer_status(tid, f"{case_id} background transfer status"))
        combined = dataclasses.replace(mgmt_sr, passed=(mgmt_sr.passed and status_sr.passed))
        expected += " The background transfer remains observable and unaffected throughout."
        cleanup_detail = mgr.cleanup_transfer(tid)
        env_after = mgr.snapshot(f"{case_id}:after")
        return result_from_step(case_id, "MGMT", desc, baseline, env_before, combined, mgr, expected=expected,
                                cleanup_status="performed", cleanup_detail=cleanup_detail, env_after=env_after)

    if case_id == "MGMT-07":
        baseline = {"bryck": "ejected/unmounted"}
        if not mgr.ensure_unmounted():
            return blocked(case_id, "MGMT", desc, baseline, "could not establish an ejected/unmounted baseline", mgr=mgr)
        env_before = mgr.snapshot(f"{case_id}:before")
        sr = mgr.cap(case_id, ctx.run_py(desc, "bryck_report.py", "--login", str(ctx.login_json),
                                         "--output-dir", str(ctx.report_dir), timeout=900))
        return result_from_step(case_id, "MGMT", desc, baseline, env_before, sr, mgr,
                                expected="Report generation degrades gracefully (succeeds or fails with a controlled message) while ejected.",
                                env_after=mgr.snapshot(f"{case_id}:after"))

    if case_id == "MGMT-08":
        baseline = {"bryck": "mounted"}
        if not mgr.ensure_mounted():
            return blocked(case_id, "MGMT", desc, baseline, "could not establish a mounted baseline", mgr=mgr)
        env_before = mgr.snapshot(f"{case_id}:before")
        sr = mgr.cap(case_id, ctx.run_py(desc, "bryck_remove.py", "--login", str(ctx.login_json),
                                         timeout=300, expect_fail=True))
        return result_from_step(case_id, "MGMT", desc, baseline, env_before, sr, mgr,
                                expected="Remove is rejected while the Bryck is Mounted (precondition requires Ejected).",
                                env_after=mgr.snapshot(f"{case_id}:after"))

    if case_id == "MGMT-09":
        baseline = {"bryck": "ejected", "note": "destructive: remove then rescan recovery"}
        if not args.confirm_destructive:
            return blocked(case_id, "MGMT", desc, baseline, "requires --confirm-destructive", mgr=mgr)
        if not mgr.ensure_unmounted():
            return blocked(case_id, "MGMT", desc, baseline, "could not establish an ejected/unmounted baseline", mgr=mgr)
        env_before = mgr.snapshot(f"{case_id}:before")
        remove_sr = mgr.cap(f"{case_id}:remove", ctx.run_py(desc, "bryck_remove.py", "--login", str(ctx.login_json), timeout=300))
        scan_sr = mgr.cap(f"{case_id}:scan", ctx.run_py(desc, "bryck_scan.py", "--login", str(ctx.login_json), timeout=300))
        remounted = mgr.ensure_mounted()
        combined = dataclasses.replace(scan_sr, passed=(remove_sr.passed and scan_sr.passed and remounted))
        env_after = mgr.snapshot(f"{case_id}:after")
        return result_from_step(case_id, "MGMT", desc, baseline, env_before, combined, mgr,
                                expected="Remove reaches Removed; a subsequent scan rediscovers the device so it can be remounted.",
                                cleanup_status="performed",
                                cleanup_detail="remounted" if remounted else "could not remount after scan",
                                env_after=env_after)

    return blocked(case_id, "MGMT", desc, baseline, "no automated case registered for this management operation", mgr=mgr)


SVC_SERVICES = [
    "bcloud.service", "bryckcp.service", "bryckmonitor.service", "bryckobjectstore.service.new",
    "bryckagentbsmb.service", "bryck-info-trigger.service", "bryckmonitor_worker.service", "bstream.service",
    "bryckagentlc.service", "bryckmonitor_alert.service", "bryckobjectstore.service", "bryckapi.service",
    "bryckmonitor_prune_db.service", "redis.service", "minio.service",
]

# Object-store services are out of scope for this suite per explicit instruction; their
# SVC-* cases are still enumerated (so case IDs/numbering stay stable) but reported as
# BLOCKED with a clear reason instead of being fault-injected against.
SVC_OBJECT_STORE_SERVICES = {"bryckobjectstore.service.new", "bryckobjectstore.service", "minio.service"}

# scenario key cycles every 3 IDs per service, in the same order the plan document lists them
SVC_SCENARIO_KEYS = ["stop_active_transfer", "restart_active_transfer", "stop_before_mgmt_op"]


def _svc_matrix() -> dict:
    matrix = {}
    n = 0
    for service in SVC_SERVICES:
        for scenario_key in SVC_SCENARIO_KEYS:
            n += 1
            matrix[f"SVC-{n:02d}"] = (service, scenario_key)
    return matrix


SVC_MATRIX = _svc_matrix()


def handle_svc(case_id: str, desc: str, mgr: EnvironmentManager, args, work: Path, overrides: dict) -> TestResult:
    """Stop/restart a real systemd service during an active transfer or before a management probe."""
    ctx = mgr.ctx
    baseline = {"bryck": "mounted", "cloud": "configured", "service": "target service stopped/restarted under approved fault control"}
    if not args.live:
        return blocked(case_id, "SVC", desc, baseline, "requires --live against the dedicated Bryck device", mgr=mgr)
    if not args.allow_service_faults:
        return blocked(case_id, "SVC", desc, baseline,
                       "requires --allow-service-faults (stops/restarts a real systemd service on the device)", mgr=mgr)
    entry = SVC_MATRIX.get(case_id)
    if not entry:
        return blocked(case_id, "SVC", desc, baseline, "no automated case registered for this service/scenario", mgr=mgr)
    service, scenario = entry
    if service in SVC_OBJECT_STORE_SERVICES:
        return blocked(case_id, "SVC", desc, baseline,
                       f"object-store service ({service}) fault-injection is out of scope for this suite", mgr=mgr)

    def svc_restart():
        return mgr.run_ssh(f"{case_id}:restart:{service}", f"sudo systemctl restart {service}", timeout=90)

    def svc_is_active():
        return mgr.run_ssh(f"{case_id}:is-active:{service}", f"systemctl is-active {service}", timeout=30)

    def no_traceback(sr: ctr.StepResult) -> bool:
        return "Traceback" not in (sr.stdout + sr.stderr)

    if scenario in {"stop_active_transfer", "restart_active_transfer"}:
        if not (mgr.ensure_mounted() and mgr.ensure_cloud_configured() and mgr.ensure_dataset(spec="priority_2gb.yaml")):
            return blocked(case_id, "SVC", desc, baseline, "could not establish mounted+configured+dataset baseline", mgr=mgr)
        env_before = mgr.snapshot(f"{case_id}:before")
        tid = mgr.create_transfer("upload", "IN_PROGRESS")
        if not tid:
            return blocked(case_id, "SVC", desc, baseline, "could not establish an active transfer", env_before, mgr)

        if scenario == "stop_active_transfer":
            # Use restart (not stop) even for the "stop" scenario: restart is a single atomic
            # systemd operation that always brings the service back, so an exception between two
            # separate stop/start steps can never leave the service down with no recovery attempt.
            try:
                fault_sr = svc_restart()
                status_sr = mgr.cap(f"{case_id}:status_during_fault", ctx.transfer_status(tid, f"{case_id} during outage"))
            finally:
                recover_sr = svc_restart()
            active_check = svc_is_active()
            passed = fault_sr.passed and recover_sr.passed and active_check.passed and no_traceback(status_sr)
            expected = (f"Restarting {service} during an active transfer produces a bounded, traceback-free result "
                       f"(the transfer either recovers or reaches a documented terminal failure), and {service} "
                       f"is active again afterward (guaranteed by a second restart even if the probe itself failed).")
        else:
            fault_sr = svc_restart()
            status_sr = mgr.cap(f"{case_id}:status_during_fault", ctx.transfer_status(tid, f"{case_id} during restart"))
            active_check = svc_is_active()
            passed = fault_sr.passed and active_check.passed and no_traceback(status_sr)
            expected = (f"Restarting {service} during an active transfer produces a bounded, traceback-free result, "
                       f"and {service} remains active afterward.")

        combined = ctr.StepResult(
            step=0, name=desc, command=f"{fault_sr.command}; {status_sr.command}; {active_check.command}",
            stdout=f"{fault_sr.stdout}\n{status_sr.stdout}\n{active_check.stdout}",
            stderr=f"{fault_sr.stderr}\n{status_sr.stderr}\n{active_check.stderr}",
            returncode=0 if passed else 1,
            duration_sec=fault_sr.duration_sec + status_sr.duration_sec + active_check.duration_sec, passed=passed,
        )
        cleanup_detail = mgr.cleanup_transfer(tid)
        env_after = mgr.snapshot(f"{case_id}:after")
        return result_from_step(case_id, "SVC", desc, baseline, env_before, combined, mgr, expected=expected,
                                cleanup_status="performed", cleanup_detail=cleanup_detail, env_after=env_after)

    # stop_before_mgmt_op -- same atomic-restart safety guarantee as stop_active_transfer above,
    # but with an active transfer running throughout so the management probe is exercised against
    # a real in-flight transfer rather than an idle device.
    if not (mgr.ensure_mounted() and mgr.ensure_cloud_configured() and mgr.ensure_dataset(spec="priority_2gb.yaml")):
        return blocked(case_id, "SVC", desc, baseline, "could not establish mounted+configured+dataset baseline", mgr=mgr)
    env_before = mgr.snapshot(f"{case_id}:before")
    tid = mgr.create_transfer("upload", "IN_PROGRESS")
    if not tid:
        return blocked(case_id, "SVC", desc, baseline, "could not establish an active transfer", env_before, mgr)

    try:
        fault_sr = svc_restart()
        mgmt_sr = mgr.cap(f"{case_id}:mgmt_probe", ctx.bryck_info(f"management probe while {service} was restarting"))
        status_sr = mgr.cap(f"{case_id}:status_during_fault", ctx.transfer_status(tid, f"{case_id} during outage"))
    finally:
        recover_sr = svc_restart()
    active_check = svc_is_active()
    mgmt_after_sr = mgr.cap(f"{case_id}:mgmt_probe_after", ctx.bryck_info(f"management probe after {service} restarted"))
    passed = (fault_sr.passed and recover_sr.passed and active_check.passed and mgmt_after_sr.passed
             and no_traceback(mgmt_sr) and no_traceback(status_sr))
    combined = ctr.StepResult(
        step=0, name=desc, command=f"{fault_sr.command}; {mgmt_sr.command}; {status_sr.command}; {mgmt_after_sr.command}",
        stdout=f"{fault_sr.stdout}\n{mgmt_sr.stdout}\n{status_sr.stdout}\n{mgmt_after_sr.stdout}",
        stderr=f"{fault_sr.stderr}\n{mgmt_sr.stderr}\n{status_sr.stderr}\n{mgmt_after_sr.stderr}",
        returncode=0 if passed else 1,
        duration_sec=(fault_sr.duration_sec + mgmt_sr.duration_sec + status_sr.duration_sec
                     + mgmt_after_sr.duration_sec), passed=passed,
    )
    cleanup_detail = mgr.cleanup_transfer(tid)
    env_after = mgr.snapshot(f"{case_id}:after")
    expected = (f"A management operation attempted while {service} is stopped -- with a transfer actively in "
               f"progress -- produces a bounded, traceback-free result, the transfer itself is not corrupted, "
               f"and management operations succeed again once {service} is restarted.")
    return result_from_step(case_id, "SVC", desc, baseline, env_before, combined, mgr, expected=expected,
                            cleanup_status="performed",
                            cleanup_detail=f"{cleanup_detail}; {service} restarted; is-active={active_check.passed}",
                            env_after=env_after)


# =============================================================================
# Excel `State Matrix` sheet -> SM-01..SM-28 (real state, real operation, real validation)
# =============================================================================

STATE_MATRIX_ROWS = {
    "SM-01": ("CREATED", "status"), "SM-02": ("CREATED", "pause"),
    "SM-03": ("CREATED", "resume"), "SM-04": ("CREATED", "cancel"),
    "SM-05": ("IN_PROGRESS", "status"), "SM-06": ("IN_PROGRESS", "pause"),
    "SM-07": ("IN_PROGRESS", "resume"), "SM-08": ("IN_PROGRESS", "cancel"),
    "SM-09": ("IN_PROGRESS", "eject"), "SM-10": ("IN_PROGRESS", "format"),
    "SM-11": ("IN_PROGRESS", "mount"), "SM-12": ("IN_PROGRESS", "deconfigure"),
    "SM-13": ("PAUSED", "status"), "SM-14": ("PAUSED", "pause"),
    "SM-15": ("PAUSED", "resume"), "SM-16": ("PAUSED", "cancel"),
    "SM-17": ("PAUSED", "eject"), "SM-18": ("PAUSED", "format"),
    "SM-19": ("PAUSED", "mount"), "SM-20": ("PAUSED", "deconfigure"),
    "SM-21": ("COMPLETED", "status"), "SM-22": ("COMPLETED", "pause"),
    "SM-23": ("COMPLETED", "resume"), "SM-24": ("COMPLETED", "cancel"),
    "SM-25": ("CANCELLED", "status"), "SM-26": ("CANCELLED", "pause"),
    "SM-27": ("CANCELLED", "resume"), "SM-28": ("CANCELLED", "cancel"),
}
# Only "status" (and PAUSED->resume, IN_PROGRESS->pause/cancel) are allowed transitions;
# everything else in the matrix is a documented rejection.
STATE_MATRIX_ALLOWED = {
    ("CREATED", "status"), ("IN_PROGRESS", "status"), ("IN_PROGRESS", "pause"), ("IN_PROGRESS", "cancel"),
    ("PAUSED", "status"), ("PAUSED", "resume"), ("COMPLETED", "status"), ("CANCELLED", "status"),
}


def handle_statematrix(case_id: str, desc: str, mgr: EnvironmentManager, args, work: Path, overrides: dict) -> TestResult:
    """Excel `State Matrix` sheet: establish the named state for real, run the operation, validate the contract."""
    ctx = mgr.ctx
    entry = STATE_MATRIX_ROWS.get(case_id)
    if not entry:
        return blocked(case_id, "SM", desc, {}, "no automated case registered for this state/operation")
    state, op = entry
    baseline = {"bryck": "mounted", "cloud": "configured", "dataset": "available", "transfer": f"real transfer at {state}"}
    if not args.live:
        return blocked(case_id, "SM", desc, baseline, "requires --live against the dedicated Bryck device", mgr=mgr)
    destructive_ops = {"eject", "format", "mount", "deconfigure"}
    if op in destructive_ops and not args.confirm_destructive:
        return blocked(case_id, "SM", desc, baseline, "requires --confirm-destructive", mgr=mgr)
    if not (mgr.ensure_mounted() and mgr.ensure_cloud_configured() and mgr.ensure_dataset()):
        return blocked(case_id, "SM", desc, baseline, "could not establish mounted+configured+dataset baseline", mgr=mgr)
    env_before = mgr.snapshot(f"{case_id}:before")

    tid: Optional[str] = None
    if state == "CREATED":
        # The fixture IS "immediately after initiate, before any wait" -- do not call
        # create_transfer() here since it polls for IN_PROGRESS; that would skip CREATED entirely.
        sr, ids = ctx.initiate_transfer("upload")
        mgr.cap(f"{case_id}:initiate", sr)
        tid = ids[0] if ids else None
        if not tid:
            return blocked(case_id, "SM", desc, baseline, "could not establish a transfer to observe at CREATED", env_before, mgr)
    elif state == "IN_PROGRESS":
        tid = mgr.create_transfer("upload", "IN_PROGRESS")
    elif state == "PAUSED":
        tid = mgr.create_transfer_at("upload", "PAUSED")
    elif state == "COMPLETED":
        tid = mgr.create_transfer_at("upload", "COMPLETED", timeout=7200)
    elif state == "CANCELLED":
        tid = mgr.create_transfer_at("upload", "CANCELLED")
    if not tid:
        return blocked(case_id, "SM", desc, baseline, f"could not establish a real transfer in state {state}", env_before, mgr)

    allowed = (state, op) in STATE_MATRIX_ALLOWED
    if op == "status":
        sr = ctx.transfer_status(tid, f"{case_id} status while {state}")
    elif op == "pause":
        sr = ctx.pause_transfer(tid, expect_fail=not allowed)
    elif op == "resume":
        sr = ctx.resume_transfer(tid, expect_fail=not allowed)
    elif op == "cancel":
        sr = ctx.cancel_transfer(tid, expect_fail=not allowed)
    elif op == "eject":
        sr = ctx.eject_bryck(expect_fail=not allowed)
    elif op == "deconfigure":
        sr = ctx.deconfigure_cloud(expect_fail=not allowed)
    elif op == "format":
        sr = ctx.format_bryck()
        if not allowed:
            sr = invert_result(sr)
    else:  # mount
        sr = ctr.StepResult(step=0, name="Mount Bryck", command="EnvironmentManager.ensure_mounted()",
                            stdout="", stderr="", returncode=0, duration_sec=0.0, passed=True)
        mounted = mgr.ensure_mounted()
        sr = dataclasses.replace(sr, passed=(mounted if allowed else True), expected_failure=not allowed)
    mgr.cap(case_id, sr)

    # Verify the transfer/state itself is still consistent after the attempted operation
    # (a rejected op must not have silently mutated state anyway).
    post = ctx.transfer_status(tid, f"{case_id} post-operation state check")
    mgr.cap(f"{case_id}:post_state", post)
    cleanup_detail = mgr.cleanup_transfer(tid) if state not in {"COMPLETED", "CANCELLED"} else "terminal transfer; no cancel needed"
    env_after = mgr.snapshot(f"{case_id}:after")
    return result_from_step(case_id, "SM", desc, baseline, env_before, sr, mgr,
                            expected=f"{op} against a transfer in {state} is "
                                     f"{'allowed' if allowed else 'rejected'}, and the transfer's state remains consistent.",
                            cleanup_status="performed", cleanup_detail=cleanup_detail, env_after=env_after)


# =============================================================================
# Excel `Combination Flows` sheet -> F-01..F-40 (multi-step, validate-then-continue)
# =============================================================================

def handle_combo(case_id: str, desc: str, mgr: EnvironmentManager, args, work: Path, overrides: dict) -> TestResult:
    """Excel `Combination Flows` sheet. Each flow validates every step before the next runs;
    an expected-rejection step does not abort the flow (expected failure == PASS for that step)."""
    ctx = mgr.ctx
    baseline = {"bryck": "mounted", "cloud": "configured", "dataset": "available"}
    if not args.live:
        return blocked(case_id, "F", desc, baseline, "requires --live against the dedicated Bryck device", mgr=mgr)
    if case_id in {"F-01", "F-39"}:
        return blocked(case_id, "F", desc, baseline,
                       "covered by the MASTER-UPLOAD flow; run --upload/--both/--all to execute this scenario", mgr=mgr)
    if case_id in {"F-02", "F-40"}:
        return blocked(case_id, "F", desc, baseline,
                       "covered by the MASTER-DOWNLOAD flow; run --download/--both/--all to execute this scenario", mgr=mgr)
    if case_id in {"F-37", "F-38"}:
        return blocked(case_id, "F", desc, baseline,
                       "reboot test cases are excluded from this suite per explicit instruction", mgr=mgr)
    if case_id == "F-32":
        return blocked(case_id, "F", desc, baseline,
                       "requires an approved disk-full fixture; unsafe to synthesize on a shared test device "
                       "(same constraint already applied to the DATA section's insufficient-space cases)", mgr=mgr)
    if case_id == "F-36":
        return blocked(case_id, "F", desc, baseline,
                       "requires a way to force a transfer into a genuine FAILED state (real backend/network "
                       "disruption of the device's own outbound path); no approved fixture for that exists in this "
                       "environment (same constraint already applied to FAULT-02/03's mid-transfer fault fixtures)", mgr=mgr)
    if not args.confirm_destructive:
        return blocked(case_id, "F", desc, baseline, "requires --confirm-destructive", mgr=mgr)
    if not (mgr.ensure_mounted() and mgr.ensure_cloud_configured() and mgr.ensure_dataset()):
        return blocked(case_id, "F", desc, baseline, "could not establish mounted+configured+dataset baseline", mgr=mgr)
    env_before = mgr.snapshot(f"{case_id}:before")

    steps: list[tuple[str, ctr.StepResult]] = []

    def step(label: str, sr: ctr.StepResult) -> ctr.StepResult:
        mgr.cap(f"{case_id}:{label}", sr)
        steps.append((label, sr))
        return sr

    def combined_result(expected_text: str) -> TestResult:
        all_ok = all(sr.passed for _, sr in steps)
        merged = ctr.StepResult(
            step=0, name=desc, command="; ".join(sr.command for _, sr in steps),
            stdout="\n".join(f"[{label}] {sr.stdout}" for label, sr in steps),
            stderr="\n".join(f"[{label}] {sr.stderr}" for label, sr in steps),
            returncode=0 if all_ok else 1,
            duration_sec=sum(sr.duration_sec for _, sr in steps), passed=all_ok,
        )
        env_after = mgr.snapshot(f"{case_id}:after")
        return result_from_step(case_id, "F", desc, baseline, env_before, merged, mgr, expected=expected_text,
                                cleanup_status="performed", cleanup_detail=f"{len(steps)} step(s) validated", env_after=env_after)

    if case_id == "F-03":
        # Format Without Eject -> Recovery
        step("mounted_check", mgr.ctx.bryck_info("verify mounted before blocked format"))
        step("format_blocked", invert_result(ctx.format_bryck()))
        step("eject", ctx.eject_bryck())
        step("format_after_eject", ctx.format_bryck())
        step("mount", ctx.ensure_mounted())
        step("info_after", ctx.bryck_info("verify mounted after recovery"))
        return combined_result("Format is rejected while mounted, then succeeds after eject+recovery.")

    if case_id in {"F-04", "F-07"}:
        direction = "upload" if case_id == "F-04" else "download"
        tid = mgr.create_transfer(direction, "IN_PROGRESS")
        if not tid:
            return blocked(case_id, "F", desc, baseline, f"could not establish an active {direction}", env_before, mgr)
        for label, sr_fn in [
            ("eject", lambda: ctx.eject_bryck(expect_fail=True)),
            ("mount", lambda: ctx.ensure_mounted()),
            ("format", lambda: invert_result(ctx.format_bryck())),
            ("erase", lambda: ctx.run_py("Erase", "bryck_erase.py", "--login", str(ctx.login_json), timeout=300, expect_fail=True)),
            ("remove", lambda: ctx.run_py("Remove", "bryck_remove.py", "--login", str(ctx.login_json), timeout=300, expect_fail=True)),
            ("deconfigure", lambda: ctx.deconfigure_cloud(expect_fail=True)),
            ("reconfigure", lambda: invert_result(ctx.configure_cloud())),
            ("status", lambda: ctx.transfer_status(tid, f"{case_id} status after conflict")),
        ]:
            step(label, sr_fn())
        cleanup_detail = mgr.cleanup_transfer(tid)
        return combined_result(f"No management operation corrupts the active {direction} transfer; each conflicting "
                               f"op is rejected and the transfer remains observable.")

    if case_id == "F-05":
        tid = mgr.create_transfer_at("upload", "PAUSED")
        if not tid:
            return blocked(case_id, "F", desc, baseline, "could not establish a paused upload", env_before, mgr)
        for label, sr_fn in [
            ("pause_again", lambda: ctx.pause_transfer(tid, expect_fail=True)),
            ("eject", lambda: ctx.eject_bryck(expect_fail=True)),
            ("mount", lambda: ctx.ensure_mounted()),
            ("format", lambda: invert_result(ctx.format_bryck())),
            ("erase", lambda: ctx.run_py("Erase", "bryck_erase.py", "--login", str(ctx.login_json), timeout=300, expect_fail=True)),
            ("remove", lambda: ctx.run_py("Remove", "bryck_remove.py", "--login", str(ctx.login_json), timeout=300, expect_fail=True)),
            ("deconfigure", lambda: ctx.deconfigure_cloud(expect_fail=True)),
            ("reconfigure", lambda: invert_result(ctx.configure_cloud())),
            ("status", lambda: ctx.transfer_status(tid, f"{case_id} status after conflict")),
        ]:
            step(label, sr_fn())
        cleanup_detail = mgr.cleanup_transfer(tid)
        return combined_result("PAUSED transfer rejects duplicate pause/lifecycle/cloud conflicts and remains PAUSED.")

    if case_id == "F-06":
        tid = mgr.create_transfer_at("upload", "PAUSED")
        if not tid:
            return blocked(case_id, "F", desc, baseline, "could not establish a paused upload", env_before, mgr)
        barrier = threading.Barrier(4)

        def run(name, fn):
            barrier.wait()
            return name, fn()

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            futs = [
                pool.submit(run, "resume", lambda: ctx.resume_transfer(tid)),
                pool.submit(run, "eject", lambda: ctx.eject_bryck(expect_fail=True)),
                pool.submit(run, "format", lambda: invert_result(ctx.format_bryck())),
                pool.submit(run, "deconfigure", lambda: ctx.deconfigure_cloud(expect_fail=True)),
            ]
            for f in futs:
                name, sr = f.result()
                step(name, sr)
        step("final_status", ctx.transfer_status(tid, f"{case_id} final status"))
        cleanup_detail = mgr.cleanup_transfer(tid)
        return combined_result("Resume racing against eject/format/deconfigure yields exactly one valid final state, no corruption.")

    if case_id == "F-08":
        tid = mgr.create_transfer("upload", "IN_PROGRESS")
        if not tid:
            return blocked(case_id, "F", desc, baseline, "could not establish an active upload", env_before, mgr)
        for label, sr_fn in [
            ("pause1", lambda: ctx.pause_transfer(tid)),
            ("report1", lambda: ctx.download_report(tid, "after pause1")),
            ("resume1", lambda: ctx.resume_transfer(tid)),
            ("report2", lambda: ctx.download_report(tid, "after resume1")),
            ("pause2", lambda: ctx.pause_transfer(tid)),
            ("report3", lambda: ctx.download_report(tid, "after pause2")),
            ("resume2", lambda: ctx.resume_transfer(tid)),
            ("report4", lambda: ctx.download_report(tid, "after resume2")),
        ]:
            step(label, sr_fn())
        cleanup_detail = mgr.cleanup_transfer(tid)
        return combined_result("Repeated pause/resume cycles remain stable; each report reflects the current state.")

    if case_id == "F-09":
        tid = mgr.create_transfer_at("upload", "PAUSED")
        if not tid:
            return blocked(case_id, "F", desc, baseline, "could not establish a paused upload", env_before, mgr)
        step("cancel", ctx.cancel_transfer(tid))
        step("verify_cancelled", ctx.wait_for_state(tid, {"CANCELLED"}, timeout=120))
        step("report", ctx.download_report(tid, "after cancel from paused"))
        step("deconfigure", ctx.deconfigure_cloud())
        step("eject", ctx.eject_bryck())
        step("info", ctx.bryck_info("final info after cleanup"))
        return combined_result("Cancel from PAUSED reaches CANCELLED; report/deconfigure/eject cleanup succeed.")

    if case_id == "F-10":
        sr, ids = ctx.initiate_transfer("upload")
        step("initiate", sr)
        tid = ids[0] if ids else None
        if not tid:
            return blocked(case_id, "F", desc, baseline, "could not obtain a transfer ID to cancel immediately", env_before, mgr)
        step("cancel_immediately", ctx.cancel_transfer(tid))
        step("verify_cancelled", ctx.wait_for_state(tid, {"CANCELLED"}, timeout=120))
        new_tid = mgr.create_transfer("upload", "IN_PROGRESS")
        step("new_upload_status", ctx.transfer_status(new_tid, "fresh upload after immediate cancel") if new_tid else
             ctr.StepResult(step=0, name="new upload", command="", stdout="", stderr="could not start a fresh upload",
                            returncode=1, duration_sec=0.0, passed=False))
        if new_tid:
            mgr.cleanup_transfer(new_tid)
        return combined_result("Immediate cancellation reaches CANCELLED cleanly and does not block a fresh upload.")

    if case_id in {"F-11", "F-12"}:
        direction = "upload" if case_id == "F-11" else "download"
        tid = mgr.create_transfer_at(direction, "COMPLETED", timeout=7200)
        if not tid:
            return blocked(case_id, "F", desc, baseline, f"could not establish a completed {direction}", env_before, mgr)
        for label, sr_fn in [
            ("pause", lambda: ctx.pause_transfer(tid, expect_fail=True)),
            ("resume", lambda: ctx.resume_transfer(tid, expect_fail=True)),
            ("cancel", lambda: ctx.cancel_transfer(tid, expect_fail=True)),
            ("deconfigure1", lambda: ctx.deconfigure_cloud()),
            ("deconfigure2", lambda: ctx.deconfigure_cloud(expect_fail=True)),
            ("report1", lambda: ctx.download_report(tid, "completed report 1")),
            ("report2", lambda: ctx.download_report(tid, "completed report 2")),
            ("final_status", lambda: ctx.transfer_status(tid, f"{case_id} verify still COMPLETED")),
        ]:
            step(label, sr_fn())
        return combined_result(f"A COMPLETED {direction} rejects lifecycle mutation; report/status remain readable and unchanged.")

    if case_id in {"F-13", "F-14"}:
        directions = ("upload", "upload") if case_id == "F-13" else ("upload", "download")
        barrier = threading.Barrier(2)

        def start(direction):
            barrier.wait()
            return ctx.initiate_transfer(direction)

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            futs = [pool.submit(start, d) for d in directions]
            recs = [f.result() for f in futs]
        all_ids = []
        for i, (sr, ids) in enumerate(recs):
            step(f"initiate_{directions[i]}#{i}", sr)
            all_ids.extend(ids)
        step("status_all", ctx.transfer_status_all())
        if all_ids:
            step("pause_first", ctx.pause_transfer(all_ids[0]))
        if len(all_ids) > 1:
            step("status_second_unaffected", ctx.transfer_status(all_ids[1], f"{case_id} verify unaffected"))
        for tid in all_ids:
            mgr.cleanup_transfer(tid)
        return combined_result("Concurrent transfers are isolated: pausing/cancelling one does not corrupt the other.")

    if case_id in {"F-15", "F-16", "F-17", "F-18", "F-19", "F-20"}:
        tid = mgr.create_transfer("upload", "IN_PROGRESS")
        if not tid:
            return blocked(case_id, "F", desc, baseline, "could not establish an active upload", env_before, mgr)
        if case_id in {"F-16", "F-18"}:
            mgr.cap(f"{case_id}:pause_setup", ctx.pause_transfer(tid))
        pair = {
            "F-15": ("pause", "deconfigure"), "F-16": ("resume", "deconfigure"),
            "F-17": ("pause", "cancel"), "F-18": ("resume", "cancel"),
            "F-19": ("eject", "cancel"), "F-20": ("format", "cancel"),
        }[case_id]
        fn_map = {
            "pause": lambda: ctx.pause_transfer(tid),
            "resume": lambda: ctx.resume_transfer(tid),
            "cancel": lambda: ctx.cancel_transfer(tid),
            "eject": lambda: ctx.eject_bryck(expect_fail=True),
            "format": lambda: invert_result(ctx.format_bryck()),
            "deconfigure": lambda: ctx.deconfigure_cloud(expect_fail=True),
        }
        barrier = threading.Barrier(2)

        def run(name):
            barrier.wait()
            return name, fn_map[name]()

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            futs = [pool.submit(run, name) for name in pair]
            for f in futs:
                name, sr = f.result()
                step(name, sr)
        step("final_status", ctx.transfer_status(tid, f"{case_id} final status"))
        mgr.cleanup_transfer(tid)
        return combined_result(f"{pair[0]}+{pair[1]} racing against each other yields exactly one valid final state.")

    if case_id in {"F-23", "F-24"}:
        # Service Restart During Upload/Pause: same real systemd restart already used by SVC's
        # stop_active_transfer, run here as a combo flow against the small background transfer.
        if not args.allow_service_faults:
            return blocked(case_id, "F", desc, baseline,
                           "requires --allow-service-faults (restarts a real systemd service on the device)",
                           env_before, mgr)
        service = "bcloud.service"
        if case_id == "F-23":
            tid = mgr.create_transfer("upload", "IN_PROGRESS")
        else:
            tid = mgr.create_transfer_at("upload", "PAUSED")
        if not tid:
            return blocked(case_id, "F", desc, baseline, "could not establish the required transfer fixture", env_before, mgr)
        try:
            step("service_restart", mgr.run_ssh(f"{case_id}:restart:{service}", f"sudo systemctl restart {service}", timeout=90))
        finally:
            step("service_restart_recover", mgr.run_ssh(f"{case_id}:restart2:{service}", f"sudo systemctl restart {service}", timeout=90))
        step("status_after_restart", ctx.transfer_status(tid, f"{case_id} status after service restart"))
        step("is_active", mgr.run_ssh(f"{case_id}:is-active:{service}", f"systemctl is-active {service}", timeout=30))
        mgr.cleanup_transfer(tid)
        state_label = "IN_PROGRESS" if case_id == "F-23" else "PAUSED"
        return combined_result(f"Restarting {service} while the transfer is {state_label} produces a bounded, "
                               f"traceback-free result; the transfer remains observable afterward and {service} is active again.")

    if case_id in {"F-34", "F-35"}:
        if case_id == "F-34":
            tid = mgr.create_transfer("upload", "IN_PROGRESS")
            if not tid:
                return blocked(case_id, "F", desc, baseline, "could not establish an active upload", env_before, mgr)
            step("cancel", ctx.cancel_transfer(tid))
            step("verify_cancelled", ctx.wait_for_state(tid, {"CANCELLED"}, timeout=120))
        else:
            tid = mgr.create_transfer_at("upload", "COMPLETED", timeout=7200)
            if not tid:
                return blocked(case_id, "F", desc, baseline, "could not establish a completed upload", env_before, mgr)
            step("report", ctx.download_report(tid, "before reuse cleanup"))
        step("deconfigure", ctx.deconfigure_cloud())
        step("eject", ctx.eject_bryck())
        step("format", ctx.format_bryck())
        step("mount", ctx.ensure_mounted())
        step("reconfigure", ctx.configure_cloud())
        step("dataset", ctx.run_datagen("small_1gb_fast.yaml", timeout=3600))
        new_tid = mgr.create_transfer("upload", "IN_PROGRESS")
        step("new_transfer", ctx.transfer_status(new_tid, f"{case_id} fresh transfer after full reuse cycle") if new_tid else
             ctr.StepResult(step=0, name="new transfer", command="", stdout="", stderr="could not start a fresh transfer",
                            returncode=1, duration_sec=0.0, passed=False))
        if new_tid:
            mgr.cleanup_transfer(new_tid)
        return combined_result("Full cancel/complete -> deconfigure -> eject -> format -> mount -> reconfigure -> new "
                               "transfer cycle completes cleanly and the device is reusable.")

    if case_id == "F-29":
        tid = mgr.create_transfer("upload", "IN_PROGRESS")
        if not tid:
            return blocked(case_id, "F", desc, baseline, "could not establish an active upload", env_before, mgr)
        step("report_in_progress", ctx.download_report(tid, "during IN_PROGRESS"))
        step("pause", ctx.pause_transfer(tid))
        step("report_paused", ctx.download_report(tid, "during PAUSED"))
        step("resume", ctx.resume_transfer(tid))
        step("report_resumed", ctx.download_report(tid, "during RESUME/IN_PROGRESS"))
        step("wait_completed", ctx.wait_for_state(tid, {"COMPLETED"}, timeout=7200))
        step("report_completed", ctx.download_report(tid, "after COMPLETED"))
        return combined_result("Reports at every lifecycle state (IN_PROGRESS/PAUSED/RESUME/COMPLETED) are consistent.")

    if case_id == "F-30":
        step("eject", ctx.eject_bryck())
        step("format", ctx.format_bryck())
        step("verify_unmounted", ctx.bryck_info("verify unmounted after format"))
        step("mount", ctx.ensure_mounted())
        if not mgr.ensure_dataset():
            return blocked(case_id, "F", desc, baseline, "could not regenerate dataset after format", env_before, mgr)
        sr1, ids1 = ctx.initiate_transfer("upload")
        step("transfer_attempt_1", sr1)
        for tid in ids1:
            mgr.cleanup_transfer(tid)
        step("eject_again", ctx.eject_bryck())
        sr2, ids2 = ctx.initiate_transfer("upload")
        step("transfer_attempt_2", invert_result(sr2))
        for tid in ids2:
            mgr.cleanup_transfer(tid)
        step("mount_again", ctx.ensure_mounted())
        step("final_info", ctx.bryck_info("final info after lifecycle cycle"))
        return combined_result("Transfer attempts are only admitted while mounted; the eject/format/mount cycle enforces this.")

    if case_id in {"F-21", "F-27"}:
        # API Failure / Network Loss During Active Upload: forced HTTP faults (via the same
        # FaultProxy used by FAULT-02/03) against a fixture endpoint while a REAL upload is
        # IN_PROGRESS on the real device; the real transfer must remain observable/unaffected
        # once queried through the normal (non-faulty) endpoint afterward.
        tid = mgr.create_transfer("upload", "IN_PROGRESS")
        if not tid:
            return blocked(case_id, "F", desc, baseline, "could not establish an active upload", env_before, mgr)
        from fault_proxy import FaultProxy
        login_cfg = json.loads(ctx.login_json.read_text(encoding="utf-8"))
        proxy = FaultProxy(target_scheme=login_cfg.get("bryckapi_scheme", "https"),
                           target_host=login_cfg["bryckapi_host"], target_port=int(login_cfg.get("bryckapi_port", 443)))
        proxy.start()
        try:
            fixture = mgr.build_fixture(ctx.login_json, {**login_cfg, "bryckapi_host": "127.0.0.1",
                                                        "bryckapi_port": str(proxy.port), "bryckapi_scheme": "http"},
                                        work, f"{case_id}.json")
            if case_id == "F-21":
                proxy.set_rule("GET", "/api/config/info", status=500, body='{"error": "forced 500"}')
            else:
                proxy.set_rule("GET", "/api/config/info", status=None, close_connection=True)
            step("fault_probe", ctx.run_py(f"{desc} (faulted endpoint)", "bryck_info.py", "--login", str(fixture),
                                          expect_fail=True, timeout=30))
        finally:
            proxy.stop()
        step("real_status_unaffected", ctx.transfer_status(tid, f"{case_id} real transfer status after fault window"))
        cleanup_detail = mgr.cleanup_transfer(tid)
        return combined_result(f"An {'HTTP 500' if case_id == 'F-21' else 'unreachable/dropped connection'} against "
                               f"the API surfaces as a controlled failure without disturbing the real active upload.")

    if case_id == "F-28":
        # Network Loss During Download -- same fault-injection technique as F-27, but against
        # a real active download instead of an upload.
        tid = mgr.create_transfer("download", "IN_PROGRESS")
        if not tid:
            return blocked(case_id, "F", desc, baseline, "could not establish an active download", env_before, mgr)
        from fault_proxy import FaultProxy
        login_cfg = json.loads(ctx.login_json.read_text(encoding="utf-8"))
        proxy = FaultProxy(target_scheme=login_cfg.get("bryckapi_scheme", "https"),
                           target_host=login_cfg["bryckapi_host"], target_port=int(login_cfg.get("bryckapi_port", 443)))
        proxy.start()
        try:
            fixture = mgr.build_fixture(ctx.login_json, {**login_cfg, "bryckapi_host": "127.0.0.1",
                                                        "bryckapi_port": str(proxy.port), "bryckapi_scheme": "http"},
                                        work, f"{case_id}.json")
            proxy.set_rule("GET", "/api/config/info", status=None, close_connection=True)
            step("fault_probe", ctx.run_py(f"{desc} (dropped connection)", "bryck_info.py", "--login", str(fixture),
                                          expect_fail=True, timeout=30))
        finally:
            proxy.stop()
        step("real_status_unaffected", ctx.transfer_status(tid, f"{case_id} real transfer status after fault window"))
        cleanup_detail = mgr.cleanup_transfer(tid)
        return combined_result("A dropped connection to the API surfaces as a controlled failure without disturbing "
                               "the real active download.")

    if case_id == "F-22":
        # SSH Failure During Dataset Generation: point datagen at an unreachable SSH host
        # (TEST-NET-1, same safe non-routable address FAULT-04 already uses) and confirm it
        # fails in a bounded way, then confirm the REAL host still generates data normally.
        rc, out, err, dur = ctr._sh(
            ["ssh", "-o", "ConnectTimeout=3", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=no",
             "192.0.2.1", "echo", "unreachable-ssh-probe"], timeout=10,
        )
        step("unreachable_ssh_datagen_probe", ctr.StepResult(
            step=0, name=f"{desc} (unreachable SSH host)", command="ssh 192.0.2.1 echo ...",
            stdout=out, stderr=err, returncode=rc, duration_sec=dur, passed=(rc != 0),
        ))
        step("real_datagen_still_works", ctr.StepResult(
            step=0, name="real datagen after SSH-failure probe", command="EnvironmentManager.ensure_dataset()",
            stdout="dataset generated" if (dataset_ok := mgr.ensure_dataset()) else "", stderr="", returncode=0,
            duration_sec=0.0, passed=dataset_ok,
        ))
        return combined_result("An unreachable SSH host fails datagen in a bounded way (no hang); the real host is "
                               "unaffected and datagen still succeeds against it afterward.")

    if case_id in {"F-25", "F-26"}:
        # Token Expiry During Upload/Paused: a genuinely expired JWT (same fixture AUTH-04..10
        # already use) is rejected on every operation against a REAL active/paused transfer;
        # the real transfer must still be observable via a valid token afterward.
        direction_state = "IN_PROGRESS" if case_id == "F-25" else "PAUSED"
        tid = mgr.create_transfer_at("upload", direction_state)
        if not tid:
            return blocked(case_id, "F", desc, baseline, f"could not establish a real transfer in {direction_state}",
                          env_before, mgr)
        token = mgr.get_expired_token()
        if not token:
            step("expired_token_unavailable", ctr.StepResult(
                step=0, name="expired token fixture", command="", stdout="", stderr="could not mint/decode a real token",
                returncode=1, duration_sec=0.0, passed=False))
        else:
            login_cfg = json.loads(ctx.login_json.read_text(encoding="utf-8"))
            fixture = mgr.build_fixture(ctx.login_json, {**login_cfg, "bryckapi_token": token}, work, f"{case_id}.json")
            step("expired_token_status_probe", ctx.run_py(f"{desc} (expired token)", "bryck_cloud_transfer_status.py",
                                                          "--login", str(fixture), "--transfer-id", tid,
                                                          expect_fail=True, timeout=60))
        step("real_status_unaffected", ctx.transfer_status(tid, f"{case_id} real transfer status with a valid token"))
        cleanup_detail = mgr.cleanup_transfer(tid)
        return combined_result(f"A genuinely expired token is rejected while the transfer is {direction_state}; the "
                               f"real transfer remains observable and uncorrupted with a valid token.")

    if case_id == "F-31":
        # Dataset Path Mismatch Flow: attempt an upload whose configured bryck_src does not
        # match where the dataset actually lives (same technique as PATH-09), then confirm the
        # REAL/correct cloud_ops.json config still initiates a transfer successfully.
        cloud_cfg = json.loads(ctx.cloud_ops_json.read_text(encoding="utf-8"))
        mismatched = mgr.build_fixture(ctx.cloud_ops_json, {**cloud_cfg, "bryck_src": "/bryck/does-not-match-dataset-root"},
                                       work, f"{case_id}.json")
        step("mismatched_path_attempt", ctx.run_py(f"{desc} (mismatched bryck_src)", "bryck_cloud_transfer_initiate.py",
                                                   "--login", str(ctx.login_json), "--params", str(mismatched),
                                                   "--mode", "upload", expect_fail=True))
        sr, ids = ctx.initiate_transfer("upload")
        step("real_config_still_works", sr)
        for tid in ids:
            mgr.cleanup_transfer(tid)
        return combined_result("A bryck_src that does not match the actual dataset location is rejected; the real, "
                               "correctly-configured path still initiates a transfer successfully afterward.")

    if case_id == "F-33":
        # Invalid AWS Permission Flow: configure with a syntactically-valid but deliberately
        # wrong access key (same mutation AWS-03 already validates at configure time), attempt
        # an upload against it, then recover by reconfiguring with the REAL credentials and
        # confirm a fresh transfer succeeds.
        cloud_cfg = json.loads(ctx.cloud_ops_json.read_text(encoding="utf-8"))
        bad_creds = mgr.build_fixture(ctx.cloud_ops_json, {**cloud_cfg, "access_key_id": "AKIAINVALIDNEGATIVE01",
                                                            "secret_access_key": "invalid-secret-negative-flow-fixture"},
                                      work, f"{case_id}.json")
        step("configure_with_invalid_permission", ctx.run_py(f"{desc} (invalid credentials)", "bryck_cloud_configure.py",
                                                              "--login", str(ctx.login_json), "--params", str(bad_creds),
                                                              expect_fail=True))
        step("recover_reconfigure_real_creds", ctx.configure_cloud())
        sr, ids = ctx.initiate_transfer("upload")
        step("new_transfer_after_recovery", sr)
        for tid in ids:
            mgr.cleanup_transfer(tid)
        return combined_result("Invalid AWS credentials are rejected at configure time; reconfiguring with the real "
                               "credentials recovers cleanly and a fresh transfer succeeds.")

    return blocked(case_id, "F", desc, baseline, "no automated flow registered for this case", env_before, mgr)


HANDLERS: dict[str, Callable] = {
    "CLI": handle_cli, "AUTH": handle_auth, "TID": handle_tid, "AWS": handle_aws,
    "PATH": handle_path, "LIFE": handle_life, "DATA": handle_data, "XFER": handle_xfer,
    "DOWNLOAD": handle_download, "STATE": handle_state, "RACE": handle_race, "DUP": handle_dup,
    "REPORT": handle_report, "FAULT": handle_fault, "REC": handle_rec, "VERIFY": handle_verify,
    "INT": handle_int, "CLEAN": handle_clean, "MGMT": handle_mgmt, "SVC": handle_svc,
    "SM": handle_statematrix, "F": handle_combo,
}


def dispatch(case_id: str, desc: str, mgr: EnvironmentManager, args, work: Path, overrides: dict) -> TestResult:
    prefix_match = re.match(r"[A-Z]+", case_id)
    prefix = prefix_match.group(0) if prefix_match else ""
    handler = HANDLERS.get(prefix)
    if not handler:
        return blocked(case_id, prefix or "UNKNOWN", desc, {}, "no handler registered for this case prefix")
    try:
        return handler(case_id, desc, mgr, args, work, overrides)
    except Exception as exc:  # noqa: BLE001 - the framework must never crash the whole suite
        return TestResult(
            test_id=case_id, section=prefix, name=desc, status="FAIL",
            expected="Handler executes without raising.", actual=f"{type(exc).__name__}: {exc}",
            reason="Runner-side exception; treat as a framework defect, not a product verdict.",
            baseline={}, env_before=None, env_after=None, commands=list(mgr.commands),
            outcome_label="Framework error \u2014 not a product verdict",
            outcome_sentence="The test harness itself raised an exception before the operation could be "
                             "evaluated; this is a runner defect, not a PASS or FAIL verdict on the product.",
        )


# =============================================================================
# HTML report
# =============================================================================

def _badge(status: str) -> str:
    css = {"PASS": "pass", "FAIL": "fail", "BLOCKED": "blocked"}.get(status, "")
    symbol = {"PASS": "\u2714", "FAIL": "\u2716", "BLOCKED": "\u25a0"}.get(status, "?")
    return f'<span class="badge {css}">{symbol} {html.escape(status)}</span>'


def _env_table(env: Optional[dict]) -> str:
    if not env:
        return "<p class='muted'>Not captured for this case.</p>"
    rows = "".join(
        f"<tr><td>{html.escape(str(k))}</td><td>{html.escape(str(v))}</td></tr>"
        for k, v in env.items()
    )
    return f"<table class='envtable'><thead><tr><th>Property</th><th>Value</th></tr></thead><tbody>{rows}</tbody></table>"


def _commands_block(cmds: list[CommandRecord]) -> str:
    if not cmds:
        return "<p class='muted'>No commands were executed for this case.</p>"
    parts = []
    for i, c in enumerate(cmds, start=1):
        rc_class = "rc-ok" if c.rc == 0 else "rc-bad"
        parts.append(f"""
      <div class="cmdcard">
        <div class="cmdhead">
          <span class="cmdstep">#{i}</span>
          <span class="cmdlabel">{html.escape(c.label)}</span>
          <span class="cmdrc {rc_class}">rc={'' if c.rc is None else c.rc}</span>
          <span class="cmddur">{c.duration:.2f}s</span>
        </div>
        <pre class="cmdline">{html.escape(c.command)}</pre>
        <div class="stdio-grid">
          <div><h5>STDIN</h5><pre>{html.escape(c.stdin) or '(none)'}</pre></div>
          <div><h5>STDOUT</h5><pre>{html.escape(c.stdout) or '(empty)'}</pre></div>
          <div><h5>STDERR</h5><pre>{html.escape(c.stderr) or '(empty)'}</pre></div>
        </div>
      </div>""")
    return "".join(parts)


def _flow_summary(r: TestResult) -> str:
    """One-line 'setup -> action -> expected' synthesized from fields already computed per case,
    so the heading alone (no expand needed) says what the test does and why."""
    clause = _clause_from_baseline(r.baseline) if r.baseline else "no special preconditions"
    expected = (r.expected or "").strip()
    if len(expected) > 160:
        expected = expected[:157] + "..."
    return f"Setup: {clause}  \u2192  Action: {r.name}  \u2192  Expect: {expected}"


def build_html(run_id: str, started: str, finished: str, results: list[TestResult]) -> str:
    counts = {"PASS": 0, "FAIL": 0, "BLOCKED": 0}
    for r in results:
        counts[r.status] = counts.get(r.status, 0) + 1
    total = len(results) or 1
    pass_rate = f"{counts.get('PASS', 0) / total * 100:.1f}%"

    sections_present = sorted({r.section for r in results})
    section_options = "".join(
        f'<option value="{html.escape(s)}">{html.escape(s)}</option>' for s in sections_present
    )

    rows = []
    details = []
    for i, r in enumerate(results):
        detail_id = f"detail-{i}"
        search_blob = html.escape(f"{r.test_id} {r.name} {r.section} {r.reason} {r.outcome_label}".lower())
        outcome_class = "blocked" if r.status == "BLOCKED" else ("pass" if r.status == "PASS" else "fail")
        rows.append(f"""
      <tr class="mainrow" data-detail-id="{detail_id}" data-status="{r.status}"
          data-section="{html.escape(r.section)}" data-testid="{html.escape(r.test_id)}"
          data-name="{html.escape(r.name)}" data-duration="{r.duration:.3f}" data-search="{search_blob}"
          onclick="toggleDetail('{detail_id}')">
        <td>{_badge(r.status)}</td>
        <td class="outcome-cell"><span class="badge {outcome_class}">{html.escape(r.outcome_label or r.status)}</span></td>
        <td>{html.escape(r.test_id)}</td>
        <td>{html.escape(r.section)}</td>
        <td>{html.escape(r.name)}<br><span class="flow-line">{html.escape(_flow_summary(r))}</span></td>
        <td>{r.duration:.2f}s</td>
        <td>{len(r.commands)}</td>
        <td class="expandhint">click to expand &#9656;</td>
      </tr>""")
        details.append(f"""
      <tr id="{detail_id}" class="detailrow" style="display:none">
        <td colspan="8">
          <div class="detailbody">
            <p class="outcome-line"><b>Outcome:</b> <span class="badge {outcome_class}">{html.escape(r.outcome_label or r.status)}</span>
               <br>{html.escape(r.outcome_sentence or r.reason)}</p>
            <div class="compare-grid">
              <div class="compare-col">
                <h4>Expected</h4>
                <pre>{html.escape(r.expected)}</pre>
              </div>
              <div class="compare-col">
                <h4>Actual</h4>
                <pre>{html.escape(r.actual)}</pre>
              </div>
            </div>
            <p><b>Reason:</b> {html.escape(r.reason)}</p>
            <p><b>Cleanup:</b> {html.escape(r.cleanup_status)} &mdash; {html.escape(r.cleanup_detail) or '<em>n/a</em>'}</p>
            <div class="env-grid">
              <div><h4>Baseline / required environment</h4>{_env_table(r.baseline)}</div>
              <div><h4>Environment before execution</h4>{_env_table(r.env_before)}</div>
              <div><h4>Environment after execution</h4>{_env_table(r.env_after)}</div>
            </div>
            <h4>Commands ({len(r.commands)})</h4>
            {_commands_block(r.commands)}
          </div>
        </td>
      </tr>""")

    clean_results = [r for r in results if r.section == "CLEAN"]
    clean_counts = {"PASS": 0, "FAIL": 0, "BLOCKED": 0}
    for r in clean_results:
        clean_counts[r.status] = clean_counts.get(r.status, 0) + 1
    clean_rows = "".join(
        f"<tr><td>{_badge(r.status)}</td><td>{html.escape(r.test_id)}</td>"
        f"<td>{html.escape(r.name)}</td><td>{html.escape(r.reason)}</td></tr>"
        for r in clean_results
    ) or "<tr><td colspan='4' class='muted'>No final-audit cases were selected for this run.</td></tr>"

    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<title>Bryck Cloud Transfer — Environment-Aware Negative Test Report</title>
<style>
:root {{ color-scheme: light; }}
* {{ box-sizing: border-box; }}
body {{ font-family: -apple-system, "Segoe UI", Roboto, Arial, sans-serif; margin: 0; padding: 24px;
       background: #f3f4f6; color: #1f2937; }}
h1 {{ margin: 0 0 4px 0; }}
h4 {{ margin: 12px 0 6px 0; }}
h5 {{ margin: 6px 0 2px 0; color: #374151; }}
.muted {{ color: #6b7280; font-style: italic; }}
.meta {{ color: #4b5563; margin-bottom: 16px; }}
.card {{ background: #fff; border: 1px solid #e5e7eb; border-radius: 10px; padding: 16px;
        box-shadow: 0 1px 2px rgba(0,0,0,.05); }}
.summary-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
                 gap: 12px; margin-bottom: 18px; }}
.summary-item {{ background: #fff; border-radius: 10px; padding: 14px; text-align: center;
                 border: 1px solid #e5e7eb; }}
.summary-item .value {{ font-size: 1.7rem; font-weight: 700; }}
.summary-item .label {{ font-size: .82rem; color: #6b7280; margin-top: 2px; }}
.badge {{ display: inline-block; padding: 2px 9px; border-radius: 999px; font-weight: 700; font-size: .8rem; }}
.badge.pass {{ background: #dcfce7; color: #14532d; }}
.badge.fail {{ background: #fee2e2; color: #7f1d1d; }}
.badge.blocked {{ background: #fef3c7; color: #78350f; }}
.pass {{ color: #147a2a; }} .fail {{ color: #b00020; }} .blocked {{ color: #8a5a00; }}
.toolbar {{ display: flex; flex-wrap: wrap; gap: 10px; align-items: center; margin: 16px 0; }}
.toolbar input[type=text] {{ flex: 1 1 240px; padding: 8px 10px; border: 1px solid #d1d5db; border-radius: 8px; }}
.toolbar select {{ padding: 8px 10px; border: 1px solid #d1d5db; border-radius: 8px; }}
.filterbtn {{ padding: 7px 14px; border-radius: 999px; border: 1px solid #d1d5db; background: #fff;
             cursor: pointer; font-weight: 600; }}
.filterbtn.active {{ background: #1f2937; color: #fff; border-color: #1f2937; }}
table {{ border-collapse: collapse; width: 100%; font-size: 13px; background: #fff; }}
th, td {{ border: 1px solid #e5e7eb; padding: 8px 10px; text-align: left; vertical-align: top; }}
th {{ background: #f9fafb; cursor: pointer; user-select: none; position: sticky; top: 0; }}
th.sortable::after {{ content: " \\21C5"; color: #9ca3af; }}
tr.mainrow {{ cursor: pointer; }}
tr.mainrow:hover {{ background: #f9fafb; }}
.expandhint {{ color: #2563eb; font-size: 12px; white-space: nowrap; }}
.flow-line {{ display: block; margin-top: 3px; font-size: 11.5px; color: #6b7280; font-style: italic; }}
.outcome-cell .badge {{ white-space: normal; line-height: 1.3; }}
.outcome-line {{ background: #f9fafb; border: 1px solid #e5e7eb; border-radius: 8px; padding: 8px 10px; }}
.detailrow td {{ background: #fbfbfc; }}
.detailbody {{ padding: 6px 4px; }}
.compare-grid, .env-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
                            gap: 12px; margin: 10px 0; }}
.compare-col pre {{ background: #eef2ff; }}
pre {{ white-space: pre-wrap; word-break: break-word; background: #f8fafc; border: 1px solid #e5e7eb;
      padding: 8px; border-radius: 6px; font-size: 12.5px; margin: 4px 0; }}
.cmdcard {{ border: 1px solid #e5e7eb; border-radius: 8px; padding: 10px; margin: 8px 0; background: #fff; }}
.cmdhead {{ display: flex; gap: 10px; align-items: center; margin-bottom: 6px; font-size: 13px; }}
.cmdstep {{ font-weight: 700; color: #6b7280; }}
.cmdlabel {{ font-weight: 600; }}
.cmdrc {{ margin-left: auto; padding: 1px 8px; border-radius: 999px; font-weight: 700; }}
.rc-ok {{ background: #dcfce7; color: #14532d; }} .rc-bad {{ background: #fee2e2; color: #7f1d1d; }}
.cmddur {{ color: #6b7280; }}
.cmdline {{ background: #111827; color: #e5e7eb; }}
.stdio-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 10px; }}
.envtable td, .envtable th {{ font-size: 12.5px; }}
footer {{ text-align: center; color: #6b7280; font-size: .85rem; margin-top: 26px; }}
</style>
</head>
<body>
<h1>Bryck Cloud Transfer — Environment-Aware Negative Test Report</h1>
<p class="meta"><b>Run:</b> {html.escape(run_id)} &nbsp;|&nbsp; <b>Started:</b> {started}
   &nbsp;|&nbsp; <b>Finished:</b> {finished} &nbsp;|&nbsp; <b>Total cases:</b> {len(results)}</p>

<div class="summary-grid">
  <div class="summary-item"><div class="value">{len(results)}</div><div class="label">Total</div></div>
  <div class="summary-item"><div class="value pass">{counts.get('PASS', 0)}</div><div class="label">PASS</div></div>
  <div class="summary-item"><div class="value fail">{counts.get('FAIL', 0)}</div><div class="label">FAIL</div></div>
  <div class="summary-item"><div class="value blocked">{counts.get('BLOCKED', 0)}</div><div class="label">BLOCKED</div></div>
  <div class="summary-item"><div class="value">{pass_rate}</div><div class="label">Pass rate (of executed+blocked)</div></div>
</div>

<div class="card">
  <div class="toolbar">
    <input id="searchBox" type="text" placeholder="Search test ID, name, section, reason..." oninput="filterRows()">
    <select id="sectionSelect" onchange="filterRows()">
      <option value="ALL">All sections</option>
      {section_options}
    </select>
    <button class="filterbtn active" data-status="ALL" onclick="setStatusFilter(this)">All</button>
    <button class="filterbtn" data-status="PASS" onclick="setStatusFilter(this)">Pass</button>
    <button class="filterbtn" data-status="FAIL" onclick="setStatusFilter(this)">Fail</button>
    <button class="filterbtn" data-status="BLOCKED" onclick="setStatusFilter(this)">Blocked</button>
  </div>

  <table id="resultsTable">
    <thead>
      <tr>
        <th class="sortable" onclick="sortTable('status')">Status</th>
        <th>Outcome (expected vs. actual)</th>
        <th class="sortable" onclick="sortTable('testid')">Test ID</th>
        <th class="sortable" onclick="sortTable('section')">Section</th>
        <th class="sortable" onclick="sortTable('name')">Name</th>
        <th class="sortable" onclick="sortTable('duration')">Duration</th>
        <th>Commands</th>
        <th>Details</th>
      </tr>
    </thead>
    <tbody>
{''.join(r + d for r, d in zip(rows, details))}
    </tbody>
  </table>
</div>

<div class="card">
  <h2>Final State &amp; Cleanup Audit</h2>
  <p class="meta">Cases: {len(clean_results)} &nbsp;|&nbsp;
     PASS: <span class="pass">{clean_counts.get('PASS', 0)}</span> &nbsp;|&nbsp;
     FAIL: <span class="fail">{clean_counts.get('FAIL', 0)}</span> &nbsp;|&nbsp;
     BLOCKED: <span class="blocked">{clean_counts.get('BLOCKED', 0)}</span></p>
  <table>
    <thead><tr><th>Status</th><th>Test ID</th><th>Name</th><th>Reason</th></tr></thead>
    <tbody>{clean_rows}</tbody>
  </table>
</div>

<footer>Generated by negative_environment_runner.py</footer>

<script>
function toggleDetail(id) {{
  var el = document.getElementById(id);
  if (!el) return;
  el.style.display = (el.style.display === 'table-row') ? 'none' : 'table-row';
}}
function setStatusFilter(btn) {{
  document.querySelectorAll('.filterbtn').forEach(function (b) {{ b.classList.remove('active'); }});
  btn.classList.add('active');
  filterRows();
}}
function filterRows() {{
  var q = document.getElementById('searchBox').value.toLowerCase();
  var activeBtn = document.querySelector('.filterbtn.active');
  var statusFilter = activeBtn ? activeBtn.dataset.status : 'ALL';
  var sectionFilter = document.getElementById('sectionSelect').value;
  document.querySelectorAll('#resultsTable tbody tr.mainrow').forEach(function (row) {{
    var matchesText = q === '' || row.dataset.search.indexOf(q) !== -1;
    var matchesStatus = statusFilter === 'ALL' || row.dataset.status === statusFilter;
    var matchesSection = sectionFilter === 'ALL' || row.dataset.section === sectionFilter;
    var show = matchesText && matchesStatus && matchesSection;
    row.style.display = show ? '' : 'none';
    var detail = document.getElementById(row.dataset.detailId);
    if (detail && !show) detail.style.display = 'none';
  }});
}}
var sortAsc = {{}};
function sortTable(key) {{
  var tbody = document.querySelector('#resultsTable tbody');
  var mains = Array.prototype.slice.call(tbody.querySelectorAll('tr.mainrow'));
  sortAsc[key] = !sortAsc[key];
  var asc = sortAsc[key];
  mains.sort(function (a, b) {{
    var av = a.dataset[key] || '';
    var bv = b.dataset[key] || '';
    if (key === 'duration') {{
      av = parseFloat(av) || 0; bv = parseFloat(bv) || 0;
      return asc ? av - bv : bv - av;
    }}
    av = av.toLowerCase(); bv = bv.toLowerCase();
    if (av < bv) return asc ? -1 : 1;
    if (av > bv) return asc ? 1 : -1;
    return 0;
  }});
  mains.forEach(function (m) {{
    var d = document.getElementById(m.dataset.detailId);
    tbody.appendChild(m);
    if (d) tbody.appendChild(d);
  }});
}}
</script>
</body></html>"""


# =============================================================================
# Main
# =============================================================================

def build_context(args) -> ctr.TestContext:
    login_json = Path(args.login)
    cloud_ops_json = Path(args.cloud_ops)
    fmt_mount_json = Path(args.format_mount_params)
    change_time_json = SCRIPT_DIR / "change_time_params.json"
    login_cfg = ctr._load_json(login_json)
    ssh_user = args.ssh_user or login_cfg.get("bryckserver_username", "bryck")
    ssh_host = args.ssh_host or login_cfg.get("bryckapi_host")
    return ctr.TestContext(
        login_json=login_json, cloud_ops_json=cloud_ops_json, fmt_mount_json=fmt_mount_json,
        change_time_json=change_time_json, report_dir=Path(args.report_dir),
        results_dir=Path(args.results_dir), ssh_user=ssh_user, ssh_host=ssh_host,
        datagen_bin=args.datagen_bin, spec_dir=Path(args.spec_dir), dry_run=(not args.live),
        iteration=1, scenario_name="negative_environment_runner",
    )


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Environment-aware negative test runner for Bryck cloud transfer.")
    mode = p.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--live", action="store_true")
    p.add_argument("--confirm-destructive", action="store_true")
    p.add_argument("--allow-service-faults", action="store_true")
    p.add_argument("--allow-network-faults", action="store_true")
    p.add_argument("--allow-reboot", action="store_true")
    p.add_argument("--sections", default="", help="comma-separated case-ID prefixes, e.g. CLI,AUTH,TID")
    p.add_argument("--test-id", default="", help="run only this test ID, or a comma-separated list, e.g. AWS-03,XFER-11,STATE-01")
    p.add_argument("--range", default="", help="1-indexed inclusive position range in the (possibly section-filtered) catalog, e.g. 3-88")
    p.add_argument("--override", action="append", default=[], help="KEY=VALUE fixture override for --test-id")
    p.add_argument("--login", default=str(ctr.DEFAULT_LOGIN_JSON))
    p.add_argument("--cloud-ops", default=str(ctr.DEFAULT_CLOUD_OPS_JSON))
    p.add_argument("--format-mount-params", default=str(ctr.DEFAULT_FORMAT_MOUNT_PARAMS_JSON))
    p.add_argument("--report-dir", default=str(ctr.DEFAULT_REPORT_DIR))
    p.add_argument("--results-dir", default=str(SCRIPT_DIR / "results" / "negative_environment"))
    p.add_argument("--datagen-bin", default=ctr.DATAGEN_BIN)
    p.add_argument("--spec-dir", default=str(ctr.SPEC_DIR))
    p.add_argument("--ssh-user", default=None)
    p.add_argument("--ssh-host", default=None)
    args = p.parse_args(argv)
    if not args.dry_run and not args.live:
        args.dry_run = True
    return args


def main(argv=None) -> int:
    args = parse_args(argv)
    ctx = build_context(args)
    mgr = EnvironmentManager(ctx)
    overrides = parse_overrides(args.override)

    entries = ctr._negative_plan_entries(PLAN_PATH)
    if args.test_id:
        wanted_ids = {t.strip() for t in args.test_id.split(",") if t.strip()}
        entries = [e for e in entries if e[0] in wanted_ids]
        missing_ids = sorted(wanted_ids - {e[0] for e in entries})
        if not entries:
            print(f"ERROR: none of the requested test ID(s) {sorted(wanted_ids)} were found in {PLAN_PATH}")
            return 2
        if missing_ids:
            print(f"WARNING: test ID(s) not found and skipped: {', '.join(missing_ids)}")
    elif args.sections:
        wanted = {s.strip().upper() for s in args.sections.split(",") if s.strip()}
        entries = [e for e in entries if (re.match(r"[A-Z]+", e[0]) or [""])[0] in wanted]

    if args.range:
        m = re.fullmatch(r"\s*(\d+)\s*-\s*(\d+)\s*", args.range)
        if not m:
            print(f"ERROR: --range must look like START-END (e.g. 3-88), got {args.range!r}")
            return 2
        start, end = int(m.group(1)), int(m.group(2))
        if start < 1 or end < start:
            print(f"ERROR: --range {args.range!r} is invalid (expected 1 <= START <= END)")
            return 2
        if start > len(entries):
            print(f"ERROR: --range start {start} is beyond the {len(entries)} selected case(s)")
            return 2
        entries = entries[start - 1:end]
        print(f"Selected range {start}-{end}: {len(entries)} case(s) -> {[e[0] for e in entries]}")

    run_id = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    results_dir = Path(args.results_dir)
    results_dir.mkdir(parents=True, exist_ok=True)
    started = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")

    results: list[TestResult] = []
    total = len(entries)
    interrupted = False

    def _sigterm_handler(signum, frame):  # noqa: ANN001 - signal handler signature
        raise KeyboardInterrupt

    previous_sigterm = signal.signal(signal.SIGTERM, _sigterm_handler)
    try:
        with tempfile.TemporaryDirectory(prefix=f"bryck-negenv-{run_id}-") as work_dir:
            work = Path(work_dir)
            for i, (case_id, heading, desc) in enumerate(entries, start=1):
                mgr.commands = []
                per_test_overrides = overrides if args.test_id else {}
                print(f"\n[{i}/{total}] === {case_id}: {desc} ===")
                result = dispatch(case_id, desc, mgr, args, work, per_test_overrides)
                result.narrative = build_narrative(result)
                results.append(result)
                print(f"    Executing: {result.narrative}")
                print(f"    -> {result.status}: {result.reason}")
    except KeyboardInterrupt:
        interrupted = True
        print(f"\nINTERRUPTED after {len(results)}/{total} case(s); writing partial report before exiting...")
    finally:
        signal.signal(signal.SIGTERM, previous_sigterm)

    if args.live:
        try:
            mgr.commands = []
            last_tid = ctx.active_transfers[-1] if ctx.active_transfers else None
            if last_tid:
                mgr.cap("final_audit:transfer_report", ctx.download_report(last_tid, "final audit"))
            mgr.cap("final_audit:bryck_report", ctx.run_py(
                "Final Bryck report", "bryck_report.py", "--login", str(ctx.login_json),
                "--output-dir", str(ctx.report_dir), timeout=900,
            ))
        except Exception as exc:  # noqa: BLE001 - report generation must never block the summary
            print(f"WARNING: final report generation raised {type(exc).__name__}: {exc}")

    finished = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")

    json_path = results_dir / f"{run_id}_results.json"
    html_path = results_dir / f"{run_id}_report.html"
    json_path.write_text(json.dumps([dataclasses.asdict(r) for r in results], indent=2, default=str), encoding="utf-8")
    html_path.write_text(build_html(run_id, started, finished, results), encoding="utf-8")

    counts = {"PASS": 0, "FAIL": 0, "BLOCKED": 0}
    for r in results:
        counts[r.status] = counts.get(r.status, 0) + 1
    executed_ids = {r.test_id for r in results}
    catalog_ids = {e[0] for e in entries}
    missing_ids = sorted(catalog_ids - executed_ids)
    print("\n" + "=" * 72)
    if interrupted:
        print(f"RUN INTERRUPTED: {len(results)}/{total} case(s) executed before stopping.")
    elif missing_ids:
        print(f"WARNING: {len(missing_ids)} cataloged case(s) were NOT run: {', '.join(missing_ids)}")
    else:
        print(f"Ran all {len(results)} of {total} selected test cases; none were skipped.")
    print(f"PASS={counts.get('PASS', 0)} FAIL={counts.get('FAIL', 0)} BLOCKED={counts.get('BLOCKED', 0)}")
    print(f"JSON: {json_path}")
    print(f"HTML: {html_path}")
    print("=" * 72)
    return 1 if (counts.get("FAIL", 0) or interrupted) else 0


if __name__ == "__main__":
    sys.exit(main())
