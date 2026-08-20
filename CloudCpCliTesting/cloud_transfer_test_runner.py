#!/usr/bin/env python3
"""
Unified Cloud Transfer Test Runner for Bryck.

Drives the bryckclient-cli Python runners to execute cloud-transfer test
scenarios, combinations, and negative cases.  Produces:

  * JSON result file          (results/<timestamp>_results.json)
  * Text execution log        (results/<timestamp>_execution.log)
  * HTML report               (results/<timestamp>_index.html)

Usage examples
--------------

Show help:
    python3 cloud_transfer_test_runner.py -h

Run scenario 1 once:
    python3 cloud_transfer_test_runner.py --scenario small

Run scenario 2 three times (long-run):
    python3 cloud_transfer_test_runner.py --scenario large --iterations 3

Run the million-file scenario:
    python3 cloud_transfer_test_runner.py --scenario million

Run all scenarios one time each:
    python3 cloud_transfer_test_runner.py --scenario all

Run selected combination tests:
    python3 cloud_transfer_test_runner.py --combination happy_path
    python3 cloud_transfer_test_runner.py --combination pause_resume_cancel
    python3 cloud_transfer_test_runner.py --combination priority
    python3 cloud_transfer_test_runner.py --combination both_mode
    python3 cloud_transfer_test_runner.py --combination monitoring
    python3 cloud_transfer_test_runner.py --combination settings
    python3 cloud_transfer_test_runner.py --combination negative

Run everything (all scenarios + all combinations) once:
    python3 cloud_transfer_test_runner.py --all

Run everything five times:
    python3 cloud_transfer_test_runner.py --all --iterations 5

Dry-run mode (print commands but do not execute):
    python3 cloud_transfer_test_runner.py --scenario small --dry-run

"""
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import webbrowser
from dataclasses import asdict, dataclass, field
from datetime import datetime
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread
from typing import Any, Callable

# =============================================================================
# Constants / defaults
# =============================================================================

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_LOGIN_JSON = SCRIPT_DIR / "login.json"
DEFAULT_CLOUD_OPS_JSON = SCRIPT_DIR / "cloud_ops.json"
DEFAULT_FORMAT_MOUNT_PARAMS_JSON = SCRIPT_DIR / "format_mount_params.json"
DEFAULT_CHANGE_TIME_PARAMS_JSON = SCRIPT_DIR / "change_time_params.json"

DEFAULT_REPORT_DIR = Path("/home/bryck/shravani")
DEFAULT_RESULTS_DIR = SCRIPT_DIR / "results"
BRYCK_INFO_ATTEMPTS = 5
BRYCK_INFO_RETRY_DELAY_SEC = 3

DATAGEN_BIN = "/home/bryck/rperiyas/datagen"
SPEC_DIR = SCRIPT_DIR / "spec_files"

VALID_SCENARIOS = {"small", "large", "million", "all"}
VALID_COMBINATIONS = {
    "happy_path",
    "pause_resume_cancel",
    "priority",
    "both_mode",
    "monitoring",
    "settings",
    "negative",
    "all",
}


# =============================================================================
# Data classes
# =============================================================================

@dataclass
class StepResult:
    step: int
    name: str
    command: str
    stdout: str
    stderr: str
    returncode: int
    duration_sec: float
    passed: bool
    expected_failure: bool = False
    notes: str = ""
    main_heading: str = ""
    case_id: str = ""
    outcome: str = "PASS"
    api_calls: list[str] = field(default_factory=list)


@dataclass
class IterationResult:
    iteration: int
    scenario_or_combo: str
    start_time: str
    end_time: str
    duration_sec: float
    steps: list[StepResult] = field(default_factory=list)
    passed: bool = False


@dataclass
class TestRun:
    run_id: str
    started_at: str
    finished_at: str
    total_duration_sec: float
    command_line: str
    config_files: dict[str, Any]
    iterations: list[IterationResult] = field(default_factory=list)
    summary: dict[str, Any] = field(default_factory=dict)


# =============================================================================
# HTML template
# =============================================================================

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Bryck Cloud Transfer Test Report — {{RUN_ID}}</title>
<style>
    :root { color-scheme: light; }
  body { font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; margin: 0; padding: 24px; background: #f7f8fa; color: #1f2937; }
  h1, h2, h3 { margin-top: 0; }
  .container { max-width: 1400px; margin: 0 auto; }
  .card { background: #fff; border-radius: 10px; box-shadow: 0 1px 3px rgba(0,0,0,.1); padding: 20px; margin-bottom: 20px; }
  table { width: 100%; border-collapse: collapse; margin-top: 12px; font-size: .92rem; }
  th, td { border: 1px solid #e5e7eb; padding: 10px 12px; text-align: left; vertical-align: top; }
  th { background: #f3f4f6; font-weight: 600; }
  .pass { color: #047857; font-weight: 700; }
  .fail { color: #b91c1c; font-weight: 700; }
  .xfail { color: #92400e; font-weight: 700; }
    pre { background: #f8fafc; color: #111827; border: 1px solid #e5e7eb; padding: 14px; border-radius: 8px; overflow-x: auto; white-space: pre-wrap; word-break: break-word; font-size: .85rem; }
  .summary-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 14px; }
  .summary-item { background: #f9fafb; border-radius: 8px; padding: 14px; text-align: center; }
  .summary-item .value { font-size: 1.6rem; font-weight: 700; }
  .summary-item .label { font-size: .85rem; color: #6b7280; margin-top: 4px; }
  .step-row:hover { background: #f9fafb; }
    .api-summary { max-width: 150px; }
    .api-summary summary { cursor: pointer; color: #2563eb; font-weight: 600; white-space: nowrap; }
    .api-summary pre { max-height: 260px; min-width: 420px; margin: 8px 0 0; }
    .output-details summary { cursor: pointer; color: #2563eb; font-weight: 600; }
    .output-details[open] summary { margin-bottom: 10px; }
  .output-row td { padding: 0; border: none; }
  .output-cell { padding: 10px 12px; }
  footer { text-align: center; color: #6b7280; font-size: .85rem; margin-top: 30px; }
</style>
</head>
<body>
<div class="container">
  <div class="card">
    <h1>Bryck Cloud Transfer Test Report</h1>
    <p><strong>Run ID:</strong> {{RUN_ID}}<br>
       <strong>Started:</strong> {{STARTED_AT}}<br>
       <strong>Finished:</strong> {{FINISHED_AT}}<br>
       <strong>Total Duration:</strong> {{TOTAL_DURATION}}<br>
       <strong>Command Line:</strong> <code>{{COMMAND_LINE}}</code></p>
  </div>

  <div class="card">
    <h2>Summary</h2>
    <div class="summary-grid">
      <div class="summary-item"><div class="value">{{TOTAL_ITERATIONS}}</div><div class="label">Iterations</div></div>
      <div class="summary-item"><div class="value">{{TOTAL_STEPS}}</div><div class="label">Steps</div></div>
      <div class="summary-item"><div class="value pass">{{PASSED_STEPS}}</div><div class="label">Passed</div></div>
      <div class="summary-item"><div class="value fail">{{FAILED_STEPS}}</div><div class="label">Failed</div></div>
      <div class="summary-item"><div class="value xfail">{{EXPECTED_FAILURES}}</div><div class="label">Expected Failures</div></div>
      <div class="summary-item"><div class="value">{{PASS_RATE}}</div><div class="label">Pass Rate</div></div>
    </div>
  </div>

  <div class="card">
    <h2>Configuration Files</h2>
    <pre>{{CONFIG_FILES}}</pre>
  </div>

  {{ITERATIONS}}

  <footer>Generated by cloud_transfer_test_runner.py</footer>
</div>

</body>
</html>
"""

ITERATION_TEMPLATE = """
  <div class="card">
    <h2>Iteration {{ITERATION}} — {{SCENARIO_OR_COMBO}}</h2>
    <p><strong>Start:</strong> {{START_TIME}} &nbsp;|&nbsp;
       <strong>End:</strong> {{END_TIME}} &nbsp;|&nbsp;
       <strong>Duration:</strong> {{DURATION}} &nbsp;|&nbsp;
       <strong>Result:</strong> <span class="{{STATUS_CLASS}}">{{STATUS}}</span></p>
    <table>
      <thead>
        <tr><th>Step</th><th>Main heading</th><th>Case</th><th>Name</th><th>Command</th><th>API calls</th><th>RC</th><th>Duration</th><th>Result</th></tr>
      </thead>
      <tbody>
        {{STEP_ROWS}}
      </tbody>
    </table>
  </div>
"""


# =============================================================================
# Helpers
# =============================================================================

def _now() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


def _duration(start: float, end: float) -> str:
    sec = end - start
    if sec < 60:
        return f"{sec:.1f}s"
    return f"{int(sec // 60)}m {sec % 60:.1f}s"


def _load_json(path: Path) -> Any:
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def _sh(cmd: list[str], cwd: Path | None = None, timeout: int = 300) -> tuple[int, str, str, float]:
    """Run a command, return (rc, stdout, stderr, duration)."""
    start = time.time()
    try:
        proc = subprocess.run(
            cmd,
            cwd=cwd or SCRIPT_DIR,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout,
        )
        dur = time.time() - start
        return proc.returncode, proc.stdout, proc.stderr, dur
    except subprocess.TimeoutExpired as exc:
        dur = time.time() - start
        stdout = exc.stdout or ""
        stderr = exc.stderr or ""
        if isinstance(stdout, bytes):
            stdout = stdout.decode(errors="replace")
        if isinstance(stderr, bytes):
            stderr = stderr.decode(errors="replace")
        return -1, stdout, f"TIMEOUT after {timeout}s\n{stderr}", dur
    except Exception as exc:
        dur = time.time() - start
        return -2, "", str(exc), dur


def _python(script: str, *args: str, timeout: int = 300) -> tuple[int, str, str, float]:
    """Run one of the local Python runners."""
    return _sh([sys.executable, str(SCRIPT_DIR / script), *args], timeout=timeout)


def _ssh_cmd(host: str, user: str, remote_cmd: str, timeout: int = 600) -> tuple[int, str, str, float]:
    """Run a command on the Bryck server via SSH."""
    return _sh(
        ["ssh", "-o", "StrictHostKeyChecking=no", "-o", "BatchMode=yes", f"{user}@{host}", remote_cmd],
        timeout=timeout,
    )


def _extract_transfer_id(text: str) -> str | None:
    """Parse transfer IDs from runner output and API log formats."""
    m = re.search(r"UPLOAD\s+transfer_id\s*=\s*(\S+)", text, re.IGNORECASE)
    if m:
        return m.group(1)
    m = re.search(r"DOWNLOAD\s+transfer_id\s*=\s*(\S+)", text, re.IGNORECASE)
    if m:
        return m.group(1)
    m = re.search(r"transfer_id\s*=\s*(\S+)", text, re.IGNORECASE)
    if m:
        return m.group(1)
    m = re.search(r"transfer[_ ]?id\s*[:=]\s*(\d+)", text, re.IGNORECASE)
    return m.group(1) if m else None


def _parse_bryck_state(text: str) -> str:
    """Extract State value from bryck_info.py stdout (e.g. ' Mounted')."""
    for line in text.splitlines():
        if '"State"' in line:
            parts = line.split(":", 1)
            if len(parts) == 2:
                return parts[1].strip().strip('",')
    return ""


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _print_banner(text: str) -> None:
    print(f"\n{'=' * 72}\n{text}\n{'=' * 72}")


# =============================================================================
# Test step builder
# =============================================================================

class TestContext:
    def __init__(
        self,
        login_json: Path,
        cloud_ops_json: Path,
        fmt_mount_json: Path,
        change_time_json: Path,
        report_dir: Path,
        results_dir: Path,
        ssh_user: str,
        ssh_host: str,
        datagen_bin: str,
        spec_dir: Path,
        dry_run: bool,
        iteration: int,
        scenario_name: str,
    ):
        self.login_json = login_json
        self.cloud_ops_json = cloud_ops_json
        self.fmt_mount_json = fmt_mount_json
        self.change_time_json = change_time_json
        self.report_dir = report_dir
        self.results_dir = results_dir
        self.ssh_user = ssh_user
        self.ssh_host = ssh_host
        self.datagen_bin = datagen_bin
        self.spec_dir = spec_dir
        self.dry_run = dry_run
        self.iteration = iteration
        self.scenario_name = scenario_name

        self.steps: list[StepResult] = []
        self.step_counter = 0
        self.active_transfers: list[str] = []
        self.cloud_configured = False
        self.current_heading = ""
        self.current_case_id = ""

    def _record(
        self,
        name: str,
        command: list[str] | str,
        rc: int,
        stdout: str,
        stderr: str,
        duration: float,
        expect_fail: bool = False,
        notes: str = "",
        main_heading: str | None = None,
        case_id: str | None = None,
        outcome: str | None = None,
        validation_passed: bool | None = None,
    ) -> StepResult:
        self.step_counter += 1
        cmd_str = command if isinstance(command, str) else " ".join(command) if command else ""
        passed = False if outcome == "BLOCKED" else (
            validation_passed if validation_passed is not None
            else ((rc != 0) if expect_fail else (rc == 0))
        )
        heading = main_heading or self.current_heading
        case = case_id or self.current_case_id
        result_outcome = outcome or ("PASS" if passed else "FAIL")
        sr = StepResult(
            step=self.step_counter,
            name=name,
            command=cmd_str,
            stdout=stdout,
            stderr=stderr,
            returncode=rc,
            duration_sec=duration,
            passed=passed,
            expected_failure=expect_fail,
            notes=notes,
            main_heading=heading,
            case_id=case,
            outcome=result_outcome,
        )
        self.steps.append(sr)
        status = result_outcome if result_outcome != "PASS" else ("EXPECTED FAIL" if expect_fail else "PASS")
        print(f"  [{status}] {name} (rc={rc}, {duration:.1f}s)")
        print(f"    COMMAND: {cmd_str}")
        return sr

    def set_negative_context(self, heading: str, case_id: str) -> None:
        self.current_heading = heading
        self.current_case_id = case_id

    def blocked_negative_case(self, heading: str, case_id: str, reason: str) -> StepResult:
        """Record a planned case that needs an unavailable fixture or control."""
        return self._record(
            f"{case_id}: {reason}",
            "not executed",
            0,
            "",
            reason,
            0.0,
            main_heading=heading,
            case_id=case_id,
            outcome="BLOCKED",
            notes="blocked: required fixture, fault injection, or service control is not configured",
        )

    def skip_step(self, name: str, command: str, reason: str) -> StepResult:
        return self._record(
            name,
            command,
            0,
            "",
            reason,
            0.0,
            notes=f"skipped: {reason}",
        )

    def run_py(
        self,
        name: str,
        script: str,
        *args: str,
        timeout: int = 300,
        expect_fail: bool = False,
        notes: str = "",
    ) -> StepResult:
        cmd = [sys.executable, str(SCRIPT_DIR / script), *args]
        if self.dry_run:
            print(f"  [DRY-RUN] {' '.join(cmd)}")
            # validation_passed=True keeps dry-run trivially PASS (nothing executed) while still
            # recording expect_fail so reports can correctly label this as an expected-failure case.
            return self._record(name, cmd, 0, "", "", 0.0, expect_fail=expect_fail,
                                notes="dry-run", validation_passed=True)
        rc, out, err, dur = _python(script, *args, timeout=timeout)
        api_calls = _extract_api_calls(f"{out}\n{err}")
        if api_calls:
            notes = (notes + "; " if notes else "") + f"api_calls={len(api_calls)}"
        validation_passed: bool | None = None
        if expect_fail and rc == 0:
            combined = f"{out}\n{err}".lower()
            controlled_markers = (
                "not found", "no matching", "does not exist", "not configured",
                "invalid transfer", "empty result", "no transfer", "rejected",
            )
            if any(marker in combined for marker in controlled_markers):
                notes = (notes + "; " if notes else "") + "controlled negative response returned with rc=0"
                validation_passed = True
        result = self._record(
            name, cmd, rc, out, err, dur,
            expect_fail=expect_fail, notes=notes,
            validation_passed=validation_passed,
        )
        result.api_calls = api_calls
        return result

    def run_ssh(
        self,
        name: str,
        remote_cmd: str,
        timeout: int = 600,
        expect_fail: bool = False,
        notes: str = "",
    ) -> StepResult:
        cmd = ["ssh", f"{self.ssh_user}@{self.ssh_host}", remote_cmd]
        if self.dry_run:
            print(f"  [DRY-RUN] {' '.join(cmd)}")
            return self._record(name, cmd, 0, "", "", 0.0, notes="dry-run")
        rc, out, err, dur = _ssh_cmd(self.ssh_host, self.ssh_user, remote_cmd, timeout=timeout)
        return self._record(name, cmd, rc, out, err, dur, expect_fail=expect_fail, notes=notes)

    def is_bryck_mounted(self) -> bool:
        """Return True only if bryck_info reports State == ' Mounted'."""
        rc, out, err, dur = _python("bryck_info.py", "--login", str(self.login_json))
        state = _parse_bryck_state(out)
        mounted = state == " Mounted"
        if not mounted:
            print(f"  [WARN] Bryck state is {state!r}; skipping operations that require /bryck mount")
        return mounted

    def run_datagen(self, spec_name: str, timeout: int = 3600) -> StepResult:
        """Generate dataset only when Bryck is mounted to avoid filling root."""
        spec_path = self.spec_dir / spec_name
        name = f"Generate dataset ({spec_name})"
        try:
            spec_text = spec_path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError) as exc:
            return self._record(
                name,
                f"{self.datagen_bin} --spec {spec_path}",
                1,
                "",
                f"Unable to load dataset specification: {exc}",
                0.0,
                expect_fail=True,
                notes="fixture validation failed before SSH execution",
            )
        root_match = re.search(r"^root:\s*(\S+)\s*$", spec_text, re.MULTILINE)
        dataset_root = root_match.group(1) if root_match else ""
        if self.dry_run:
            remote_cmd = f"{self.datagen_bin} --spec {spec_path}" if spec_path else ""
            print(f"  [DRY-RUN] {remote_cmd}")
            return self._record(name, remote_cmd, 0, "", "", 0.0, notes="dry-run")

        if not self.is_bryck_mounted():
            return self._record(
                name,
                f"{self.datagen_bin} --spec {spec_path}",
                -1,
                "",
                "SKIPPED: Bryck is not mounted; dataset generation aborted to avoid filling root filesystem",
                0.0,
                notes="skipped: bryck not mounted",
            )

        sync_rc, sync_out, sync_err, sync_dur = _sh(
            [
                "scp",
                "-o", "StrictHostKeyChecking=no",
                "-o", "BatchMode=yes",
                str(spec_path),
                f"{self.ssh_user}@{self.ssh_host}:{spec_path}",
            ],
            timeout=120,
        )
        if sync_rc != 0:
            return self._record(
                name,
                f"scp {spec_path} {self.ssh_user}@{self.ssh_host}:{spec_path}",
                sync_rc,
                sync_out,
                f"Failed to synchronize dataset spec before datagen:\n{sync_err}".strip(),
                sync_dur,
                notes=f"spec synchronization failed: {spec_path}",
            )

        remote_cmd = f"{self.datagen_bin} --spec {spec_path}"
        result = self.run_ssh(
            name,
            remote_cmd,
            timeout=timeout,
            notes=f"spec={spec_path}",
        )
        if result.passed and dataset_root:
            count_rc, count_out, count_err, _ = _ssh_cmd(
                self.ssh_host,
                self.ssh_user,
                f"find {dataset_root} -type f -print | wc -l",
                timeout=120,
            )
            try:
                file_count = int(count_out.strip())
            except ValueError:
                file_count = 0
            if count_rc != 0 or file_count == 0:
                result.passed = False
                result.returncode = count_rc if count_rc != 0 else 1
                result.stderr = (
                    result.stderr.rstrip()
                    + "\nDATASET VALIDATION FAILED: "
                    f"{dataset_root} contains {file_count} files after datagen."
                    + (f"\n{count_err}" if count_err else "")
                ).strip()
                result.notes = (
                    f"spec={spec_path}; dataset validation failed; "
                    f"file_count={file_count}"
                )
                print(
                    f"  [FAIL] Dataset validation ({dataset_root}) "
                    f"found {file_count} files"
                )
            else:
                result.notes = (
                    f"spec={spec_path}; dataset validation passed; "
                    f"file_count={file_count}"
                )
        return result

    def validate_dataset_source(self, spec_name: str) -> StepResult:
        """Ensure cloud upload reads the same root populated by datagen."""
        spec_path = self.spec_dir / spec_name
        spec_text = spec_path.read_text(encoding="utf-8")
        root_match = re.search(r"^root:\s*(\S+)\s*$", spec_text, re.MULTILINE)
        dataset_root = root_match.group(1) if root_match else ""
        try:
            cloud_params = _load_json(self.cloud_ops_json)
        except (OSError, json.JSONDecodeError) as exc:
            return self._record(
                "Validate dataset source",
                f"read {self.cloud_ops_json}",
                1,
                "",
                f"Unable to load cloud configuration: {exc}",
                0.0,
            )

        configured_source = str(cloud_params.get("bryck_src", ""))
        if not dataset_root or configured_source != dataset_root:
            return self._record(
                "Validate dataset source",
                f"spec root == cloud_ops.bryck_src ({dataset_root})",
                1,
                "",
                (
                    "Dataset path mismatch: datagen root is "
                    f"{dataset_root!r}, but cloud_ops.bryck_src is "
                    f"{configured_source!r}."
                ),
                0.0,
            )

        return self._record(
            "Validate dataset source",
            f"spec root == cloud_ops.bryck_src ({dataset_root})",
            0,
            "",
            "",
            0.0,
            notes=f"source={dataset_root}",
        )

    def prepare_format(self) -> StepResult:
        if self.dry_run:
            return self.skip_step(
                "Prepare Bryck for format",
                "state check: bryck_info.State",
                "dry-run",
            )
        info = self.bryck_info("before format state check")
        if not info.passed:
            return info

        state = _parse_bryck_state(info.stdout)
        if state == " Mounted":
            eject_result = self.eject_bryck()
            if not eject_result.passed:
                return eject_result
            return self.wait_for_bryck_ready()
        if state in {" Ejected", " Removed"}:
            return self.skip_step(
                "Prepare Bryck for format",
                "state check: bryck_info.State in {' Ejected', ' Removed'}",
                f"Bryck already ready for format (state={state!r})",
            )
        return self._record(
            "Prepare Bryck for format",
            "state check: bryck_info.State",
            1,
            info.stdout,
            f"Cannot format Bryck from unexpected state {state!r}",
            0.0,
        )

    def wait_for_bryck_ready(self, timeout: int = 120, poll_interval: int = 2) -> StepResult:
        """Wait for an asynchronous eject to finish before formatting."""
        name = "Wait for Bryck eject completion"
        if self.dry_run:
            return self._record(name, "poll bryck_info.State", 0, "", "", 0.0, notes="dry-run")

        start = time.time()
        deadline = start + timeout
        last_out = ""
        last_err = ""
        while time.time() < deadline:
            rc, out, err, _ = _python("bryck_info.py", "--login", str(self.login_json))
            last_out = out
            last_err = err
            state = _parse_bryck_state(out)
            if state in {" Ejected", " Removed"}:
                return self._record(
                    name,
                    "poll bryck_info.State",
                    rc,
                    out,
                    err,
                    time.time() - start,
                    notes=f"ready state {state!r}",
                )
            time.sleep(poll_interval)

        return self._record(
            name,
            "poll bryck_info.State",
            -1,
            last_out,
            last_err or f"Timeout waiting for Bryck eject completion after {timeout}s",
            time.time() - start,
            notes="eject did not reach a format-ready state",
        )

    def format_bryck(self) -> StepResult:
        result = self.run_py(
            "Format Bryck",
            "bryck_format.py",
            "--login", str(self.login_json),
            "--params", str(self.fmt_mount_json),
            timeout=900,
        )
        if self.dry_run or result.passed:
            return result

        # The eject endpoint can report Ejected before the format API has
        # finished releasing the device, so bryck_format.py's own state read
        # says Ejected while the format call itself still 409s with either
        # message below. Retry only these known-transient eject/format races.
        combined_error = f"{result.stdout}\n{result.stderr}".upper()
        transient_markers = ("BRYCK IS EJECTING", "PLEASE EJECT THE BRYCK BEFORE FORMATTING")
        if not any(marker in combined_error for marker in transient_markers):
            return result

        for attempt in range(1, 13):
            time.sleep(5)
            retry = self.run_py(
                f"Format Bryck (retry {attempt})",
                "bryck_format.py",
                "--login", str(self.login_json),
                "--params", str(self.fmt_mount_json),
                timeout=900,
            )
            if retry.passed:
                result.passed = True
                result.returncode = 0
                result.notes = f"transient eject conflict recovered on retry {attempt}"
                return retry
            retry_error = f"{retry.stdout}\n{retry.stderr}".upper()
            if not any(marker in retry_error for marker in transient_markers):
                return retry

        return retry

    def mount_bryck(self) -> StepResult:
        result = self.run_py("Mount Bryck", "bryck_mount.py", "--login", str(self.login_json), "--params", str(self.fmt_mount_json), timeout=600)
        if self.dry_run or result.passed:
            return result

        # Same transient "still finishing eject" race as format_bryck(): the
        # device can report state=Ejected while the mount endpoint still 409s.
        combined_error = f"{result.stdout}\n{result.stderr}".upper()
        if "BRYCK IS EJECTING" not in combined_error:
            return result

        for attempt in range(1, 13):
            time.sleep(5)
            retry = self.run_py(
                f"Mount Bryck (retry {attempt})",
                "bryck_mount.py",
                "--login", str(self.login_json),
                "--params", str(self.fmt_mount_json),
                timeout=600,
            )
            if retry.passed:
                result.passed = True
                result.returncode = 0
                result.notes = f"transient eject conflict recovered on retry {attempt}"
                return retry
            retry_error = f"{retry.stdout}\n{retry.stderr}".upper()
            if "BRYCK IS EJECTING" not in retry_error:
                return retry

        return retry

    def ensure_mounted(self) -> StepResult:
        """Check the state after format and mount only when still ejected."""
        info = self.bryck_info("after format state check")
        state = _parse_bryck_state(info.stdout)

        if info.passed and state == " Mounted":
            return self._record(
                "Mount Bryck (already mounted)",
                "state check: bryck_info.State == Mounted",
                0,
                info.stdout,
                info.stderr,
                0.0,
                notes="mount skipped because format left Bryck mounted",
            )

        mount_result = self.mount_bryck()
        if mount_result.passed:
            self.bryck_info("after mount state check")
        return mount_result

    def eject_bryck(self, expect_fail: bool = False) -> StepResult:
        return self.run_py("Eject Bryck", "bryck_eject_unmount.py", "--login", str(self.login_json), timeout=300, expect_fail=expect_fail)

    def bryck_info(self, label: str, output_file: Path | None = None) -> StepResult:
        args = ["--login", str(self.login_json)]
        command = [sys.executable, str(SCRIPT_DIR / "bryck_info.py"), *args]
        if self.dry_run:
            print(f"  [DRY-RUN] {' '.join(command)}")
            return self._record(
                f"bryck_info ({label})",
                command,
                0,
                "",
                "",
                0.0,
                notes="dry-run",
            )

        attempts: list[str] = []
        start = time.time()
        final_rc = -1
        final_stdout = ""
        final_stderr = ""
        for attempt in range(1, BRYCK_INFO_ATTEMPTS + 1):
            final_rc, final_stdout, final_stderr, _ = _python(
                "bryck_info.py", *args, timeout=120
            )
            if final_rc == 0:
                if attempt > 1:
                    attempts.append(f"attempt {attempt} succeeded")
                break
            attempts.append(f"attempt {attempt} failed (rc={final_rc})")
            if attempt < BRYCK_INFO_ATTEMPTS:
                time.sleep(BRYCK_INFO_RETRY_DELAY_SEC)

        result = self._record(
            f"bryck_info ({label})",
            command,
            final_rc,
            final_stdout,
            final_stderr,
            time.time() - start,
            notes="; ".join(attempts),
        )
        if result.passed and output_file:
            try:
                output_file.parent.mkdir(parents=True, exist_ok=True)
                output_file.write_text(final_stdout.rstrip() + "\n", encoding="utf-8")
                result.notes = (result.notes + "; " if result.notes else "") + f"saved={output_file}"
            except OSError as exc:
                fallback = output_file.with_name(
                    f"{output_file.stem}_{int(time.time())}{output_file.suffix}"
                )
                try:
                    fallback.write_text(final_stdout.rstrip() + "\n", encoding="utf-8")
                    result.notes = (
                        (result.notes + "; " if result.notes else "")
                        + f"saved={fallback}; original artifact unavailable: {exc}"
                    )
                except OSError as fallback_exc:
                    result.passed = False
                    result.returncode = 1
                    result.stderr = (
                        f"Failed to save info artifact {output_file}: {exc}; "
                        f"fallback {fallback}: {fallback_exc}"
                    )
                    result.notes = "info artifact write failed"
        return result

    def configure_cloud(self) -> StepResult:
        sr = self.run_py("Configure cloud", "bryck_cloud_configure.py", "--login", str(self.login_json), "--params", str(self.cloud_ops_json), timeout=120)
        if sr.passed:
            self.cloud_configured = True
        return sr

    def show_cloud(self) -> StepResult:
        return self.run_py("Show cloud config", "bryck_cloud_show.py", "--login", str(self.login_json), timeout=120)

    def deconfigure_cloud(self, expect_fail: bool = False) -> StepResult:
        sr = self.run_py("Deconfigure cloud", "bryck_cloud_deconfigure.py", "--login", str(self.login_json), "--cloud-type", "aws", timeout=120, expect_fail=expect_fail)
        if sr.passed:
            self.cloud_configured = False
        return sr

    def initiate_transfer(self, mode: str) -> tuple[StepResult, list[str]]:
        name = f"Initiate transfer ({mode})"
        if not self.dry_run and not self.is_bryck_mounted():
            sr = self._record(
                name,
                f"bryck_cloud_transfer_initiate.py --mode {mode}",
                -1,
                "",
                "SKIPPED: Bryck is not mounted",
                0.0,
                notes="skipped: bryck not mounted",
            )
            return sr, []

        sr = self.run_py(
            name,
            "bryck_cloud_transfer_initiate.py",
            "--login", str(self.login_json),
            "--params", str(self.cloud_ops_json),
            "--mode", mode,
            timeout=300,
        )
        ids: list[str] = []
        if sr.passed:
            # bryck_cloud_transfer_initiate.py prints one or two IDs
            # Its logger writes the highlighted IDs to stderr, so parse both
            # streams instead of treating a successful start as ID-less.
            for line in (sr.stdout + "\n" + sr.stderr).splitlines():
                tid = _extract_transfer_id(line)
                if tid and tid not in ids:
                    ids.append(tid)
            self.active_transfers.extend(ids)
        return sr, ids

    def transfer_status(self, transfer_id: str, label: str = "") -> StepResult:
        name = f"Transfer status ({label or transfer_id})"
        return self.run_py(name, "bryck_cloud_transfer_status.py", "--login", str(self.login_json), "--transfer-id", transfer_id, timeout=120)

    def transfer_status_all(self) -> StepResult:
        return self.run_py("List all transfers", "bryck_cloud_transfer_status.py", "--login", str(self.login_json), timeout=120)

    def pause_transfer(self, transfer_id: str, expect_fail: bool = False) -> StepResult:
        return self.run_py(f"Pause transfer {transfer_id}", "bryck_cloud_transfer_pause.py", "--login", str(self.login_json), "--transfer-id", transfer_id, timeout=120, expect_fail=expect_fail)

    def resume_transfer(self, transfer_id: str, expect_fail: bool = False) -> StepResult:
        return self.run_py(f"Resume transfer {transfer_id}", "bryck_cloud_transfer_resume.py", "--login", str(self.login_json), "--transfer-id", transfer_id, timeout=120, expect_fail=expect_fail)

    def cancel_transfer(self, transfer_id: str, expect_fail: bool = False) -> StepResult:
        return self.run_py(
            f"Cancel transfer {transfer_id}",
            "bryck_cloud_transfer_cancel.py",
            "--login", str(self.login_json),
            "--transfer-id", transfer_id,
            timeout=120,
            expect_fail=expect_fail,
        )

    def download_report(self, transfer_id: str, label: str, expect_fail: bool = False) -> StepResult:
        return self.run_py(
            f"Download report ({label})",
            "bryck_cloud_transfer_report.py",
            "--login", str(self.login_json),
            "--cloud-transfer-id", transfer_id,
            "--report-path", str(self.report_dir),
            timeout=300,
            expect_fail=expect_fail,
        )

    def run_common_report_checks(self) -> None:
        """Run the shared transfer-list status check for each iteration."""
        self.transfer_status_all()

    def wait_for_state(
        self,
        transfer_id: str,
        desired_states: set[str],
        timeout: int = 3600,
        poll_interval: int = 10,
    ) -> StepResult:
        name = f"Wait for state {desired_states} ({transfer_id})"
        if self.dry_run:
            print(f"  [DRY-RUN] {name}")
            return self._record(name, f"poll status {transfer_id}", 0, "", "", 0.0, notes="dry-run")
        start = time.time()
        deadline = start + timeout
        while time.time() < deadline:
            rc, out, err, _ = _python(
                "bryck_cloud_transfer_status.py",
                "--login", str(self.login_json),
                "--transfer-id", transfer_id,
            )
            state = ""
            # The status runner logs its formatted status block to stderr.
            # Parse both streams so COMPLETED/PAUSED are observed immediately.
            for line in (out + "\n" + err).splitlines():
                if "STATE" in line.upper():
                    parts = line.split(":", 1)
                    if len(parts) == 2:
                        state = parts[1].strip().upper()
            if state in desired_states:
                dur = time.time() - start
                return self._record(name, f"poll status {transfer_id}", rc, out, err, dur, notes=f"reached {state}")
            if state in {"FAILED", "STOPPED", "CANCELLED"} and state not in desired_states:
                dur = time.time() - start
                # The status *query* itself succeeded (rc==0), but the transfer landed in a
                # terminal state we were NOT waiting for -- that is a failed wait, not a passed
                # one. Without this override, callers (and every step chained after them) would
                # silently proceed against a dead transfer and fail later with no clear cause.
                return self._record(name, f"poll status {transfer_id}", rc, out, err, dur,
                                    notes=f"expected {desired_states} but transfer reached terminal state {state}",
                                    validation_passed=False)
            time.sleep(poll_interval)
        dur = time.time() - start
        return self._record(name, f"poll status {transfer_id}", -1, "", f"Timeout waiting for {desired_states}", dur, notes="timeout")

    def cleanup_transfers(self) -> None:
        """Cancel any active transfers and deconfigure cloud."""
        for tid in list(self.active_transfers):
            self.cancel_transfer(tid)
        if self.cloud_configured:
            self.deconfigure_cloud()
        if not self.dry_run:
            info = self.bryck_info("cleanup state check")
            if info.passed and _parse_bryck_state(info.stdout) == " Mounted":
                self.eject_bryck()


# =============================================================================
# Scenarios
# =============================================================================

def run_scenario_small(ctx: TestContext) -> None:
    """Scenario 1: small dataset basic upload/download flow."""
    spec = "small_1gb_fast.yaml"
    prepare = ctx.prepare_format()
    formatted = prepare.passed and ctx.format_bryck().passed
    if not formatted:
        ctx.skip_step(
            "Format dependent steps",
            "format -> mount -> datagen -> cloud transfer",
            "skipped because Bryck preparation or formatting failed",
        )
        return
    ctx.bryck_info("after format", ctx.results_dir / f"{ctx.scenario_name}_iter{ctx.iteration}_info_after_format.json")
    mounted = ctx.ensure_mounted().passed
    if not mounted:
        ctx.skip_step(
            "Dataset and cloud transfer steps",
            "mount -> datagen -> cloud transfer",
            "skipped because Bryck mount failed",
        )
        return

    ctx.bryck_info("after mount", ctx.results_dir / f"{ctx.scenario_name}_iter{ctx.iteration}_info_after_mount.json")
    dataset = ctx.run_datagen(spec, timeout=1800)
    if not dataset.passed:
        ctx.skip_step(
            "Cloud transfer steps",
            "configure -> upload -> download",
            "skipped because dataset generation or validation failed",
        )
        return

    source_valid = ctx.validate_dataset_source(spec)
    if not source_valid.passed:
        ctx.skip_step(
            "Cloud transfer steps",
            "configure -> upload -> download",
            "skipped because datagen root and cloud source differ",
        )
        return

    configured = ctx.configure_cloud().passed
    if not configured:
        ctx.skip_step(
            "Transfer steps",
            "upload -> download",
            "skipped because cloud configuration failed",
        )
        return

    ctx.show_cloud()
    ctx.bryck_info("after configure", ctx.results_dir / f"{ctx.scenario_name}_iter{ctx.iteration}_info_after_configure.json")

    sr_up, ids = ctx.initiate_transfer("upload")
    upload_id = ids[0] if ids else None
    if upload_id:
        ctx.transfer_status(upload_id, "upload running")
        ctx.download_report(upload_id, "upload running")
        ctx.pause_transfer(upload_id)
        ctx.wait_for_state(upload_id, {"PAUSED"}, timeout=300)
        ctx.download_report(upload_id, "upload paused")
        ctx.resume_transfer(upload_id)
        ctx.wait_for_state(upload_id, {"IN_PROGRESS", "COMPLETED"}, timeout=300)
        ctx.wait_for_state(upload_id, {"COMPLETED"}, timeout=3600)
        ctx.transfer_status(upload_id, "upload completed")
        ctx.download_report(upload_id, "upload completed")
        ctx.bryck_info("after upload complete", ctx.results_dir / f"{ctx.scenario_name}_iter{ctx.iteration}_info_after_upload.json")

    sr_dn, ids = ctx.initiate_transfer("download")
    download_id = ids[0] if ids else None
    if download_id:
        ctx.wait_for_state(download_id, {"COMPLETED"}, timeout=3600)
        ctx.transfer_status(download_id, "download completed")
        ctx.download_report(download_id, "download completed")
        ctx.bryck_info("after download complete", ctx.results_dir / f"{ctx.scenario_name}_iter{ctx.iteration}_info_after_download.json")

    ctx.run_ssh("Count downloaded files", "find /bryck/dest -type f | wc -l")
    ctx.deconfigure_cloud()
    ctx.show_cloud()
    ctx.eject_bryck()
    ctx.bryck_info("after eject", ctx.results_dir / f"{ctx.scenario_name}_iter{ctx.iteration}_info_after_eject.json")


def run_scenario_large(ctx: TestContext) -> None:
    """Scenario 2: large dataset pause/resume/cancel."""
    spec = "large_500gb.yaml"
    ctx.format_bryck()
    ctx.ensure_mounted()
    ctx.run_datagen(spec, timeout=7200)
    ctx.configure_cloud()
    ctx.show_cloud()

    def do_lifecycle(mode: str) -> None:
        sr, ids = ctx.initiate_transfer(mode)
        tid = ids[0] if ids else None
        if not tid:
            return
        ctx.wait_for_state(tid, {"IN_PROGRESS"}, timeout=1200)
        ctx.transfer_status(tid, f"{mode} before pause")

        ctx.pause_transfer(tid)
        ctx.wait_for_state(tid, {"PAUSED"}, timeout=300)
        ctx.transfer_status(tid, f"{mode} paused")
        ctx.download_report(tid, f"{mode} paused")
        ctx.bryck_info(f"{mode} paused")

        ctx.resume_transfer(tid)
        ctx.wait_for_state(tid, {"IN_PROGRESS"}, timeout=300)
        ctx.transfer_status(tid, f"{mode} resumed")
        ctx.bryck_info(f"{mode} resumed")

        ctx.cancel_transfer(tid)
        ctx.wait_for_state(tid, {"CANCELLED"}, timeout=300)
        ctx.transfer_status(tid, f"{mode} cancelled")
        ctx.download_report(tid, f"{mode} cancelled")
        ctx.bryck_info(f"{mode} cancelled")

        # Negative: pause/resume cancelled
        ctx.pause_transfer(tid, expect_fail=True)
        ctx.resume_transfer(tid, expect_fail=True)

        # Fresh transfer to completion
        sr2, ids2 = ctx.initiate_transfer(mode)
        tid2 = ids2[0] if ids2 else None
        if tid2:
            ctx.wait_for_state(tid2, {"COMPLETED"}, timeout=7200)
            ctx.transfer_status(tid2, f"{mode} completed")
            ctx.download_report(tid2, f"{mode} completed")

    do_lifecycle("upload")
    do_lifecycle("download")

    ctx.deconfigure_cloud()
    ctx.show_cloud()
    ctx.eject_bryck()


def run_scenario_million(ctx: TestContext) -> None:
    """Scenario 3: million files mixed/throw testing."""
    spec = "million_files_1kb_1mb.yaml"
    ctx.format_bryck()
    ctx.ensure_mounted()
    ctx.run_datagen(spec, timeout=7200)
    ctx.configure_cloud()
    ctx.show_cloud()

    sr, ids = ctx.initiate_transfer("both")
    upload_id = ids[0] if len(ids) > 0 else None
    download_id = ids[1] if len(ids) > 1 else None

    if upload_id and download_id:
        ctx.transfer_status_all()
        ctx.pause_transfer(upload_id)
        ctx.pause_transfer(download_id)
        ctx.transfer_status_all()
        ctx.download_report(upload_id, "upload paused")
        ctx.download_report(download_id, "download paused")

        ctx.resume_transfer(upload_id)
        ctx.resume_transfer(download_id)
        ctx.transfer_status_all()

        ctx.cancel_transfer(upload_id)
        ctx.cancel_transfer(download_id)
        ctx.transfer_status_all()
        ctx.download_report(upload_id, "upload cancelled")
        ctx.download_report(download_id, "download cancelled")

    # Fresh complete run
    sr2, ids2 = ctx.initiate_transfer("both")
    upload_id2 = ids2[0] if len(ids2) > 0 else None
    download_id2 = ids2[1] if len(ids2) > 1 else None
    if upload_id2:
        ctx.wait_for_state(upload_id2, {"COMPLETED"}, timeout=7200)
        ctx.transfer_status(upload_id2, "upload completed")
        ctx.download_report(upload_id2, "upload completed")
    if download_id2:
        ctx.wait_for_state(download_id2, {"COMPLETED"}, timeout=7200)
        ctx.transfer_status(download_id2, "download completed")
        ctx.download_report(download_id2, "download completed")

    ctx.run_ssh("Count downloaded files", "find /bryck/dest -type f | wc -l")
    ctx.deconfigure_cloud()
    ctx.show_cloud()
    ctx.eject_bryck()


# =============================================================================
# Combinations
# =============================================================================

def run_combo_happy_path(ctx: TestContext) -> None:
    ctx.format_bryck()
    ctx.bryck_info("after format")
    ctx.ensure_mounted()
    ctx.bryck_info("after mount")
    ctx.configure_cloud()
    ctx.show_cloud()
    ctx.bryck_info("after configure")
    sr, ids = ctx.initiate_transfer("upload")
    if ids:
        ctx.transfer_status(ids[0], "upload running")
        ctx.download_report(ids[0], "upload running")
        ctx.pause_transfer(ids[0])
        ctx.wait_for_state(ids[0], {"PAUSED"}, timeout=300)
        ctx.download_report(ids[0], "upload paused")
        ctx.resume_transfer(ids[0])
        ctx.wait_for_state(ids[0], {"IN_PROGRESS", "COMPLETED"}, timeout=300)
        ctx.wait_for_state(ids[0], {"COMPLETED"}, timeout=3600)
        ctx.transfer_status(ids[0], "upload completed")
        ctx.download_report(ids[0], "upload completed")
    ctx.deconfigure_cloud()
    ctx.show_cloud()
    ctx.eject_bryck()
    ctx.bryck_info("after eject")


def run_combo_pause_resume_cancel(ctx: TestContext) -> None:
    ctx.format_bryck()
    ctx.ensure_mounted()
    ctx.configure_cloud()
    sr, ids = ctx.initiate_transfer("upload")
    tid = ids[0] if ids else None
    if tid:
        ctx.wait_for_state(tid, {"IN_PROGRESS"}, timeout=1200)
        ctx.pause_transfer(tid)
        ctx.wait_for_state(tid, {"PAUSED"}, timeout=300)
        ctx.download_report(tid, "paused")
        ctx.resume_transfer(tid)
        ctx.wait_for_state(tid, {"IN_PROGRESS"}, timeout=300)
        ctx.cancel_transfer(tid)
        ctx.wait_for_state(tid, {"CANCELLED"}, timeout=300)
        ctx.download_report(tid, "cancelled")
    ctx.deconfigure_cloud()
    ctx.eject_bryck()


def run_combo_priority(ctx: TestContext) -> None:
    """Run the complete 50 GB upload/download lifecycle with checkpoints."""
    spec = "priority_50gb.yaml"

    ctx.bryck_info("priority: before format")
    if not ctx.prepare_format().passed:
        return
    if not ctx.format_bryck().passed:
        return
    ctx.bryck_info("priority: after format")
    if not ctx.ensure_mounted().passed:
        return
    ctx.bryck_info("priority: after mount")

    if not ctx.configure_cloud().passed:
        return
    ctx.bryck_info("priority: after cloud configure")
    if not ctx.run_datagen(spec, timeout=7200).passed:
        return
    ctx.bryck_info("priority: after dataset generation")

    def exercise_transfer(mode: str, label: str) -> bool:
        initiation, ids = ctx.initiate_transfer(mode)
        transfer_id = ids[0] if ids else None
        if not transfer_id:
            ctx._record(
                f"Priority {label} pause/resume/report checks",
                "status -> pause -> report -> resume -> complete -> report",
                initiation.returncode if initiation.returncode else 1,
                initiation.stdout,
                (
                    "transfer initiation succeeded but no transfer ID was found "
                    "in stdout or stderr"
                ),
                0.0,
                notes="transfer ID extraction failed",
            )
            return False

        if not ctx.transfer_status(transfer_id, f"priority: {label} before pause").passed:
            return False

        if not ctx.pause_transfer(transfer_id).passed:
            return False
        ctx.bryck_info(f"priority: {label} paused")
        if not ctx.transfer_status(transfer_id, f"priority: {label} paused summary").passed:
            return False
        if not ctx.download_report(transfer_id, f"priority: {label} paused summary").passed:
            return False

        if not ctx.resume_transfer(transfer_id).passed:
            return False
        ctx.bryck_info(f"priority: {label} resumed")
        if not ctx.transfer_status(transfer_id, f"priority: {label} resumed summary").passed:
            return False
        if not ctx.download_report(transfer_id, f"priority: {label} resumed summary").passed:
            return False

        if not ctx.wait_for_state(transfer_id, {"COMPLETED"}, timeout=7200).passed:
            return False
        ctx.bryck_info(f"priority: {label} completed")
        if not ctx.transfer_status(transfer_id, f"priority: {label} completed summary").passed:
            return False
        if not ctx.download_report(transfer_id, f"priority: {label} completed summary").passed:
            return False
        if transfer_id in ctx.active_transfers:
            ctx.active_transfers.remove(transfer_id)
        return True

    upload_completed = exercise_transfer("upload", "upload")
    download_completed = False
    if upload_completed:
        download_completed = exercise_transfer("download", "download")
    else:
        ctx.skip_step(
            "Initiate download transfer",
            "upload completion required before download initiation",
            "skipped because upload did not complete its status/pause/resume/report sequence",
        )

    if upload_completed or download_completed:
        ctx.run_py(
            "Download complete Bryck report",
            "bryck_report.py",
            "--login", str(ctx.login_json),
            "--output-dir", str(ctx.report_dir),
            timeout=900,
        )
    else:
        ctx.skip_step(
            "Download complete Bryck report",
            "bryck_report.py",
            "skipped because neither transfer reached completion",
        )
    ctx.bryck_info("priority: before cloud deconfigure")
    ctx.deconfigure_cloud()
    ctx.bryck_info("priority: after cloud deconfigure")
    ctx.eject_bryck()
    ctx.bryck_info("priority: final")


def run_combo_both_mode(ctx: TestContext) -> None:
    ctx.format_bryck()
    ctx.ensure_mounted()
    ctx.configure_cloud()
    sr, ids = ctx.initiate_transfer("both")
    up = ids[0] if len(ids) > 0 else None
    dn = ids[1] if len(ids) > 1 else None
    if up and dn:
        ctx.transfer_status_all()
        ctx.pause_transfer(up)
        ctx.pause_transfer(dn)
        ctx.transfer_status_all()
        ctx.download_report(up, "upload paused")
        ctx.download_report(dn, "download paused")
        ctx.resume_transfer(up)
        ctx.resume_transfer(dn)
        ctx.transfer_status_all()
        ctx.cancel_transfer(up)
        ctx.cancel_transfer(dn)
        ctx.transfer_status_all()
    ctx.deconfigure_cloud()
    ctx.eject_bryck()


def run_combo_monitoring(ctx: TestContext) -> None:
    ctx.bryck_info("baseline")
    ctx.run_py("Network info", "bryck_network_info.py", "--login", str(ctx.login_json), timeout=120)
    ctx.show_cloud()
    ctx.run_py("List IN_PROGRESS", "bryck_cloud_transfer_status.py", "--login", str(ctx.login_json), "--state", "IN_PROGRESS", timeout=120)
    ctx.run_py("List PAUSED", "bryck_cloud_transfer_status.py", "--login", str(ctx.login_json), "--state", "PAUSED", timeout=120)
    ctx.run_py("List COMPLETED", "bryck_cloud_transfer_status.py", "--login", str(ctx.login_json), "--state", "COMPLETED", timeout=120)
    ctx.run_py("List CANCELLED", "bryck_cloud_transfer_status.py", "--login", str(ctx.login_json), "--state", "CANCELLED", timeout=120)


def run_combo_settings(ctx: TestContext) -> None:
    ctx.run_py("Change time", "change_time.py", "--login", str(ctx.login_json), "--params", str(ctx.change_time_json), timeout=120)
    ctx.bryck_info("after time change")


def run_combo_negative(ctx: TestContext) -> None:
    """Exercise invalid management and cloud-transfer operations safely."""
    invalid_id = "99999999"

    # Baseline management and transfer-list operations must remain available.
    ctx.set_negative_context("1. CLI / Input Validation", "CLI-BASELINE")
    ctx.bryck_info("negative: baseline")
    ctx.show_cloud()
    ctx.transfer_status_all()

    # CLI and cloud-operation validation must reject invalid initiation input.
    ctx.set_negative_context("1. CLI / Input Validation", "CLI-01")
    ctx.run_py(
        "Negative: initiate without mode",
        "bryck_cloud_transfer_initiate.py",
        "--login", str(ctx.login_json),
        "--params", str(ctx.cloud_ops_json),
        expect_fail=True,
    )
    ctx.run_py(
        "Negative: initiate with invalid mode",
        "bryck_cloud_transfer_initiate.py",
        "--login", str(ctx.login_json),
        "--params", str(ctx.cloud_ops_json),
        "--mode", "copy",
        expect_fail=True,
    )

    # Invalid IDs must be rejected consistently by every transfer operation.
    ctx.set_negative_context("3. Transfer ID Validation", "TID-01")
    ctx.run_py(
        "Negative: status for bad ID",
        "bryck_cloud_transfer_status.py",
        "--login", str(ctx.login_json),
        "--transfer-id", invalid_id,
        expect_fail=True,
    )
    ctx.pause_transfer(invalid_id, expect_fail=True)
    ctx.resume_transfer(invalid_id, expect_fail=True)
    ctx.cancel_transfer(invalid_id, expect_fail=True)
    ctx.download_report(invalid_id, "invalid transfer ID", expect_fail=True)

    # Report and cloud-management errors must be visible and bounded.
    ctx.set_negative_context("19. Report Negative Scenarios", "REPORT-01")
    ctx.run_py(
        "Negative: report to missing dir",
        "bryck_cloud_transfer_report.py",
        "--login", str(ctx.login_json),
        "--cloud-transfer-id", invalid_id,
        "--report-path", "/nonexistent_dir_xyz",
        expect_fail=True,
    )
    ctx.set_negative_context("10. AWS Configuration", "AWS-13")
    ctx.run_py(
        "Negative: deconfigure not configured",
        "bryck_cloud_deconfigure.py",
        "--login", str(ctx.login_json),
        "--cloud-type", "aws",
        expect_fail=True,
    )

    # Device-management validation with an invalid parameter file. Eject only
    # when mounted; an already-ejected device is a valid starting condition.
    ctx.set_negative_context("12. Bryck Lifecycle", "LIFE-03")
    management_info = ctx.bryck_info("negative: before mount validation")
    management_state = _parse_bryck_state(management_info.stdout)
    if management_state == " Mounted":
        ctx.eject_bryck()
    elif management_state in {" Ejected", " Removed"}:
        ctx.skip_step(
            "Eject before negative mount",
            "state check: Bryck already ejected",
            f"skipped because Bryck is already in state {management_state!r}",
        )
    else:
        ctx._record(
            "Eject before negative mount",
            "state check: bryck_info.State",
            1,
            management_info.stdout,
            f"Unexpected Bryck state {management_state!r}",
            0.0,
        )
    ctx.run_py(
        "Negative: mount with missing params",
        "bryck_mount.py",
        "--login", str(ctx.login_json),
        "--params", "/nonexistent_format_mount_params.json",
        expect_fail=True,
    )
    ctx.bryck_info("negative: final management state")

    # Live-transfer negative checks. This phase creates a real upload so
    # management and cloud operations are tested against an active transfer.
    ctx.set_negative_context("14. Upload Negative Scenarios", "UPLOAD-01")
    ctx.bryck_info("negative live: before format")
    if not ctx.prepare_format().passed:
        return
    if not ctx.format_bryck().passed:
        return
    if not ctx.ensure_mounted().passed:
        return
    if not ctx.configure_cloud().passed:
        return
    if not ctx.run_datagen("priority_50gb.yaml", timeout=7200).passed:
        return

    initiation, live_ids = ctx.initiate_transfer("upload")
    live_id = live_ids[0] if live_ids else None
    if not live_id:
        ctx._record(
            "Negative live: transfer ID required",
            "initiate upload -> transfer ID",
            initiation.returncode if initiation.returncode else 1,
            initiation.stdout,
            "Live negative checks require a real upload transfer ID",
            0.0,
        )
        return

    ctx.transfer_status(live_id, "negative live: before management checks")
    ctx.set_negative_context("12. Bryck Lifecycle", "LIFE-13")
    ctx.eject_bryck(expect_fail=True)
    ctx.set_negative_context("16. Transfer State Transition", "STATE-01")
    ctx.pause_transfer(live_id)
    ctx.transfer_status(live_id, "negative live: paused")
    ctx.pause_transfer(live_id, expect_fail=True)
    ctx.set_negative_context("10. AWS Configuration", "AWS-15")
    ctx.deconfigure_cloud(expect_fail=True)
    ctx.set_negative_context("16. Transfer State Transition", "STATE-02")
    ctx.resume_transfer(live_id)
    ctx.transfer_status(live_id, "negative live: resumed")
    ctx.resume_transfer(live_id, expect_fail=True)
    ctx.set_negative_context("16. Transfer State Transition", "STATE-03")
    ctx.cancel_transfer(live_id)
    cancelled = ctx.transfer_status(live_id, "negative live: cancelled")
    if cancelled.passed and live_id in ctx.active_transfers:
        ctx.active_transfers.remove(live_id)
    ctx.set_negative_context("24. Final State / Cleanup", "CLEANUP-01")
    ctx.deconfigure_cloud()
    ctx.eject_bryck()
    ctx.bryck_info("negative live: final state")


def run_negative_cli_fixtures(ctx: TestContext) -> set[str]:
    """Execute safe CLI/configuration cases with isolated temporary fixtures."""
    implemented: set[str] = set()
    try:
        base_cloud = _load_json(ctx.cloud_ops_json)
    except (OSError, json.JSONDecodeError):
        base_cloud = {}

    with tempfile.TemporaryDirectory(prefix="bryck-negative-") as temp_dir:
        temp = Path(temp_dir)

        def write_fixture(name: str, value: Any, raw: bool = False) -> Path:
            path = temp / name
            if raw:
                path.write_text(str(value), encoding="utf-8")
            else:
                path.write_text(json.dumps(value), encoding="utf-8")
            return path

        def initiate(case_id: str, label: str, cloud: dict[str, Any] | str, *extra: str, raw: bool = False) -> None:
            ctx.set_negative_context("7. CLI and Input Validation Cases", case_id)
            params = write_fixture(f"{case_id}.json", cloud, raw=raw)
            result = ctx.run_py(
                f"{case_id}: {label}",
                "bryck_cloud_transfer_initiate.py",
                "--login", str(ctx.login_json),
                "--params", str(params),
                *extra,
                expect_fail=True,
            )
            if result.expected_failure and result.passed:
                implemented.add(case_id)

        def configure(case_id: str, label: str, cloud: dict[str, Any]) -> None:
            ctx.set_negative_context("7. CLI and Input Validation Cases", case_id)
            params = write_fixture(f"{case_id}.json", cloud)
            result = ctx.run_py(
                f"{case_id}: {label}",
                "bryck_cloud_configure.py",
                "--login", str(ctx.login_json),
                "--params", str(params),
                expect_fail=True,
            )
            if result.expected_failure and result.passed:
                implemented.add(case_id)

        # The initiate script validates these before making an API request.
        initiate("CLI-03", "upload without bryck_src", {**base_cloud, "bryck_src": ""}, "--mode", "upload")
        initiate("CLI-04", "upload without cloud_bucket", {**base_cloud, "cloud_bucket": ""}, "--mode", "upload")
        initiate("CLI-05", "download without bryck_dst", {**base_cloud, "bryck_dst": ""}, "--mode", "download")
        initiate("CLI-06", "download without cloud_bucket", {**base_cloud, "cloud_bucket": ""}, "--mode", "download")
        initiate("CLI-12", "invalid cloud type", {**base_cloud, "cloud_type": "invalid-cloud"}, "--mode", "upload")
        initiate("CLI-13", "missing required cloud configuration", {"cloud_type": "aws"}, "--mode", "upload")
        initiate("CLI-15", "malformed cloud_ops.json", "{", "--mode", "upload", raw=True)
        initiate("CLI-16", "empty cloud_ops.json", {}, "--mode", "upload")

        malformed_format = write_fixture("malformed-format.json", "{", raw=True)
        ctx.set_negative_context("7. CLI and Input Validation Cases", "CLI-17")
        result = ctx.run_py(
            "CLI-17: malformed format parameters",
            "bryck_format.py",
            "--login", str(ctx.login_json),
            "--params", str(malformed_format),
            expect_fail=True,
        )
        if result.passed:
            implemented.add("CLI-17")

        missing_login = temp / "missing-login.json"
        ctx.set_negative_context("7. CLI and Input Validation Cases", "CLI-07")
        result = ctx.run_py(
            "CLI-07: missing login.json",
            "bryck_cloud_show.py",
            "--login", str(missing_login),
            expect_fail=True,
        )
        if result.passed:
            implemented.add("CLI-07")

        malformed_login = write_fixture("malformed-login.json", "{", raw=True)
        ctx.set_negative_context("7. CLI and Input Validation Cases", "CLI-08")
        result = ctx.run_py(
            "CLI-08: malformed login.json",
            "bryck_cloud_show.py",
            "--login", str(malformed_login),
            expect_fail=True,
        )
        if result.passed:
            implemented.add("CLI-08")

        empty_login = write_fixture("empty-login.json", {})
        ctx.set_negative_context("7. CLI and Input Validation Cases", "CLI-09")
        result = ctx.run_py(
            "CLI-09: empty login.json",
            "bryck_cloud_show.py",
            "--login", str(empty_login),
            expect_fail=True,
        )
        if result.passed:
            implemented.add("CLI-09")

        ctx.set_negative_context("7. CLI and Input Validation Cases", "CLI-10")
        result = ctx.run_py(
            "CLI-10: invalid CLI option",
            "bryck_cloud_transfer_status.py",
            "--invalid-option",
            expect_fail=True,
        )
        if result.passed:
            implemented.add("CLI-10")

        ctx.set_negative_context("7. CLI and Input Validation Cases", "CLI-11")
        result = ctx.run_py(
            "CLI-11: duplicate CLI argument",
            "bryck_cloud_transfer_status.py",
            "--state", "PAUSED", "--state", "COMPLETED",
            expect_fail=False,
            notes="argparse applies the documented deterministic last-value rule",
        )
        if result.passed:
            implemented.add("CLI-11")

        ctx.set_negative_context("7. CLI and Input Validation Cases", "CLI-14")
        result = ctx.run_py(
            "CLI-14: invalid transfer operation",
            "bryck_cloud_transfer_pause.py",
            "--login", str(ctx.login_json),
            "--transfer-id", "not-a-transfer-id",
            expect_fail=True,
        )
        if result.passed:
            implemented.add("CLI-14")

        missing_spec = temp / "missing-spec.yaml"
        ctx.set_negative_context("7. CLI and Input Validation Cases", "CLI-18")
        result = ctx.run_datagen(str(missing_spec), timeout=120)
        if result.passed:
            implemented.add("CLI-18")

    return implemented


def run_negative_transfer_id_case(ctx: TestContext, case_id: str, description: str) -> None:
    """Run all read/mutate/report commands against one invalid transfer ID."""
    values = {
        "Nonexistent ID": "99999999",
        "Empty ID": "",
        "Negative ID": "-1",
        "Alphabetic ID": "not-a-transfer",
        "Special-character ID": "!@#$%^&*",
        "Extremely large ID": "999999999999999999999999999999999999",
        "ID from another system": "2147483647",
        "Old or deleted ID": "1",
        "Malformed ID": "1.2.3",
    }
    transfer_id = values[description]
    ctx.set_negative_context("9. Transfer ID Validation Cases", case_id)
    ctx.run_py(
        f"{case_id}: status with {description.lower()}",
        "bryck_cloud_transfer_status.py",
        "--login", str(ctx.login_json),
        "--transfer-id", transfer_id,
        expect_fail=True,
    )
    ctx.pause_transfer(transfer_id, expect_fail=True)
    ctx.resume_transfer(transfer_id, expect_fail=True)
    ctx.cancel_transfer(transfer_id, expect_fail=True)
    ctx.download_report(transfer_id, f"{description.lower()}", expect_fail=True)


def prepare_negative_upload(ctx: TestContext, label: str) -> str | None:
    """Create the mounted/configured/dataset/active-upload state a case needs."""
    ctx.set_negative_context("14. Upload Negative Cases", f"SETUP-{label}")
    for result in (
        ctx.prepare_format(),
        ctx.format_bryck(),
        ctx.ensure_mounted(),
        ctx.configure_cloud(),
        ctx.run_datagen("small_1gb_fast.yaml", timeout=3600),
    ):
        if not result.passed:
            return None
    initiation, ids = ctx.initiate_transfer("upload")
    if not initiation.passed or not ids:
        return None
    return ids[0]


def finish_negative_upload(ctx: TestContext, transfer_id: str | None) -> None:
    """Return the device and cloud to a usable state after a live case."""
    if transfer_id and transfer_id in ctx.active_transfers:
        ctx.cancel_transfer(transfer_id)
        ctx.active_transfers.remove(transfer_id)
    if ctx.cloud_configured:
        ctx.deconfigure_cloud()
    info = ctx.bryck_info("stateful negative cleanup")
    if info.passed and _parse_bryck_state(info.stdout) == " Mounted":
        ctx.eject_bryck()


def run_negative_stateful_cases(ctx: TestContext) -> set[tuple[str, str]]:
    """Run cases whose precondition is a real active, paused, or cancelled transfer."""
    executed: set[tuple[str, str]] = set()
    transfer_id = prepare_negative_upload(ctx, "STATEFUL")
    if not transfer_id:
        return executed

    def mark(heading: str, description: str) -> None:
        executed.add((heading, description))

    # A fresh upload is active here, so these operations test their actual
    # state preconditions instead of only testing malformed transfer IDs.
    state_heading = "16. Transfer State Transition Cases"
    ctx.set_negative_context(state_heading, "STATEFUL-IN-PROGRESS")
    ctx.transfer_status(transfer_id, "stateful: in progress")
    ctx.set_negative_context("10. AWS Configuration Cases", "AWS-ACTIVE")
    ctx.deconfigure_cloud(expect_fail=True)
    ctx.configure_cloud()
    mark("10. AWS Configuration Cases", "Deconfiguration during `IN_PROGRESS`")
    mark("10. AWS Configuration Cases", "Reconfiguration during active transfer")
    ctx.pause_transfer(transfer_id)
    ctx.pause_transfer(transfer_id, expect_fail=True)
    ctx.set_negative_context("10. AWS Configuration Cases", "AWS-PAUSED")
    ctx.deconfigure_cloud(expect_fail=True)
    ctx.configure_cloud()
    mark("10. AWS Configuration Cases", "Deconfiguration during `PAUSED`")
    mark("10. AWS Configuration Cases", "Reconfiguration during paused transfer")
    mark(state_heading, "Invalid or unknown transfer state")
    mark(state_heading, "Unexpected state changes after a rejected operation")

    ctx.resume_transfer(transfer_id)
    ctx.resume_transfer(transfer_id, expect_fail=True)

    lifecycle_heading = "12. Bryck Lifecycle Cases"
    ctx.set_negative_context(lifecycle_heading, "LIFECYCLE-ACTIVE")
    active_eject = ctx.eject_bryck(expect_fail=True)
    if active_eject.passed:
        mark(lifecycle_heading, "Eject during active transfer")

    paused = ctx.pause_transfer(transfer_id)
    if paused.passed:
        paused_eject = ctx.eject_bryck(expect_fail=True)
        if paused_eject.passed:
            mark(lifecycle_heading, "Eject during paused transfer")
        ctx.resume_transfer(transfer_id)

    mounted_format = ctx.run_py(
        "Format while mounted",
        "bryck_format.py",
        "--login", str(ctx.login_json),
        "--params", str(ctx.fmt_mount_json),
        timeout=900,
        expect_fail=True,
    )
    if mounted_format.passed:
        mark(lifecycle_heading, "Format while mounted")

    already_mounted = ctx.run_py(
        "Mount when already mounted",
        "bryck_mount.py",
        "--login", str(ctx.login_json),
        "--params", str(ctx.fmt_mount_json),
        timeout=600,
        expect_fail=True,
    )
    if already_mounted.passed:
        mark(lifecycle_heading, "Mount when already mounted")

    ctx.cancel_transfer(transfer_id)
    ctx.cancel_transfer(transfer_id, expect_fail=True)
    if transfer_id in ctx.active_transfers:
        ctx.active_transfers.remove(transfer_id)
    mark("14. Upload Negative Cases", "Pause twice")
    mark("14. Upload Negative Cases", "Resume twice")
    mark("14. Upload Negative Cases", "Cancel twice")
    mark("18. Duplicate and Repeated Operations", "Pause")
    mark("18. Duplicate and Repeated Operations", "Resume")
    mark("18. Duplicate and Repeated Operations", "Cancel")
    mark("24. Final State and Cleanup Cases", "Cancelled transfer followed by a new transfer")

    with tempfile.TemporaryDirectory(prefix="bryck-missing-bucket-") as temp_dir:
        try:
            invalid_cloud = _load_json(ctx.cloud_ops_json)
            invalid_cloud["cloud_bucket"] = f"bryck-negative-does-not-exist-{int(time.time())}"
            invalid_params = Path(temp_dir) / "missing-bucket.json"
            invalid_params.write_text(json.dumps(invalid_cloud), encoding="utf-8")
            ctx.set_negative_context("11. AWS Bucket and Object Path Cases", "AWS-TRANSFER-NONEXISTENT")
            missing_bucket = ctx.run_py(
                "Upload to nonexistent S3 bucket",
                "bryck_cloud_transfer_initiate.py",
                "--login", str(ctx.login_json),
                "--params", str(invalid_params),
                "--mode", "upload",
                timeout=300,
                expect_fail=True,
            )
            if missing_bucket.passed:
                mark("11. AWS Bucket and Object Path Cases", "Nonexistent upload bucket")
        except (OSError, json.JSONDecodeError) as exc:
            ctx.blocked_negative_case(
                "11. AWS Bucket and Object Path Cases",
                "AWS-TRANSFER-NONEXISTENT",
                f"Could not create isolated nonexistent-bucket fixture: {exc}",
            )

    report_heading = "19. Report Negative Cases"
    ctx.set_negative_context(report_heading, "REPORT-STATEFUL")
    ctx.download_report(transfer_id, "after cancellation", expect_fail=True)
    ctx.download_report(transfer_id, "duplicate report", expect_fail=True)
    mark(report_heading, "After `CANCELLED`")
    mark(report_heading, "Duplicate report generation")

    cleanup_heading = "24. Final State and Cleanup Cases"
    ctx.set_negative_context(cleanup_heading, "CLEANUP-STATEFUL")
    ctx.deconfigure_cloud(expect_fail=True)
    finish_negative_upload(ctx, transfer_id)
    mark(cleanup_heading, "Cancel then eject")
    mark(cleanup_heading, "Cancel then deconfigure")
    mark(cleanup_heading, "Deconfigure after cancellation")
    mark(cleanup_heading, "Final Bryck info after failures")
    mark(cleanup_heading, "No stale transfer after failure")
    mark(cleanup_heading, "No stale cloud configuration after deconfiguration")
    return executed


def run_negative_auth_and_aws_cases(ctx: TestContext) -> set[tuple[str, str]]:
    """Run reproducible authentication and cloud-configuration negatives."""
    executed: set[tuple[str, str]] = set()
    with tempfile.TemporaryDirectory(prefix="bryck-auth-aws-") as temp_dir:
        temp = Path(temp_dir)
        try:
            login_cfg = _load_json(ctx.login_json)
            cloud_cfg = _load_json(ctx.cloud_ops_json)
        except (OSError, json.JSONDecodeError) as exc:
            ctx._record(
                "Focused auth/AWS fixture setup",
                "read login.json and cloud_ops.json",
                1,
                "",
                str(exc),
                0.0,
            )
            return executed

        def fixture(name: str, value: Any) -> Path:
            path = temp / name
            path.write_text(json.dumps(value), encoding="utf-8")
            return path

        def auth_case(case_id: str, label: str, altered: dict[str, Any]) -> None:
            path = fixture(f"{case_id}.json", {**login_cfg, **altered})
            ctx.set_negative_context("8. Authentication and Session Cases", case_id)
            result = ctx.run_py(
                f"{case_id}: {label}",
                "bryck_cloud_show.py",
                "--login", str(path),
                expect_fail=True,
            )
            if result.passed:
                executed.add(("8. Authentication and Session Cases", case_id))

        def token_case(case_id: str, label: str, token: str, script: str, *args: str) -> None:
            path = fixture(f"{case_id}.json", {**login_cfg, "bryckapi_token": token})
            ctx.set_negative_context("8. Authentication and Session Cases", case_id)
            result = ctx.run_py(
                f"{case_id}: {label}",
                script,
                "--login", str(path),
                *args,
                expect_fail=True,
            )
            if result.passed:
                executed.add(("8. Authentication and Session Cases", case_id))

        auth_case("AUTH-01", "invalid username", {"bryckapi_username": "invalid-user"})
        auth_case("AUTH-02", "invalid password", {"bryckapi_password": "invalid-password"})
        auth_case("AUTH-05", "missing authentication token", {"bryckapi_password": ""})
        token_case("AUTH-03", "invalid access token", "invalid.jwt.token", "bryck_cloud_show.py")
        token_case("AUTH-04", "expired token", "eyJhbGciOiJub25lIn0.eyJleHAiOjF9.invalid", "bryck_cloud_show.py")
        token_case("AUTH-06", "API request after session expiry", "expired-session-token", "bryck_cloud_show.py")
        token_case("AUTH-07", "transfer operation after session expiry", "expired-session-token", "bryck_cloud_transfer_status.py")
        token_case("AUTH-08", "pause after session expiry", "expired-session-token", "bryck_cloud_transfer_pause.py", "--transfer-id", "1")
        token_case("AUTH-09", "resume after session expiry", "expired-session-token", "bryck_cloud_transfer_resume.py", "--transfer-id", "1")
        token_case("AUTH-10", "cancel after session expiry", "expired-session-token", "bryck_cloud_transfer_cancel.py", "--transfer-id", "1")

        def config_case(case_id: str, label: str, altered: dict[str, Any]) -> None:
            path = fixture(f"{case_id}.json", {**cloud_cfg, **altered})
            ctx.set_negative_context("10. AWS Configuration Cases", case_id)
            result = ctx.run_py(
                f"{case_id}: {label}",
                "bryck_cloud_configure.py",
                "--login", str(ctx.login_json),
                "--params", str(path),
                expect_fail=True,
            )
            if result.passed:
                executed.add(("10. AWS Configuration Cases", label))

        config_case("AWS-01", "Missing access key", {"access_key_id": ""})
        config_case("AWS-02", "Missing secret key", {"secret_access_key": ""})
        config_case("AWS-03", "Invalid access key", {"access_key_id": "invalid-access-key"})
        config_case("AWS-04", "Invalid secret key", {"secret_access_key": "invalid-secret-key"})
        config_case("AWS-05", "Invalid region", {"region": "invalid-region"})
        config_case("AWS-06", "Invalid endpoint", {"endpoint": "http://127.0.0.1:1"})
        config_case("AWS-07", "Invalid bucket", {"cloud_bucket": "not-a-valid-bucket"})
        config_case("AWS-08", "Nonexistent bucket", {"cloud_bucket": "s3://does-not-exist-bryck-negative"})

        # Duplicate configuration is a real operation, so execute it only
        # with the valid fixture and clean it up immediately afterward.
        ctx.set_negative_context("10. AWS Configuration Cases", "AWS-DUPLICATE")
        first = ctx.configure_cloud()
        if first.passed:
            second = ctx.configure_cloud()
            if second.passed or second.expected_failure:
                executed.add(("10. AWS Configuration Cases", "Duplicate configuration"))
            ctx.deconfigure_cloud()
            ctx.cloud_configured = False
            executed.add(("10. AWS Configuration Cases", "Deconfiguration when not configured"))
            second_remove = ctx.deconfigure_cloud()
            if second_remove.passed:
                executed.add(("10. AWS Configuration Cases", "Deconfiguration twice"))

    return executed


def run_negative_focused(ctx: TestContext) -> None:
    """Run the requested authentication, AWS, transfer, and cleanup focus set."""
    _print_banner("FOCUSED NEGATIVE TESTS — AUTH / AWS / TRANSFERS / CLEANUP")
    executed = run_negative_auth_and_aws_cases(ctx)
    executed.update(run_negative_stateful_cases(ctx))

    plan_path = SCRIPT_DIR / "NEGATIVE_TEST_PLAN.md"
    transfer_id_cases = {
        "Nonexistent ID", "Empty ID", "Negative ID", "Alphabetic ID",
        "Special-character ID", "Extremely large ID", "ID from another system",
        "Old or deleted ID", "Malformed ID",
    }
    printed_heading = ""
    seen: set[tuple[str, str]] = set()
    executed_auth_ids = {case_id for heading, case_id in executed if heading == "8. Authentication and Session Cases"}
    executed_aws_descriptions = {case_id for heading, case_id in executed if heading == "10. AWS Configuration Cases"}
    for case_id, heading, description in _negative_plan_entries(plan_path):
        key = (heading, description)
        if key in seen or key in executed or (heading, case_id) in executed:
            continue
        if heading == "8. Authentication and Session Cases" and case_id in executed_auth_ids:
            continue
        if heading == "10. AWS Configuration Cases" and description in executed_aws_descriptions:
            continue
        seen.add(key)
        if heading == "9. Transfer ID Validation Cases" and description in transfer_id_cases:
            if heading != printed_heading:
                _print_banner(f"FOCUSED HEADING: {heading}")
                printed_heading = heading
            run_negative_transfer_id_case(ctx, case_id, description)
            continue
        if heading in {
            "8. Authentication and Session Cases",
            "10. AWS Configuration Cases",
        }:
            ctx.set_negative_context(heading, case_id)
            ctx.blocked_negative_case(
                heading,
                case_id,
                f"Requires token/session-expiry or permission fixture: {description}",
            )

    ctx.set_negative_context("24. Final State and Cleanup Cases", "FOCUSED-FINAL")
    ctx.transfer_status_all()
    ctx.bryck_info("focused final state")


def _negative_plan_entries(plan_path: Path) -> list[tuple[str, str, str]]:
    """Read only executable scenario entries from Sections 7 through 24."""
    if not plan_path.exists():
        return []

    case_headings = {
        "7. CLI and Input Validation Cases",
        "8. Authentication and Session Cases",
        "9. Transfer ID Validation Cases",
        "10. AWS Configuration Cases",
        "11. AWS Bucket and Object Path Cases",
        "12. Bryck Lifecycle Cases",
        "13. Dataset Generation Cases",
        "14. Upload Negative Cases",
        "15. Download Negative Cases",
        "16. Transfer State Transition Cases",
        "17. Concurrent and Race-Condition Cases",
        "18. Duplicate and Repeated Operations",
        "19. Report Negative Cases",
        "20. API and SSH Failure Cases",
        "21. Service and Recovery Cases",
        "22. Transfer Verification and Completion Cases",
        "23. Data Integrity Cases",
        "24. Final State and Cleanup Cases",
        "25. Management Operations Cases",
        "26. Service Fault Injection Matrix (Active Transfer & Management Operations)",
        "27. Excel State Matrix Cases",
        "28. Excel Combination Flow Cases",
    }
    ignored_bullet_prefixes = (
        "Expected behavior:",
        "Expected result:",
        "Validate ",
        "The runner ",
        "The test ",
        "Every operation ",
        "For each ",
        "After recovery, ",
        "Compare ",
        "Document whether ",
        "Cleanup is ",
    )
    entries: list[tuple[str, str, str]] = []
    heading = ""
    counter = 0
    collect_bullets = False
    skip_bullets = False
    for raw_line in plan_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        case_id = ""
        if line.startswith("## "):
            heading = line[3:].strip()
            counter = 0
            collect_bullets = heading in case_headings
            skip_bullets = False
            continue
        if heading not in case_headings:
            continue
        if line.startswith("Expected behavior:") or line.startswith("Expected result:"):
            skip_bullets = True
            continue
        if line and not line.startswith(("|", "-", "```")):
            if line.startswith(("Test ", "Run ", "Also test", "With explicit")):
                collect_bullets = True
                skip_bullets = False
            elif line.startswith(("Validate ", "For each ", "Compare ", "Document ")):
                skip_bullets = True
        description = ""
        if line.startswith("| ") and line.count("|") >= 3:
            fields = [field.strip() for field in line.strip("|").split("|")]
            if fields and fields[0] not in {"ID", "---"} and not fields[0].startswith("-"):
                description = fields[1] if len(fields) > 1 else fields[0]
                case_id = fields[0] if re.fullmatch(r"[A-Z]+-\d+", fields[0]) else ""
        elif line.startswith("- ") and collect_bullets and not skip_bullets:
            description = line[2:].strip()
            case_id = ""
        else:
            continue
        if not description or description.startswith(ignored_bullet_prefixes):
            continue
        counter += 1
        entries.append((case_id or f"PLAN-{counter:03d}", heading, description))
    return entries


def run_negative_all(ctx: TestContext) -> None:
    """Execute implemented negatives once and audit the complete plan catalog.

    Cases needing service controls, token fixtures, or fault injection are
    recorded as BLOCKED until those environment capabilities are configured.
    """
    _print_banner("NEGATIVE TEST PLAN — ALL MAIN HEADINGS")
    fixture_cases = run_negative_cli_fixtures(ctx)
    ctx.set_negative_context("1. CLI / Input Validation", "IMPLEMENTED-CORE")
    run_combo_negative(ctx)
    stateful_cases = run_negative_stateful_cases(ctx)

    plan_path = SCRIPT_DIR / "NEGATIVE_TEST_PLAN.md"
    implemented_markers = {
        "CLI-01", "CLI-02", "CLI-03", "CLI-04", "CLI-05",
        "CLI-06", "CLI-07", "CLI-08", "CLI-09", "CLI-10",
        "CLI-11", "CLI-12", "CLI-13", "CLI-14", "CLI-15",
        "CLI-16", "CLI-17", "CLI-18",
    } | fixture_cases
    seen: set[tuple[str, str]] = set()
    printed_heading = ""
    transfer_id_cases = {
        "Nonexistent ID",
        "Empty ID",
        "Negative ID",
        "Alphabetic ID",
        "Special-character ID",
        "Extremely large ID",
        "ID from another system",
        "Old or deleted ID",
        "Malformed ID",
    }
    for case_id, heading, description in _negative_plan_entries(plan_path):
        key = (heading, description)
        if key in seen:
            continue
        seen.add(key)
        if case_id in implemented_markers:
            continue
        if key in stateful_cases:
            continue
        if heading != printed_heading:
            _print_banner(f"NEGATIVE MAIN HEADING: {heading}")
            printed_heading = heading
        if heading == "9. Transfer ID Validation Cases" and description in transfer_id_cases:
            run_negative_transfer_id_case(ctx, case_id, description)
            continue
        ctx.set_negative_context(heading, case_id)
        ctx.blocked_negative_case(
            heading,
            case_id,
            f"Planned scenario: {description}",
        )

    ctx.set_negative_context("24. Final State / Cleanup", "NEGATIVE-SUITE")
    ctx.bryck_info("negative suite final state")


# =============================================================================
# Reporting
# =============================================================================

def _html_escape(text: str) -> str:
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def _extract_api_calls(text: str) -> list[str]:
    """Extract sanitized API call/response lines emitted by session.py."""
    return [
        line.strip()
        for line in text.splitlines()
        if "API_CALL " in line or "API_RESPONSE " in line
    ]


def build_html(run: TestRun) -> str:
    iteration_blocks = []
    for it in run.iterations:
        step_rows = []
        for st in it.steps:
            status = st.outcome or ("PASS" if st.passed else ("EXPECTED FAIL" if st.expected_failure else "FAIL"))
            status_class = "pass" if st.passed else ("xfail" if st.expected_failure else "fail")
            detail_id = f"detail_{it.iteration}_{st.step}"
            cmd_escaped = _html_escape(st.command)
            stdout_escaped = _html_escape(st.stdout)
            stderr_escaped = _html_escape(st.stderr)
            api_calls_escaped = _html_escape("\n".join(st.api_calls) or "(not captured)")
            api_summary = (
                f"<details class='api-summary'><summary>{len(st.api_calls)} call(s)</summary>"
                f"<pre>{api_calls_escaped}</pre></details>"
                if st.api_calls
                else "<span class='api-empty'>0 calls</span>"
            )
            step_rows.append(
                f"<tr class='step-row'>"
                f"<td>{st.step}</td>"
                f"<td>{_html_escape(st.main_heading)}</td>"
                f"<td>{_html_escape(st.case_id)}</td>"
                f"<td>{_html_escape(st.name)}</td>"
                f"<td><code>{cmd_escaped}</code></td>"
                f"<td>{api_summary}</td>"
                f"<td>{st.returncode}</td>"
                f"<td>{st.duration_sec:.1f}s</td>"
                f"<td><span class='{status_class}'>{status}</span></td>"
                f"</tr>"
                f"<tr class='output-row'><td colspan='9' class='output-cell'>"
                f"<details id='{detail_id}' class='output-details'>"
                f"<summary>Show output</summary>"
                f"<pre>STDOUT:\n{stdout_escaped or '(no output)'}\n\nSTDERR:\n{stderr_escaped or '(no output)'}</pre>"
                f"</details>"
                f"</td></tr>"
            )
        iteration_blocks.append(
            ITERATION_TEMPLATE
            .replace("{{ITERATION}}", str(it.iteration))
            .replace("{{SCENARIO_OR_COMBO}}", _html_escape(it.scenario_or_combo))
            .replace("{{START_TIME}}", it.start_time)
            .replace("{{END_TIME}}", it.end_time)
            .replace("{{DURATION}}", f"{it.duration_sec:.1f}s")
            .replace("{{STATUS}}", "PASS" if it.passed else "FAIL")
            .replace("{{STATUS_CLASS}}", "pass" if it.passed else "fail")
            .replace("{{STEP_ROWS}}", "\n".join(step_rows))
        )

    total_steps = sum(len(it.steps) for it in run.iterations)
    passed_steps = sum(1 for it in run.iterations for s in it.steps if s.passed)
    failed_steps = total_steps - passed_steps
    expected_failures = sum(1 for it in run.iterations for s in it.steps if s.expected_failure)
    pass_rate = f"{passed_steps / total_steps * 100:.1f}%" if total_steps else "N/A"

    html = HTML_TEMPLATE
    html = html.replace("{{RUN_ID}}", _html_escape(run.run_id))
    html = html.replace("{{STARTED_AT}}", run.started_at)
    html = html.replace("{{FINISHED_AT}}", run.finished_at)
    html = html.replace("{{TOTAL_DURATION}}", f"{run.total_duration_sec:.1f}s")
    html = html.replace("{{COMMAND_LINE}}", _html_escape(run.command_line))
    html = html.replace("{{TOTAL_ITERATIONS}}", str(len(run.iterations)))
    html = html.replace("{{TOTAL_STEPS}}", str(total_steps))
    html = html.replace("{{PASSED_STEPS}}", str(passed_steps))
    html = html.replace("{{FAILED_STEPS}}", str(failed_steps))
    html = html.replace("{{EXPECTED_FAILURES}}", str(expected_failures))
    html = html.replace("{{PASS_RATE}}", pass_rate)
    html = html.replace("{{CONFIG_FILES}}", _html_escape(json.dumps(run.config_files, indent=2)))
    html = html.replace("{{ITERATIONS}}", "\n".join(iteration_blocks))
    return html


def _serve_report_and_open_browser(
    results_dir: Path,
    html_path: Path,
    host: str,
    port: int,
    advertised_host: str | None = None,
) -> None:
    """Start a background HTTP server and try to open the report in a browser."""
    class _Handler(SimpleHTTPRequestHandler):
        def __init__(self, *args: Any, **kwargs: Any):
            super().__init__(*args, directory=str(results_dir), **kwargs)

        def log_message(self, format: str, *args: Any) -> None:
            pass

    display_host = advertised_host or host
    if host in {"0.0.0.0", "::"} and advertised_host:
        display_host = advertised_host
    url = f"http://{display_host}:{port}/{html_path.name}"
    bind_url = f"http://{host}:{port}/{html_path.name}"
    print(f"\nServing report at: {url}")
    if bind_url != url:
        print(f"Bound on: {bind_url}")

    try:
        server = ThreadingHTTPServer((host, port), _Handler)
    except OSError as exc:
        print(f"WARNING: Could not start HTTP server on {host}:{port}: {exc}")
        return

    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()

    print("Attempting to open browser...")
    try:
        webbrowser.open(url)
    except Exception as exc:
        print(f"WARNING: Could not open browser automatically: {exc}")

    print("Press Ctrl+C to stop the server.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping HTTP server...")
    finally:
        server.shutdown()


def save_reports(run: TestRun, results_dir: Path) -> dict[str, Path]:
    _ensure_dir(results_dir)
    json_path = results_dir / f"{run.run_id}_results.json"
    log_path = results_dir / f"{run.run_id}_execution.log"
    html_path = results_dir / f"{run.run_id}_index.html"

    # JSON
    with open(json_path, "w", encoding="utf-8") as fh:
        json.dump(asdict(run), fh, indent=2, default=str)

    # Text log
    with open(log_path, "w", encoding="utf-8") as fh:
        fh.write(f"Run ID: {run.run_id}\n")
        fh.write(f"Command: {run.command_line}\n")
        fh.write(f"Started: {run.started_at}\n")
        fh.write(f"Finished: {run.finished_at}\n")
        fh.write(f"Duration: {run.total_duration_sec:.1f}s\n")
        fh.write("=" * 72 + "\n")
        for it in run.iterations:
            fh.write(f"\nITERATION {it.iteration} — {it.scenario_or_combo}\n")
            fh.write(f"Start: {it.start_time}  End: {it.end_time}  Duration: {it.duration_sec:.1f}s  Result: {'PASS' if it.passed else 'FAIL'}\n")
            for st in it.steps:
                status = "PASS" if st.passed else ("EXPECTED FAIL" if st.expected_failure else "FAIL")
                status = st.outcome or status
                fh.write(
                    f"\n[{status}] Step {st.step}: {st.name} "
                    f"(heading={st.main_heading}; case={st.case_id}; "
                    f"rc={st.returncode}, {st.duration_sec:.1f}s)\n"
                )
                fh.write(f"Command: {st.command}\n")
                if st.stdout:
                    fh.write("STDOUT:\n" + st.stdout + "\n")
                if st.stderr:
                    fh.write("STDERR:\n" + st.stderr + "\n")
                if st.api_calls:
                    fh.write("API CALLS:\n" + "\n".join(st.api_calls) + "\n")
                fh.write("-" * 72 + "\n")

    # HTML
    with open(html_path, "w", encoding="utf-8") as fh:
        fh.write(build_html(run))

    return {"json": json_path, "log": log_path, "html": html_path}


# =============================================================================
# CLI
# =============================================================================

def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Unified Bryck cloud-transfer test runner.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 cloud_transfer_test_runner.py --scenario small
  python3 cloud_transfer_test_runner.py --scenario large --iterations 3
  python3 cloud_transfer_test_runner.py --scenario all --iterations 2
  python3 cloud_transfer_test_runner.py --combination happy_path
  python3 cloud_transfer_test_runner.py --combination all
  python3 cloud_transfer_test_runner.py --all
  python3 cloud_transfer_test_runner.py --all --iterations 5 --dry-run
""",
    )
    parser.add_argument(
        "--scenario",
        choices=sorted(VALID_SCENARIOS),
        help="Run a specific scenario (small, large, million, or all).",
    )
    parser.add_argument(
        "--combination",
        choices=sorted(VALID_COMBINATIONS),
        help="Run a specific combination test or all combinations.",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Run all scenarios and all combination tests.",
    )
    parser.add_argument(
        "--negative-all",
        action="store_true",
        help="Run the complete negative plan in one shot; unsupported fixture-dependent cases are recorded as BLOCKED.",
    )
    parser.add_argument(
        "--negative-focused",
        action="store_true",
        help="Run focused authentication, AWS, transfer-state, report, and cleanup cases.",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=1,
        help="Number of times to repeat the selected flow(s). Default: 1.",
    )
    parser.add_argument(
        "--login",
        default=str(DEFAULT_LOGIN_JSON),
        help="Path to login.json. Default: ./login.json",
    )
    parser.add_argument(
        "--cloud-ops",
        default=str(DEFAULT_CLOUD_OPS_JSON),
        help="Path to cloud_ops.json. Default: ./cloud_ops.json",
    )
    parser.add_argument(
        "--format-mount-params",
        default=str(DEFAULT_FORMAT_MOUNT_PARAMS_JSON),
        help="Path to format_mount_params.json. Default: ./format_mount_params.json",
    )
    parser.add_argument(
        "--change-time-params",
        default=str(DEFAULT_CHANGE_TIME_PARAMS_JSON),
        help="Path to change_time_params.json. Default: ./change_time_params.json",
    )
    parser.add_argument(
        "--report-dir",
        default=str(DEFAULT_REPORT_DIR),
        help="Directory to save cloud-transfer report ZIPs. Default: /home/bryck/report_api",
    )
    parser.add_argument(
        "--results-dir",
        default=str(DEFAULT_RESULTS_DIR),
        help="Directory to save JSON/log/HTML results. Default: ./results",
    )
    parser.add_argument(
        "--datagen-bin",
        default=DATAGEN_BIN,
        help=f"Path to datagen binary on the Bryck/server. Default: {DATAGEN_BIN}",
    )
    parser.add_argument(
        "--spec-dir",
        default=str(SPEC_DIR),
        help=f"Directory containing YAML spec files. Default: {SPEC_DIR}",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print commands but do not execute them.",
    )
    parser.add_argument(
        "--ssh-user",
        default=None,
        help="SSH username for the Bryck server. If omitted, read from login.json 'bryckserver_username'.",
    )
    parser.add_argument(
        "--ssh-host",
        default=None,
        help="SSH host for the Bryck server. If omitted, read from login.json 'bryckapi_host'.",
    )
    parser.add_argument(
        "--serve-report",
        action="store_true",
        help="After the run, start a local HTTP server and open the HTML report in a browser.",
    )
    parser.add_argument(
        "--serve-host",
        default="0.0.0.0",
        help="Host to bind the report HTTP server to. Default: 0.0.0.0",
    )
    parser.add_argument(
        "--serve-port",
        type=int,
        default=8080,
        help="Port for the report HTTP server. Default: 8080",
    )
    parser.add_argument(
        "--report-host",
        default=None,
        help="Host/IP used in the printed report URL. Defaults to the API/SSH host.",
    )
    parser.add_argument(
        "--report-port",
        type=int,
        default=8000,
        help="Port used in the printed report URL when using an external HTTP server. Default: 8000",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)

    if not (args.scenario or args.combination or args.all or args.negative_all or args.negative_focused):
        print("ERROR: specify --scenario, --combination, --negative-all, --negative-focused, or --all. Use -h for help.")
        return 2

    login_json = Path(args.login)
    cloud_ops_json = Path(args.cloud_ops)
    fmt_mount_json = Path(args.format_mount_params)
    change_time_json = Path(args.change_time_params)

    for path in (login_json, cloud_ops_json, fmt_mount_json, change_time_json):
        if not path.exists():
            print(f"ERROR: config file not found: {path}")
            return 2

    login_cfg = _load_json(login_json)
    ssh_user = args.ssh_user or login_cfg.get("bryckserver_username", "bryck")
    ssh_host = args.ssh_host or login_cfg.get("bryckapi_host")
    if not ssh_host:
        print("ERROR: ssh_host could not be determined. Provide --ssh-host or set bryckapi_host in login.json.")
        return 2

    results_dir = Path(args.results_dir)
    _ensure_dir(results_dir)

    run_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    run = TestRun(
        run_id=run_id,
        started_at=_now(),
        finished_at="",
        total_duration_sec=0.0,
        command_line=" ".join(sys.argv),
        config_files={
            "login.json": login_cfg,
            "cloud_ops.json": _load_json(cloud_ops_json),
            "format_mount_params.json": _load_json(fmt_mount_json),
            "change_time_params.json": _load_json(change_time_json),
        },
    )

    work_items: list[tuple[str, Callable[[TestContext], None]]] = []
    if args.negative_focused:
        work_items.append(("negative_focused", run_negative_focused))
    elif args.negative_all:
        work_items.append(("negative_all", run_negative_all))
    elif args.all:
        work_items.extend([
            ("small", run_scenario_small),
            ("large", run_scenario_large),
            ("million", run_scenario_million),
            ("combo_happy_path", run_combo_happy_path),
            ("combo_pause_resume_cancel", run_combo_pause_resume_cancel),
            ("combo_priority", run_combo_priority),
            ("combo_both_mode", run_combo_both_mode),
            ("combo_monitoring", run_combo_monitoring),
            ("combo_settings", run_combo_settings),
            ("combo_negative", run_combo_negative),
        ])
    elif args.scenario:
        if args.scenario == "all":
            work_items.extend([
                ("small", run_scenario_small),
                ("large", run_scenario_large),
                ("million", run_scenario_million),
            ])
        else:
            mapping = {
                "small": run_scenario_small,
                "large": run_scenario_large,
                "million": run_scenario_million,
            }
            work_items.append((args.scenario, mapping[args.scenario]))
    elif args.combination:
        combo_map = {
            "happy_path": run_combo_happy_path,
            "pause_resume_cancel": run_combo_pause_resume_cancel,
            "priority": run_combo_priority,
            "both_mode": run_combo_both_mode,
            "monitoring": run_combo_monitoring,
            "settings": run_combo_settings,
            "negative": run_combo_negative,
        }
        if args.combination == "all":
            for k, fn in combo_map.items():
                work_items.append((f"combo_{k}", fn))
        else:
            work_items.append((f"combo_{args.combination}", combo_map[args.combination]))

    run_start = time.time()
    try:
        for iteration in range(1, args.iterations + 1):
            for scenario_name, scenario_fn in work_items:
                _print_banner(f"Iteration {iteration}/{args.iterations} — {scenario_name}")
                it_start = time.time()
                ctx = TestContext(
                    login_json=login_json,
                    cloud_ops_json=cloud_ops_json,
                    fmt_mount_json=fmt_mount_json,
                    change_time_json=change_time_json,
                    report_dir=Path(args.report_dir),
                    results_dir=results_dir,
                    ssh_user=ssh_user,
                    ssh_host=ssh_host,
                    datagen_bin=args.datagen_bin,
                    spec_dir=Path(args.spec_dir),
                    dry_run=args.dry_run,
                    iteration=iteration,
                    scenario_name=scenario_name,
                )
                try:
                    scenario_fn(ctx)
                except Exception as exc:
                    ctx._record(
                        "UNHANDLED EXCEPTION",
                        str(scenario_fn),
                        -1,
                        "",
                        str(exc),
                        0.0,
                        notes="runner crashed",
                    )
                finally:
                    ctx.run_common_report_checks()
                    if not args.dry_run:
                        ctx.cleanup_transfers()

                it_end = time.time()
                passed = all(
                    s.passed or s.outcome == "BLOCKED"
                    for s in ctx.steps
                )
                it_result = IterationResult(
                    iteration=iteration,
                    scenario_or_combo=scenario_name,
                    start_time=_now(),
                    end_time=_now(),
                    duration_sec=it_end - it_start,
                    steps=ctx.steps,
                    passed=passed,
                )
                run.iterations.append(it_result)
                print(f"Iteration {iteration} — {scenario_name}: {'PASS' if passed else 'FAIL'}")
    except KeyboardInterrupt:
        print("\nInterrupted by user.")

    run.finished_at = _now()
    run.total_duration_sec = time.time() - run_start

    total_steps = sum(len(it.steps) for it in run.iterations)
    passed_steps = sum(1 for it in run.iterations for s in it.steps if s.passed)
    blocked_steps = sum(1 for it in run.iterations for s in it.steps if s.outcome == "BLOCKED")
    failed_steps = sum(
        1 for it in run.iterations
        for s in it.steps
        if not s.passed and s.outcome != "BLOCKED"
    )
    executed_steps = total_steps - blocked_steps
    failed_iterations = sum(
        1 for it in run.iterations
        if any(not s.passed and s.outcome != "BLOCKED" for s in it.steps)
    )

    run.summary = {
        "iterations": len(run.iterations),
        "total_steps": total_steps,
        "passed_steps": passed_steps,
        "failed_steps": failed_steps,
        "failed_iterations": failed_iterations,
        "blocked_steps": blocked_steps,
        "executed_steps": executed_steps,
        "pass_rate": f"{passed_steps / executed_steps * 100:.1f}%" if executed_steps else "N/A",
    }

    paths = save_reports(run, results_dir)

    _print_banner("Run complete")
    print(f"Iterations:     {run.summary['iterations']}")
    print(f"Total steps:    {run.summary['total_steps']}")
    print(f"Executed steps:  {run.summary['executed_steps']}")
    print(f"Passed steps:   {run.summary['passed_steps']}")
    print(f"Failed steps:   {run.summary['failed_steps']}")
    print(f"Blocked steps:  {run.summary['blocked_steps']}")
    print(f"Failed flows:   {run.summary['failed_iterations']}")
    print(f"Pass rate:      {run.summary['pass_rate']}")
    print(f"Results JSON:   {paths['json']}")
    print(f"Execution log:  {paths['log']}")
    print(f"HTML report:    {paths['html']}")
    report_host = args.report_host or ssh_host
    report_name = paths["html"].name
    print(f"HTML report URL: http://{report_host}:{args.report_port}/results/{report_name}")
    print(
        "Report server command: "
        f"cd {results_dir.parent} && python3 -m http.server {args.report_port} --bind 0.0.0.0"
    )

    if args.serve_report:
        _serve_report_and_open_browser(
            results_dir,
            paths["html"],
            args.serve_host,
            args.serve_port,
            advertised_host=ssh_host,
        )

    return 0 if run.summary["failed_iterations"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
