# Phase 9 — CLI (bryckclient-cli)

[← Back to master plan](../complete_plan.md)

> **What this phase is:** the operator-facing **CloudCP CLI** surface
> (`bryckclient-cli`: mount/eject/format/erase, cloud configure/show/deconfigure,
> transfer initiate/status/pause/resume/cancel/report). This phase validates the CLI's
> functional flows against a **real Bryck appliance** across the full dataset size range,
> including disruptive lifecycle conditions — service restarts, ejects, format/erase/remove
> attempts, and cancel + re-transfer — with evidence-backed PASS/FAIL per operation.

**Priority:** P0 (functional CLI flows). Throughput/scale is P2.
**Driven through:** `bryckclient-cli` runners via `cloud_cli_runner.py` (two-phase
`--plan` → `--execute`).
**Status:** This phase has a **complete, runnable suite** in
[../../CloudCpCliTesting/](../../CloudCpCliTesting/) — see
[cloud_cli_plan.md](../../CloudCpCliTesting/cloud_cli_plan.md).

---

## 1. Execution Model

Two phases, one safety gate:

1. **`--plan`** (read-only) — validate JSON/YAML configs, SSH + REST connectivity, read
   Bryck state, resolve datasets, build the full ordered test-case list, and render a
   single confirmation screen. Modifies nothing.
2. **`--execute`** (real work) — run every confirmed case: transfers, live interventions,
   service restarts, edge and negative cases; write per-case evidence to
   `results/<RUN_ID>/<TEST_ID>/`; auto-clean datasets + cloud objects after each case;
   emit JSON + HTML + Markdown reports.

Destructive operations (format/erase/remove/eject) are **executed for real**, so the single
pre-execute confirmation gate is the only safety checkpoint. Every case runs against a
**single Bryck**, and `Executor.ensure_mounted()` guarantees the device is mounted before
any data generation or transfer.

> **Register (Excel):** the full case list is maintained as a shareable workbook at
> [../../CloudCpCliTesting/CloudCpCli_TestCases.xlsx](../../CloudCpCliTesting/CloudCpCli_TestCases.xlsx)
> (sheets: *Overview*, *Transfer Matrix*, *Live Intervention*, *Service & Edge*,
> *Negative - CLI Input*, *Negative - AWS Config*).

---

## 2. Test Cases

**Totals:** 41 top-level test cases (18 transfer + 6 live-intervention + 2 service +
4 edge + 9 CLI-input negative + 8 AWS-config negative), plus 60 live-intervention action
sub-results (6 tiers × 10 actions).

### 2.1 Transfer Matrix — one case per tier × per mode (18, P0)

Every dataset runs `upload`, `download`, and `both`. **Pass:** transfer reaches
`COMPLETED`, object count/sizes match the source, and `final_report.csv` row count matches
the expected file count. `download` cases require the tier's objects to already exist, so
the runner always executes `CLI-U-<TIER>` before `CLI-D-<TIER>`.

| Tier | Dataset | Test IDs |
|---|---|---|
| ZERO   | `DS-P1-01` | `CLI-U-ZERO`, `CLI-D-ZERO`, `CLI-B-ZERO` |
| TINY   | `DS-P1-02` | `CLI-U-TINY`, `CLI-D-TINY`, `CLI-B-TINY` |
| SMALL  | `DS-P1-03` | `CLI-U-SMALL`, `CLI-D-SMALL`, `CLI-B-SMALL` (crosses 64 MB multipart edge) |
| MEDIUM | `DS-P1-04` | `CLI-U-MEDIUM`, `CLI-D-MEDIUM`, `CLI-B-MEDIUM` (target of the service-restart tests) |
| LARGE  | `DS-P1-05` | `CLI-U-LARGE`, `CLI-D-LARGE`, `CLI-B-LARGE` |
| SPARSE | `06_sparse_files.yaml` | `CLI-U-SPARSE`, `CLI-D-SPARSE`, `CLI-B-SPARSE` |

### 2.2 Live Intervention Matrix — includes pause/resume (6 cases × 10 actions, P0)

`CLI-LC-<TIER>` (ZERO/TINY/SMALL/MEDIUM/LARGE/SPARSE) each run the same 10-action sequence
against the `CLI-B-<TIER>` transfer while it is `IN_PROGRESS`. Each action logs the full
`Before State → Action → API/CLI Response → After State → Expected → Actual → PASS/FAIL`.

| # | Action | Expected result |
|---|---|---|
| 1 | **Pause** | Status transitions to `PAUSED`; no data loss on resume |
| 2 | **Resume** | Status returns to `IN_PROGRESS` and eventually `COMPLETED` |
| 3 | Cancel | Status transitions to `CANCELLED`; no further progress |
| 4 | Re-transfer | A new `transfer_id` is created and reaches `COMPLETED` independently of the cancelled one |
| 5 | Mount | Bryck state becomes `Mounted`; no-op if already mounted |
| 6 | Eject (mid-transfer) | Negative: transfer surfaces a failure/stopped state, not hang or corrupt report; Bryck ejects cleanly |
| 7 | Format attempt | Bryck refuses or fully reformats per state; if it proceeds, the dataset is regenerated afterward |
| 8 | Erase attempt | Cloud config/transfer history reset; runner reconfigures cloud afterward |
| 9 | Remove attempt | Bryck deregistered from `bryckapi`; re-added if needed; run-ending if remove succeeds |
| 10 | Restart `bcloud` / `bryckapi` | Service returns within a bounded wait; any `IN_PROGRESS` transfer resumes or is cleanly `FAILED`/`STOPPED`, never silently lost |

> **Pause/Resume** is the core recoverability check here (actions 1–2): a paused transfer
> must hold state and resume to completion with no data loss and no duplicated work.

### 2.3 Service Restart Matrix (2, P0)

| Test ID | Restart target | Timing |
|---|---|---|
| `CLI-SVC-BCLOUD`   | `bcloud.service`   | Mid-transfer on `CLI-B-MEDIUM` (multipart in progress) |
| `CLI-SVC-BRYCKAPI` | `bryckapi.service` | Mid-transfer on `CLI-B-MEDIUM` (multipart in progress) |

**Pass:** service comes back up within a bounded wait; the active transfer resumes or is
cleanly marked `FAILED`/`STOPPED` (never silently lost).

### 2.4 Negative / Edge Cases (4, P0)

| Test ID | Dataset | Scenario | Pass when |
|---|---|---|---|
| `CLI-EDGE-01` | `DS-P8-01` | Empty source directory upload | Completes with 0 objects; no error |
| `CLI-EDGE-02` | `DS-P8-04` | 14-level deep directory tree upload | All paths preserved; no scanner stack overflow |
| `CLI-EDGE-03` | `DS-P9-04` | Single 64 MB file upload (first multipart size) | Uses multipart; single object in report |
| `CLI-EDGE-04` | `DS-P4-01` | Tiny tier, 20 filename variants upload | Every filename variant round-trips byte-for-byte |

### 2.5 CLI Input-Validation Negative (9, P0)

`CLI-01`…`CLI-09` — every case expects rejection (`expect_fail`) **before** any real
mutation, run through the 10-step environment-aware pipeline
(`Executor._run_negative_pipeline()`): bad/missing `--mode`, empty `bryck_src` /
`cloud_bucket` / `bryck_dst`, missing/malformed `login.json`, invalid `--transfer-id`, and
`datagen` against a nonexistent spec.

### 2.6 AWS Config Negative (8, P0)

`AWS-01`…`AWS-08` — each mutates a **private per-case copy** of `cloud_ops.json` (never the
shared file): empty/invalid `access_key_id` / `secret_access_key`, invalid `region`, invalid
`cloud_bucket`. `AWS-07`/`AWS-08` (deconfigure idempotence) are **observational** — return
codes are recorded for evidence, not forced to PASS/FAIL.

---

## 3. Datasets Used

- **Transfer matrix:** size-tier primaries `DS-P1-01`…`DS-P1-05` plus a separate SPARSE spec
  (`CloudCpFallbackTesting/spec_files/06_sparse_files.yaml`, per decision #15). Default
  `--dataset-catalog all` runs every dataset in
  [../../dataset_cloudcp/spec_files/manifest.json](../../dataset_cloudcp/spec_files/manifest.json)
  (54 datasets) as its own round; `--dataset-catalog tiers` / `specfiles` narrow it.
- **Edge cases:** `DS-P8-01`, `DS-P8-04`, `DS-P9-04`, `DS-P4-01`.

---

## 4. Tools

- `cloud_cli_runner.py` — two-phase orchestrator (`--plan` / `--execute`, `--only`,
  `--suite`, `--tiers`, `--modes`, `--dry-run`, `--keep`).
- `bryckclient-cli/` — operator CLI scripts under test (mount/eject/format/erase, cloud
  configure/show/deconfigure, transfer initiate/status/pause/resume/cancel/report).
- `cloudcpclitesting.py` — dataset generation + report-validation helpers.

Runnable plan: [../../CloudCpCliTesting/cloud_cli_plan.md](../../CloudCpCliTesting/cloud_cli_plan.md).
Full `--help`: [../tools_guide.md](../tools_guide.md).

---

## 5. To Be Added

- Integrate `cloud_cli_runner.py` results into the CLI test-case register
  [../../CloudCpCliTesting/CloudCpCli_TestCases.xlsx](../../CloudCpCliTesting/CloudCpCli_TestCases.xlsx).
- Resolve open items (SPARSE spec standardization, passwordless-`sudo` for service restarts,
  destination bucket/prefix naming) — see [cloud_cli_plan.md §17](../../CloudCpCliTesting/cloud_cli_plan.md).
- Multi-Bryck parallel execution and GCP/Azure cloud types (out of scope this iteration).

Existing today: complete CLI suite (plan + two-phase runner + `bryckclient-cli` scripts +
per-case evidence/reporting) and the test-case register
[../../CloudCpCliTesting/CloudCpCli_TestCases.xlsx](../../CloudCpCliTesting/CloudCpCli_TestCases.xlsx).
