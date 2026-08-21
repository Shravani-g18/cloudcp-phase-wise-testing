# Phase 4 — Reporting & Verification

[← Back to master plan](../complete_plan.md)

> **What this phase is:** After transfers complete, the verification engine diffs the
> **source index** (every file on disk) against the **upload report** (every file
> transferred), producing a final per-file status and per-tier summary. This phase validates
> **status correctness, de-duplication, ordering barriers, encoding-safe joins, and progress
> counters.**

**Priority:** P0 (report correctness).
**Config:** `VERIFICATION.*` in `/etc/bryck/bryckcloud/config.json`
(`REPORT_FORMAT=json`, `VERIFY_S3_WORKERS=16`, `VERIFY_STAT_THREADS=32`).
**Status:** This phase has a **runnable suite** in
[../../CloudCpReportTesting/](../../CloudCpReportTesting/) — a synthetic reference-engine
suite ([PHASE4_AUTOMATION_OVERVIEW.md](../../CloudCpReportTesting/PHASE4_AUTOMATION_OVERVIEW.md))
and a live end-to-end suite ([cloud_cp_report_test_case_plan.md](../../CloudCpReportTesting/cloud_cp_report_test_case_plan.md)).

> **Register (Excel):** the full case list is maintained as a shareable workbook at
> [../../CloudCpReportTesting/CloudCpReport_TestCases.xlsx](../../CloudCpReportTesting/CloudCpReport_TestCases.xlsx)
> (sheets: *Overview*, *Final Status Model*, *Synthetic P4 Cases*, *Live RT Cases*,
> *Cross-Cutting Checks*).

---

## 1. Final Status Model

Every source file is classified into exactly one status:

| Status | Meaning | Injected by |
|---|---|---|
| `OK` | Transferred and verified | Normal upload |
| `MISSING` | In source, never uploaded | Skip uploading N files |
| `FAILED` | cloudcp + fallback both gave up | N permanently non-retryable errors |
| `MISMATCH` | S3 size ≠ source size | Mock wrong HeadObject size |
| `EXTRA` | In S3 but not in source index | Manually PUT extra objects |

---

## 2. Test Cases (P0)

| ID | Case | How | Pass when |
|---|---|---|---|
| P4-01 | Exactly one correct status per file | Inject all 5 status types in known counts | Each status count == injected count; no file missing/duplicated; CSV quoting preserves embedded newlines |
| P4-02 | Refuse verify while scan in progress | `scan_state=in_progress` | Error "scan_state=in_progress, cannot verify"; proceeds after complete |
| P4-03 | Paused transfer does not trigger verify | `pause_requested=True` mid-transfer | Verify not triggered until resumed to natural completion barrier |
| P4-04 | Encoding-safe merge-join | Upload full variant set (DS-P4-*) | All variants `OK`; zero false MISSING/MISMATCH from encoding |
| P4-05 | De-dup last-status-wins | 50 files with `SKIPPED` + `FALLBACK_OK` rows | Each appears once as `OK`; no dup rows for SUCCESS/SKIPPED/FALLBACK_OK combos |
| P4-06 | Progress counters monotonic | Sample every 5 s during a transfer | `files_done`/`bytes_done` never decrease; `total_files` non-zero from first checkpoint |
| P4-07 | Per-tier completion summary | DS-P6-01 / mixed | Per-tier file counts sum to total; batches created vs completed; bytes; `avg_batch_duration_sec` populated |
| P4-08 | Failure summary triage rows | Inject permanent failures | One row per permanently failed file with full triage context |

---

## 2A. Executable Suites (CloudCpReportTesting)

The P0 cases above are realised as two concrete suites. The **synthetic** suite proves the
merge-join assertion logic today against injected fixtures; the **live** suite runs the full
real flow `datagen → transfer → report ZIP → parse → verify` on a real Bryck.

### 2A.1 Synthetic reference-engine suite (`verify_and_report.py`)

Eight plugin cases in [../../CloudCpReportTesting/cases/](../../CloudCpReportTesting/cases/),
run against the faithful reference engine `report_engine.py` (no live transfer). These are the
P4-01…P4-08 cases in §2 above.

| ID | Case | Pass criteria |
|---|---|---|
| P4-01 | Exactly one correct status per file | Exact per-status counts (OK=101, MISMATCH=20, FAILED=20, MISSING=20, EXTRA=10); no dups; CSV quoting preserved |
| P4-02 | Refuse verify while scan in progress | Refused at `scan_state=in_progress`; succeeds when `complete` |
| P4-03 | Paused transfer does not trigger verify | Blocked while `pause_requested=True`; proceeds after resume |
| P4-04 | Encoding-safe merge-join | All 6 unicode/special-name variants classified `OK` |
| P4-05 | De-dup — last status wins | 50 files with `SKIPPED`→`FALLBACK_OK` collapse to 50 `OK` rows |
| P4-06 | Progress counters monotonic *(simulated)* | `files_done`/`bytes_done` never decrease — synthetic sequence only |
| P4-07 | Per-tier completion summary | Per-tier counts (20/40/30/15/5) sum to 110 exactly |
| P4-08 | Failure-summary triage rows | 15 `FAILED` rows, each with non-empty `last_error` + `retry_count` |

### 2A.2 Live end-to-end suite (`run_report_tests.py`)

Ten cases in [../../CloudCpReportTesting/live_cases/](../../CloudCpReportTesting/live_cases/)
that generate real data, run a real transfer, download + parse the report ZIP, and assert
against it. Source root `/bryck/report_testing/<case>`, S3 dest `s3://vijay/report_testing/<case>`.

| ID | Case | Files | Mode |
|---|---|---|---|
| RT-01 | Happy path: small flat files | 120 flat, 1–20 MB (~1 GB) | Upload |
| RT-02 | Mixed tier upload | 250 (tiny 80 + small 120 + medium 50, ~8 GB) | Upload |
| RT-03 | Filename encoding variants | 60 (spaces/parens/brackets/commas/unicode, ~300 MB) | Upload |
| RT-04 | Nested directory tree | 100 across 5 subdir levels (~500 MB) | Upload |
| RT-05 | Zero-byte files | 10 zero-byte | Upload |
| RT-06 | Single large file | 1 file, 600 MB–1 GB | Upload |
| RT-07 | High file count | 1000 flat, 512 KB each (~512 MB) | Upload |
| RT-08 | Re-upload to same destination (idempotency) | 120 uploaded twice | Upload ×2 |
| RT-09 | Download transfer (S3 → /bryck) | 120 objects (RT-01 seed) | Download |
| RT-10 | Round-trip: upload + download | 120 up then down | Upload+Download |

**Cross-cutting checks** run on every parsed report: no duplicate/null `local_path`/`s3path`,
non-negative integer `size`, `Transfer status: Completed`, summary count == report row count,
summary missing == 0, and post-run cleanup verified (dir gone, S3 prefix empty).

---

## 3. Datasets Used

| Category | Datasets | Purpose |
|---|---|---|
| 4 — Filename & Encoding | DS-P4-05 (cross-tier variants) | Merge-join correctness |
| 6 — Network Profile | DS-P6-01 | Per-tier summary aggregation |
| 5 — File Type Coverage | DS-P5-01 | No type mis-reported |

Expected counts: [../../dataset_cloudcp/spec_files/manifest.json](../../dataset_cloudcp/spec_files/manifest.json).

---

## 4. Tools

- Verification engine (config-driven; `REPORT_FORMAT=json`).
- **`verify_and_report.py`** — synthetic runner over `report_engine.py` (reference merge-join
  engine) + plugin `cases/` (`--all` / `--case` / `--list` / `--dry-run` / `--live`).
- **`run_report_tests.py`** — live end-to-end runner over `live_cases/` (`--all` /
  `--from`/`--to` / `--one` / `--manual` / `--no-datagen` / `--no-transfer` / `--transfer-id`).
- `dataset_validator.py` — independent file-count ground truth vs manifest.

Runnable plans:
[cloud_cp_report_test_case_plan.md](../../CloudCpReportTesting/cloud_cp_report_test_case_plan.md) (live),
[PHASE4_AUTOMATION_OVERVIEW.md](../../CloudCpReportTesting/PHASE4_AUTOMATION_OVERVIEW.md) (synthetic).
See also [../tools_guide.md](../tools_guide.md).

---

## 5. To Be Added

- Roll live-run results into the register
  [../../CloudCpReportTesting/CloudCpReport_TestCases.xlsx](../../CloudCpReportTesting/CloudCpReport_TestCases.xlsx)
  and integrate both runners into the master harness.
- Replace the P4-06 simulated counter sequence with a real-transfer counter sampler.
- Swap the reference `report_engine.py` for the real verification engine once invokable
  (assertion/case logic unchanged).

Existing today: synthetic reference-engine suite (P4-01…P4-08) + live end-to-end suite
(RT-01…RT-10) with cross-cutting checks, and the register
[../../CloudCpReportTesting/CloudCpReport_TestCases.xlsx](../../CloudCpReportTesting/CloudCpReport_TestCases.xlsx).

Narrative reference: [../../docs/planv2.md](../../docs/planv2.md) Phase 4.
