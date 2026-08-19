# Test Plan — Phase 4: Reporting & Verification

**Status:** Draft for team review/sign-off
**Owner:** QA / Engineering (CloudCP test effort)
**Priority:** P0 (release-gating correctness)
**Master plan:** [../TestPlan/complete_plan.md](../TestPlan/complete_plan.md)
**Detailed phase spec:** [../TestPlan/phases/04_reporting.md](../TestPlan/phases/04_reporting.md)
**Narrative reference:** [../docs/planv2.md](../docs/planv2.md) (Phase 4)

---

## 1. Purpose

Validate that, after a transfer runs, the verification engine correctly reconciles what
**should** exist on disk (`source.index`) against what **actually** happened during upload
(the upload report), and produces exactly one, correct, final status per file — with
accurate per-tier and failure-summary rollups. This is the last line of defense that proves
a transfer genuinely succeeded, rather than just "the tool exited 0."

## 2. Scope

**In scope**
- Correctness of the 5-way final status classification: `OK`, `MISSING`, `FAILED`,
  `MISMATCH`, `EXTRA`.
- Ordering/lifecycle guards: verification must refuse to run while scanning is in progress
  or the transfer is paused.
- Merge-join correctness across the full filename/encoding variant set (special characters,
  non-UTF-8 paths, embedded newlines, etc.).
- De-duplication semantics (last-status-wins) when a file has multiple report rows
  (e.g. `SKIPPED` superseded by `FALLBACK_OK`).
- Progress counters (`files_done`, `bytes_done`, `total_files`) monotonicity during a run.
- Per-tier completion summary aggregation and the failure-summary/triage rows.

**Out of scope**
- Correctness of the upload itself (covered by [../CloudCpBinaryTesting](../CloudCpBinaryTesting)
  and [../CloudCpFallbackTesting](../CloudCpFallbackTesting)).
- Batch construction (covered by [../CloudCpBatchBuilderTesting](../CloudCpBatchBuilderTesting)).
- Performance/throughput of the verification engine itself (P2, not a release gate).

## 3. System Under Test

- Verification/reporting engine driven by `/etc/bryck/bryckcloud/config.json` → `VERIFICATION.*`:

| Key | Default used | Notes |
|---|---|---|
| `REPORT_FORMAT` | `"json"` | Also test `"csv"` explicitly (CSV quoting is a pass criterion) |
| `VERIFY_S3_WORKERS` | `16` | Parallel S3 listing workers (BFS per prefix subtree) |
| `VERIFY_STAT_THREADS` | `32` | Parallel local-stat workers for source enumeration |
| `TRANSFER_SUMMARY_FILES` | `/etc/bryck/bryckcloud/transfer_summary_files.json` | Controls summary-zip contents |

- Inputs consumed: `source.index` (from Batch Builder), the merged upload report shards
  (`upload_report.*.csv` from cloudcp + fallback, statuses include `SUCCESS`, `SKIPPED`,
  `FALLBACK_OK`, permanent-failure markers).
- Output: `final_report.csv`/`.json` (`AbsoluteFilePath, S3Path, FileSize, ETag`, + status)
  and a per-tier summary.

## 4. Entry Criteria

- Batch Builder, cloudcp, and Fallback phases pass their own P0 suites (a trustworthy
  `source.index` and upload report are prerequisites — garbage in, garbage out for this phase).
- Test environment has a reachable S3-compatible endpoint (or MinIO) for `HeadObject`/listing
  calls, and permission to mock/stub responses for MISMATCH/EXTRA injection.
- `/etc/bryck/bryckcloud/config.json` reachable and writable for test overrides.

## 5. Exit Criteria

- All 8 P0 test cases (`P4-01`...`P4-08`) pass on both `REPORT_FORMAT=json` and `=csv`.
- No open Sev1/Sev2 defects against status-classification correctness or de-duplication.
- Automation harness (§8) runs unattended and produces a pass/fail report artifact.

## 6. Test Cases (P0)

| ID | Case | Preconditions | Steps | Expected Result (Pass Criteria) |
|---|---|---|---|---|
| **P4-01** | Exactly one correct status per file | Fixture with known counts of all 5 statuses injected (see §7) | 1. Build fixture `source.index` + report rows for a known mix of OK/MISSING/FAILED/MISMATCH/EXTRA, including filenames with embedded newlines/commas. 2. Run verification engine. 3. Parse final report. | Each status's count in the report == injected count. No file appears missing or duplicated. CSV quoting preserves embedded newlines/commas without corrupting other rows. |
| **P4-02** | Refuse verify while scan in progress | `manifest.json.scan_state = in_progress` | 1. Set `scan_state=in_progress`. 2. Invoke verification. 3. Flip `scan_state=complete`. 4. Invoke verification again. | Step 2 fails fast with error `"scan_state=in_progress, cannot verify"` (no partial report written). Step 4 proceeds and completes normally. |
| **P4-03** | Paused transfer does not trigger verify | Transfer mid-flight, `pause_requested=True` set | 1. Start a transfer, set `pause_requested=True` mid-way. 2. Confirm verify is not invoked at the pause point. 3. Resume to natural completion. | Verify only fires at the natural completion barrier after resume — never while paused. |
| **P4-04** | Encoding-safe merge-join | Full filename/encoding variant dataset uploaded (`DS-P4-05`, see §7) | 1. Upload all 81 filename-variant specs (12,550 files spanning tiny/small/medium/large tiers). 2. Run verification. | All variants classified `OK`. Zero false `MISSING`/`MISMATCH` caused by path encoding, special characters, or non-UTF-8 bytes. |
| **P4-05** | De-dup — last status wins | 50 files each with two report rows: an earlier `SKIPPED` (or transient-fail) row and a later `FALLBACK_OK`/`SUCCESS` row | 1. Inject the duplicate-row fixture. 2. Run verification. | Each of the 50 files appears **exactly once** in the final report, classified `OK` (not duplicated as two rows, not misclassified as the stale status). |
| **P4-06** | Progress counters monotonic | Live or simulated transfer with periodic counter snapshots | 1. Sample `files_done`/`bytes_done`/`total_files` every 5s throughout a transfer. 2. Plot/record the series. | `files_done` and `bytes_done` never decrease between samples. `total_files` is non-zero from the first checkpoint onward. |
| **P4-07** | Per-tier completion summary | Mixed-tier dataset (`DS-P6-01`, all 5 tiers) | 1. Run a full transfer + verification over `DS-P6-01`. 2. Inspect per-tier summary section of the report. | Per-tier file counts sum to the dataset total. Batches-created vs batches-completed counts match. Byte totals correct. `avg_batch_duration_sec` populated (non-null) per tier. |
| **P4-08** | Failure-summary triage rows | Permanent (non-retryable) failures injected for a known set of files | 1. Inject N permanently-failed files (exhausted fallback retries). 2. Run verification. | Exactly one triage row per permanently-failed file, containing full context (path, tier, last error, retry count) — no failures silently dropped from the triage list. |

## 7. Datasets & Fixtures Used

| ID | Purpose | Size | Notes |
|---|---|---|---|
| `DS-P4-05` | Encoding-safe merge-join (P4-04) | 12,550 files / 81 specs, cross-tier | Filename/path stress variants (`FN-01`...`FN-20`+) |
| `DS-P5-01` | No file-type mis-reported (supporting P4-01) | 32,110 files / 100 specs, all tiers | "All File Types, All Tiers" |
| `DS-P6-01` | Per-tier summary aggregation (P4-07) | 71,140 files / 41 specs, all tiers | "Profile Comparison (All Tiers)", profile `dt2_100gbe` |
| Status-injection fixture | P4-01, P4-05, P4-08 | Synthetic, small (tens–hundreds of files) | Hand-built `source.index` + report rows; no real upload needed — see automation plan §8 |

Expected/ground-truth counts: [../dataset_cloudcp/spec_files/manifest.json](../dataset_cloudcp/spec_files/manifest.json).
Cross-check tool: `dataset_validator.py` (see [../TestPlan/tools_guide.md](../TestPlan/tools_guide.md)).

## 8. Automation Approach

Three tools are needed (all currently **to be built**, tracked in §9):

1. **`generate_report_fixture.py`** — builds a synthetic `source.index` + fake upload-report
   rows for a controlled mix of statuses (OK/MISSING/FAILED/MISMATCH/EXTRA), including a
   stub `HeadObject` responder so MISMATCH can be forced deterministically without a real
   size-mismatched upload.
2. **`assert_report.py`** — runs the verification engine against a fixture (or a real
   dataset's output), parses the resulting JSON/CSV report, and asserts per-status counts,
   de-duplication, and CSV-quoting correctness.
3. **`progress_sampler.py`** — polls progress counters every 5s during a transfer and
   asserts monotonicity (P4-06).

Orchestrated by `run_reporting_tests.py --case P4-01|--all|--list|--dry-run`, following the
same CLI conventions as `run_cloudcp_tests.py` and `schedular_test.py`.

## 9. Open Items / To Be Added

- [ ] `generate_report_fixture.py` (status-injection fixtures + `HeadObject` stub)
- [ ] `assert_report.py` (report assertion harness)
- [ ] `progress_sampler.py` (progress-counter monotonicity checker)
- [ ] `run_reporting_tests.py` (orchestrator)
- [ ] Confirm exact `source.index` and upload-report-shard file formats/paths on the test host
      (needed before fixtures can be wired to the real engine)
- [ ] Confirm error string/format for the `scan_state=in_progress` refusal (P4-02) so the
      assertion is exact-match rather than substring

## 10. Sign-off

| Role | Name | Date | Approved |
|---|---|---|---|
| QA Lead | | | ☐ |
| Engineering Lead | | | ☐ |
| Product/Stakeholder | | | ☐ |

## 11. Revision History

| Date | Change |
|---|---|
| 2026-08-17 | Initial draft test plan for team review |

