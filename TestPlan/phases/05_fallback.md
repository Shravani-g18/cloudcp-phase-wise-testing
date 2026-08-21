# Phase 5 — Fallback & Retry

[← Back to master plan](../complete_plan.md)

> **What this phase is:** cloudcp is fast but hits S3 errors. Two retry paths catch failures:
> (1) **inline** whole-batch retry in `aws_transfer.py` via a boto3 ProcessPool on rc==1, and
> (2) a persistent **fallback worker** that drains `.lst` files from rc==2 partial failures,
> retries each file, and HeadObject-verifies before marking done. This phase validates that
> **no file is ever silently lost**.

**Priority:** P0 (no data loss).
**Config:** `FALLBACK_*`, `TM_THREAD_POOL_SIZE`, `rc1_retry.*` in the broker config.
**Status:** This phase has a **runnable, fault-injecting suite** in
[../../CloudCpFallbackTesting/](../../CloudCpFallbackTesting/) — API-driven
([plan_cp_fallback.md](../../CloudCpFallbackTesting/plan_cp_fallback.md)) and component-level
([plan_cp_component_fallback.md](../../CloudCpFallbackTesting/plan_cp_component_fallback.md)).

> **Register (Excel):** the full case list is maintained as a shareable workbook at
> [../../CloudCpFallbackTesting/CloudCpFallback_TestCases.xlsx](../../CloudCpFallbackTesting/CloudCpFallback_TestCases.xlsx)
> (sheets: *Overview*, *Fault Profiles*, *Positive Upload*, *Positive Download*,
> *Min Acceptance*, *Negative (API)*, *Component - Worker*, *Component - MP Retry*,
> *Component - Negative*, *Break Conditions*).

---

## 1. Retry Paths

| Trigger | Path | Owner |
|---|---|---|
| cloudcp rc==2 (partial) + `.lst` | Fallback worker drains `.lst`, per-file boto3 retry | `fallback_worker.py` |
| cloudcp rc==1 (whole batch) | Inline ProcessPool boto3 retry, immediately | `aws_transfer.py` |

Every retried file is confirmed with a **HeadObject** size check before `FALLBACK_OK`.

---

## 2. Test Cases (P0)

| ID | Case | How | Pass when |
|---|---|---|---|
| P5-01 | Partial failure `.lst` drained | Inject 1% S3 error rate → `.lst` with ~5k files | `.lst` ingested within 5 s; every file retried; batch → `completed/` |
| P5-02 | Total failure inline retry | Block S3 for one batch, unblock after 10 s | Batch → `completed/` via inline retry; other parallel batches unaffected |
| P5-03 | HeadObject confirm before FALLBACK_OK | Mock wrong size for 10 files | Those 10 not `FALLBACK_OK`; final report `FAILED` |
| P5-04 | Transient vs permanent error policy | Inject SlowDown / InternalError / RequestTimeout / AccessDenied | Transient retried with exponential backoff to `max_attempts`; AccessDenied → 1 attempt, immediate failure |
| P5-05 | Poison file does not block batch | 5 files persistently failing beyond `max_attempts` | 5 in `failed_uploads.<pid>` with `attempt_count=max_attempts`; rest `FALLBACK_OK`; batch completes |
| P5-06 | Fallback crash-restart idempotent | Kill fallback mid-drain, restart | All `.lst` entries end `FALLBACK_OK` or `failed_uploads`; `.lst.done` not re-processed; no double-processing |
| P5-07 | Verify waits for fallback done | Instrument `_fallback_done` + verify start | Verify start timestamp > `_fallback_done` write; fallback doesn't exit before all `.lst` drained |

---

## 2A. Executable Suite (CloudCpFallbackTesting)

The conceptual P5 cases above are realised as two concrete, runnable suites. **Core case
logic:** with faults injected, if the transfer *succeeds* the fallback works; if it *fails*
the fallback does not. Faults are injected through the `TEST` block of `config.json`
(`FAULT_FAIL_PERCENT` / `FAULT_CRASH_PERCENT` / `FAULT_SEED=1337`); each config change
requires `sudo systemctl restart bcloud.service` before initiate. Live `*.txt.lst` and
`cloudcp_retry_<id>_*.lst` files must be SSH-copied **while `IN_PROGRESS`** (cloudcp deletes
them on completion) — their absence for a faulted profile is a hard **FAIL**.

### 2A.1 Fault profiles

| Profile | FAIL% | CRASH% | Meaning |
|---|---|---|---|
| F0 | 0 | 0 | Control / baseline — plain success path |
| F1 | 10 | 0 | Light per-file failures |
| F2 | 50 | 0 | Half the records fail on the primary path |
| F3 | 100 | 0 | Every record fails — fallback carries all |
| F4 / F5 / F6 | 0 | 10 / 50 / 100 | Occasional / frequent / all worker crashes (`abort`) |
| F7 | 100 | 100 | Saturation extreme — fail + crash everywhere |

### 2A.2 API-driven suite (`cloudcp_fallback_test.py`)

| Group | IDs | Count | Expected |
|---|---|---|---|
| Positive Upload (`HI_PERF_OPT=True`, `FALLBACK_ENABLED=True`) | `FB-U-01`…`FB-U-22` | 22 | `COMPLETED`; all files transferred; ≥1 `FALLBACK_OK`; retry `.lst` observed |
| Positive Download (objects pre-seeded via clean F0 upload) | `FB-D-01`…`FB-D-07` | 7 | `COMPLETED`; files land in `bryck_dst` with correct sizes; faulted records `FALLBACK_OK` |
| Minimum Acceptance (`HI_PERF_OPT=False`, one/tier at F3) | `FB-HP-01`…`FB-HP-05` | 5 | `COMPLETED` with `FALLBACK_OK` |
| Negative (`FALLBACK_ENABLED=False` + faults) | `FB-N-01`…`FB-N-07` | 7 | Transfer **FAILS**; re-enabling fallback (same profile) must **PASS** |

### 2A.3 Component-level suite (`cloudcp_component_fallback_test.py`)

Exercises the two internal mechanisms in isolation by staging their exact on-disk inputs.

| Group | IDs | Count | Expected |
|---|---|---|---|
| Fallback worker (`fallback_worker`) | `CFW-U-01`…`CFW-U-12` | 12 | Batch drained clean — all `FALLBACK_OK`, batch → `completed/`, `.lst` → `.lst.done` |
| Whole-batch retry (`mp_batch_retry.retry_whole_batch`) | `CMP-U-01`…`CMP-U-12` | 12 | `ok == file_count`, `failed == 0`, rows `MP_OK` |
| Component negatives (break conditions B1–B9) | `CFW-N-01`…`CFW-N-06`, `CMP-N-01`…`CMP-N-06` | 12 | Crafted on-disk faults degrade gracefully — no hang/crash; silent-failure paths documented |

> Full break-condition analysis (B1–B9: malformed `.lst`, bad bucket, premature done-marker,
> deleted batch, empty/zero-record batch, wrong `fs_prefix`, double-call dedup) is captured in
> the *Break Conditions* sheet of the register and in
> [plan_cp_component_fallback.md §6](../../CloudCpFallbackTesting/plan_cp_component_fallback.md).

---

## 3. Configuration (P0)

| Setting | Value | Validate |
|---|---|---|
| `FALLBACK_ENABLED` | `False` | No fallback spawned; rc==2 failures appear as `FAILED` in final report only |
| `TM_THREAD_POOL_SIZE` | `4` | Fallback uses ≤4 threads during drain |
| `TM_THREAD_POOL_SIZE` | `64` | Fallback uses up to 64 threads; drain throughput scales |
| `rc1_retry.processes` | `4` | Inline retry spawns exactly 4 processes |
| `rc1_retry.threads_per_process` | `8` | Each process uses 8 threads + its own boto3 client |

---

## 4. Datasets & Fault Injection

- Any dataset with enough files to observe drain (e.g. DS-P1-03 small, DS-P6-01 mixed).
- Failures are **injected**, not data-driven: an S3 proxy applies error rates / blocks, and
  HeadObject mocks return wrong sizes.

---

## 5. Tools

- **`cloudcp_fallback_test.py`** — API-driven runner (`--all`, `--from/--to`, `--one`,
  `--negative`); patches the `TEST` + `TRANSFER` block, restarts `bcloud.service`, initiates,
  polls, captures live `.lst`/retry lists over SSH, downloads + verifies the report.
- **`cloudcp_component_fallback_test.py`** — component runner (invoked via
  `cloudcp_fallback_test.py --component*`) that stages the on-disk contract and calls
  `fallback_worker` / `mp_batch_retry.retry_whole_batch()` directly.
- Fault injection is config-driven (the `TEST` block), not a separate proxy.

Runnable plans:
[plan_cp_fallback.md](../../CloudCpFallbackTesting/plan_cp_fallback.md),
[plan_cp_component_fallback.md](../../CloudCpFallbackTesting/plan_cp_component_fallback.md).
See also [../tools_guide.md](../tools_guide.md).

---

## 6. To Be Added

- Finish/wire `cloudcp_fallback_test.py` into the master harness and roll results into the
  register [../../CloudCpFallbackTesting/CloudCpFallback_TestCases.xlsx](../../CloudCpFallbackTesting/CloudCpFallback_TestCases.xlsx).
- Resolve open items in [plan_cp_fallback.md §16](../../CloudCpFallbackTesting/plan_cp_fallback.md)
  (download-path fault scope, HI_PERF_OPT/FALLBACK_ENABLED apply-on-restart confirmation,
  scale-gating for `05_large_files` / `12_tiny_2million`, retry-list guarantee).
- Component-suite remediation follow-ups for the silent-failure break conditions (B4/B6/B8/B9).

Existing today: complete fault-injecting fallback suite (API + component plans, two runners,
tier datasets, per-case evidence/reporting) and the register
[../../CloudCpFallbackTesting/CloudCpFallback_TestCases.xlsx](../../CloudCpFallbackTesting/CloudCpFallback_TestCases.xlsx).

Narrative reference: [../../docs/planv2.md](../../docs/planv2.md) Phase 3.
