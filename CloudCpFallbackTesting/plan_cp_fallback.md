# CloudCp Fallback Test Plan

Test plan for exercising the **transfer fallback** path of the Bryck cloud
transfer engine (`cloudcp`) under **injected faults**.

The engine runs a primary transfer client (`transfermanager`). When individual
file uploads/downloads fail or the worker crashes, the engine is expected to
**fall back** and retry those records through the fallback path so the transfer
still finishes. This suite deliberately makes the primary path fail (via the
`TEST` fault-injection block in the service config) and asserts that the
transfer still reaches **`COMPLETED`** with the previously-failing files marked
**`FALLBACK_OK`** in the report.

> **Core case logic (as specified):** with faults injected, if the transfer
> **succeeds**, the fallback works. If the transfer **fails**, the fallback does
> **not** work.

---

## 1. Pipeline Overview

```text
datagen --spec <tier>.yaml                          -> materialise real files under /bryck/cloudcp_fallback/<tier>
edit /etc/bryck/bryckcloud/config.json (TEST block) -> inject faults (FAIL% / CRASH% / SEED)
sudo systemctl restart bcloud.service               -> apply the new TEST + TRANSFER config
bryck_cloud_transfer_initiate.py --mode upload      -> start transfer, capture transfer_id
bryck_cloud_transfer_status.py --transfer-id <id>   -> POLL until terminal state
   (while IN_PROGRESS)                               -> SSH-copy live *.txt.lst + cloudcp_retry_<id>_*.lst  (cloudcp deletes them!)
bryck_cloud_transfer_report.py --cloud-transfer-id  -> download the report ZIP
verify report                                        -> FALLBACK_OK rows present, all files transferred
```

The four client scripts already exist under
[CloudCpReportTesting/bryckclient-cli](../CloudCpReportTesting/bryckclient-cli/)
and read `login.json` + `cloud_ops.json` from that same directory.

---

## 2. Fault-Injection Mechanism

All fault knobs live in the `TEST` block of `/etc/bryck/bryckcloud/config.json`:

```jsonc
"TEST": {
  "SIM_TRANSFER": false,          // leave false — we want REAL transfers, only faulted
  "SIM_SLEEP_TINY_MS": 0,
  "SIM_SLEEP_SMALL_MS": 0,
  "SIM_SLEEP_MEDIUM_MS": 0,
  "SIM_SLEEP_LARGE_MS": 0,
  "SIM_SLEEP_DOWNLOAD_MS": 0,
  "FAULT_FAIL_PERCENT": 0,        // <-- CHANGE: % of records the primary path fails
  "FAULT_CRASH_PERCENT": 0,       // <-- CHANGE: % of records that crash the worker
  "FAULT_CRASH_MODE": "abort",    // keep "abort" (default) for this suite
  "FAULT_SEED": 0                 // <-- CHANGE: fixes which records are hit (reproducible)
}
```

**Only these three fields are edited per test case:** `FAULT_FAIL_PERCENT`,
`FAULT_CRASH_PERCENT`, `FAULT_SEED`. Everything else in `TEST` stays as shown
(`SIM_TRANSFER` stays `false` so the transfer is real — the faults are injected
on top of a real transfer, not a simulation).

Two additional `TRANSFER`-block toggles are exercised by this suite:

| Key                 | Values tested | Purpose                                                            |
|---------------------|---------------|--------------------------------------------------------------------|
| `FALLBACK_ENABLED`  | `True` / `False` | Master switch. `False` = negative cases (transfer must FAIL).   |
| `HI_PERF_OPT`       | `True` / `False` | High-perf path on/off. `False` gets **minimum acceptance** only. |

### Applying a config change

```bash
sudo nano /etc/bryck/bryckcloud/config.json      # edit TEST + TRANSFER values
sudo systemctl restart bcloud.service            # REQUIRED — reload config into the service
```

> Every test case that changes the config **must** restart `bcloud.service`
> before initiating the transfer, otherwise the old values remain live.

---

## 3. Fault Profiles (presets)

Each case selects one profile. `FAULT_SEED` is fixed at **`1337`** for
reproducibility (a second pass with a different seed is an optional robustness
check).

| Profile | FAIL% | CRASH% | Meaning                                                        |
|---------|-------|--------|----------------------------------------------------------------|
| `F0`    | 0     | 0      | Control / baseline — no fault; plain success path.             |
| `F1`    | 10    | 0      | Light per-file failures; fallback retries a few records.       |
| `F2`    | 50    | 0      | Half the records fail on the primary path.                     |
| `F3`    | 100   | 0      | Every record fails the primary path — fallback carries all.    |
| `F4`    | 0     | 10     | Occasional worker crash (`abort`).                             |
| `F5`    | 0     | 50     | Frequent worker crashes.                                       |
| `F6`    | 0     | 100    | Every record crashes the primary worker.                       |
| `F7`    | 100   | 100    | **Saturation extreme** — fail + crash everywhere.              |

`FAULT_CRASH_MODE` remains `"abort"` for all profiles in this suite.

---

## 4. Cloud Configuration (`cloud_ops.json`)

Set the source/destination per case in
[CloudCpReportTesting/bryckclient-cli/cloud_ops.json](../CloudCpReportTesting/bryckclient-cli/cloud_ops.json).
`login.json` (Bryck host/credentials) is assumed already configured.

```jsonc
{
  "cloud_type": "aws",
  "access_key_id": "…",
  "secret_access_key": "…",
  "region": "us-east-1",
  "bryck_src":    "/bryck/cloudcp_fallback/<tier>",   // UPLOAD source  (mounted bryck path)
  "cloud_bucket": "s3://aditya/fallback/<tier>",       // object-store prefix
  "bryck_dst":    "/bryck/cloudcp_fallback_dl/<tier>"  // DOWNLOAD target (mounted bryck path)
}
```

- **Upload** cases use `bryck_src` -> `cloud_bucket` (`--mode upload`).
- **Download** cases use `cloud_bucket` -> `bryck_dst` (`--mode download`), and
  require the objects to already exist in `cloud_bucket` (produced by a prior
  **clean** `F0` upload of the same tier).

---

## 5. Datasets

The datagen spec files live in [spec_files/](spec_files/) (copied from the tier
catalog and retargeted to `/bryck/cloudcp_fallback/<tier>`). Generate with:

```bash
/home/bryck/rperiyas/datagen --spec CloudCpFallbackTesting/spec_files/03_small_files.yaml
```

| # | Spec file                     | Tier / focus                         | Fallback relevance                                  |
|---|-------------------------------|--------------------------------------|-----------------------------------------------------|
| 1 | `01_zero_byte.yaml`           | `zero` — 0-byte files                | Fallback on empty-object records.                   |
| 2 | `02_tiny_files.yaml`          | `tiny` — many small files            | High record count; many fallback retries.           |
| 3 | `03_small_files.yaml`         | `small` — 1–16 MiB (8 MiB cutoff)    | Fallback across single-part/multipart boundary.     |
| 4 | `04_medium_files.yaml`        | `medium` — 64–512 MiB                | Fallback on multi-chunk multipart.                  |
| 5 | `05_large_files.yaml`         | `large` — 1–5 GiB (sparse)           | Fallback on long-running large multipart.           |
| 6 | `06_sparse_files.yaml`        | sparse content across tiers          | Logical-vs-physical size on the fallback path.      |
| 7 | `07_fill_files.yaml`          | deterministic fill (checksum-stable) | Byte-exact verify of fallback-transferred data.     |
| 8 | `08_deep_tree.yaml`           | deep nested paths                    | Long keys survive the fallback retry.               |
| 9 | `09_unicode_names.yaml`       | unicode / emoji / CJK names          | Name round-trip through fallback.                   |
|10 | `10_special_char_names.yaml`  | ASCII special chars / spaces         | Key edge cases through fallback.                     |
|11 | `11_mixed_realistic.yaml`     | weighted realistic mix               | Mixed workload fallback.                             |
|12 | `12_tiny_2million.yaml`       | **scale** — ~2M tiny files           | Fallback under enumeration/retry-list stress.       |

---

## 6. Live Internal-File Verification (critical)

While the transfer is `IN_PROGRESS`, `cloudcp` writes per-batch list files and,
on the fallback path, retry-list files. **`cloudcp` deletes these on completion**,
so they must be **copied off the host while the transfer is still running**:

```text
/opt/bryck/bryckapi/downloads/cloud_transfer_logs/cloud_transfer_<id>/<batch>.txt.lst
/opt/bryck/bryckapi/downloads/cloud_transfer_logs/cloud_transfer_<id>/cloudcp_retry_<id>_*.lst
```

**Procedure per case:**

1. As soon as `initiate` returns the `transfer_id`, begin a poll loop calling
   `bryck_cloud_transfer_status.py --transfer-id <id>`.
2. On **every** poll while state is `IN_PROGRESS`, SSH into the Bryck and copy
   any newly-appeared `*.txt.lst` and `cloudcp_retry_<id>_*.lst` files into the
   run artifacts dir (e.g. `runs/<id>/live_lst/`). These prove the fallback path
   actually engaged (retry lists only appear when records are re-queued).
3. When the state becomes terminal, **stop** copying.
4. Download the report ZIP and verify the files captured in step 2 appear in the
   final report as **transferred** (status `SUCCESS` or `FALLBACK_OK`).

> If **no** `cloudcp_retry_<id>_*.lst` file ever appears for a faulted profile
> (`F1`–`F7`), the fallback path never engaged — that is a **FAIL**, even if the
> transfer otherwise completes.

---

## 7. Report Verification

Download the report and inspect the per-file report(s):

```bash
python3 bryck_cloud_transfer_report.py --cloud-transfer-id <id> --report-path runs/<id>/
```

Report artifacts (under
`/opt/bryck/bryckapi/downloads/cloud_transfer_logs/cloud_transfer_<id>/` and in
the downloaded ZIP):

- `transfer_report_<id>.csv` — per-file rows with a `status` column.
- `report/upload_report.*.csv` — sharded per-file rows.
- `final_report.csv` — merged final view (`AbsoluteFilePath`, `S3Path`, `FileSize`).

**Terminal-success statuses** (a file counts as transferred):
`SUCCESS`, `SKIPPED`, **`FALLBACK_OK`**.

Assertions per positive case:

- Merged terminal-success rows == number of generated source files.
- **At least one** `FALLBACK_OK` row for any faulted profile (`F1`–`F7`).
- `final_report.csv` row count == generated file count.
- Every `S3Path` starts with the requested `cloud_bucket` prefix.
- Reported `FileSize` matches the local source file size.
- `failed_uploads.*` is empty/absent.
- No live `cloudcp_retry_<id>_*.lst` remain on the host after completion.
- Each captured `*.txt.lst` / retry-list file's records appear as transferred.

---

## 8. Positive Test Matrix — Upload (`HI_PERF_OPT=True`, `FALLBACK_ENABLED=True`)

Expected result for every row: transfer reaches **`COMPLETED`**, all files
transferred, faulted records show **`FALLBACK_OK`**.

| Case ID    | Dataset                     | Profile | FAIL/CRASH | Notes                                             |
|------------|-----------------------------|---------|------------|---------------------------------------------------|
| FB-U-01    | `01_zero_byte`              | F0      | 0 / 0      | Control — no fault, must succeed cleanly.         |
| FB-U-02    | `01_zero_byte`              | F3      | 100 / 0    | Empty-object records all via fallback.            |
| FB-U-03    | `02_tiny_files`             | F1      | 10 / 0     | Light fallback, high record count.                |
| FB-U-04    | `02_tiny_files`             | F3      | 100 / 0    | All tiny records via fallback.                    |
| FB-U-05    | `02_tiny_files`             | F6      | 0 / 100    | All-crash recovery on tiny records.               |
| FB-U-06    | `03_small_files`            | F2      | 50 / 0     | Half fail across the 8 MiB cutoff.                |
| FB-U-07    | `03_small_files`            | F3      | 100 / 0    | All small records via fallback.                   |
| FB-U-08    | `03_small_files`            | F7      | 100 / 100  | Saturation on the multipart boundary.             |
| FB-U-09    | `04_medium_files`           | F1      | 10 / 0     | Light fallback on multi-chunk multipart.          |
| FB-U-10    | `04_medium_files`           | F3      | 100 / 0    | All medium multipart via fallback.                |
| FB-U-11    | `04_medium_files`           | F6      | 0 / 100    | All-crash on multipart.                           |
| FB-U-12    | `05_large_files`            | F1      | 10 / 0     | Light fallback on large multipart.                |
| FB-U-13    | `05_large_files`            | F3      | 100 / 0    | All large records via fallback (long-running).    |
| FB-U-14    | `05_large_files`            | F7      | 100 / 100  | Saturation on large multipart.                    |
| FB-U-15    | `06_sparse_files`           | F3      | 100 / 0    | Sparse content on the fallback path.              |
| FB-U-16    | `07_fill_files`             | F3      | 100 / 0    | Checksum-stable fallback (byte-exact verify).     |
| FB-U-17    | `08_deep_tree`              | F3      | 100 / 0    | Long keys survive fallback retry.                 |
| FB-U-18    | `09_unicode_names`          | F3      | 100 / 0    | Unicode names round-trip through fallback.        |
| FB-U-19    | `10_special_char_names`     | F3      | 100 / 0    | Special-char keys through fallback.               |
| FB-U-20    | `11_mixed_realistic`        | F2      | 50 / 0     | Realistic mixed workload, half faulted.           |
| FB-U-21    | `11_mixed_realistic`        | F7      | 100 / 100  | Saturation on realistic mix.                      |
| FB-U-22    | `12_tiny_2million`          | F1      | 10 / 0     | **Scale** — fallback retry-list stress.           |

---

## 9. Positive Test Matrix — Download (`HI_PERF_OPT=True`, `FALLBACK_ENABLED=True`)

Each download case requires the tier's objects to already exist in the bucket
(seed with a prior clean `F0` upload). `--mode download`,
`cloud_bucket` -> `bryck_dst`. Expected: `COMPLETED`, all files landed in
`bryck_dst`, faulted records `FALLBACK_OK`.

| Case ID    | Dataset            | Profile | FAIL/CRASH | Notes                                    |
|------------|--------------------|---------|------------|------------------------------------------|
| FB-D-01    | `03_small_files`   | F0      | 0 / 0      | Control download, no fault.              |
| FB-D-02    | `03_small_files`   | F3      | 100 / 0    | All download records via fallback.       |
| FB-D-03    | `02_tiny_files`    | F1      | 10 / 0     | Light download fallback, many records.   |
| FB-D-04    | `04_medium_files`  | F3      | 100 / 0    | Multipart download via fallback.         |
| FB-D-05    | `05_large_files`   | F6      | 0 / 100    | All-crash recovery on large download.    |
| FB-D-06    | `07_fill_files`    | F3      | 100 / 0    | Byte-exact download verify via fallback. |
| FB-D-07    | `11_mixed_realistic`| F7     | 100 / 100  | Saturation download.                     |

**Download verification** additionally checks that files materialise under
`bryck_dst` with correct sizes (and checksums for `07_fill_files`).

---

## 10. Minimum Acceptance — `HI_PERF_OPT=False`

Set `TRANSFER.HI_PERF_OPT = "False"`, restart `bcloud.service`. One dataset per
tier at a single fault level (`F3`, all-fail — the clearest fallback trigger).
Expected: `COMPLETED` with `FALLBACK_OK` rows.

| Case ID     | Dataset            | HI_PERF_OPT | Profile | Notes                          |
|-------------|--------------------|-------------|---------|--------------------------------|
| FB-HP-01    | `01_zero_byte`     | False       | F3      | zero tier, high-perf off.      |
| FB-HP-02    | `02_tiny_files`    | False       | F3      | tiny tier, high-perf off.      |
| FB-HP-03    | `03_small_files`   | False       | F3      | small tier, high-perf off.     |
| FB-HP-04    | `04_medium_files`  | False       | F3      | medium tier, high-perf off.    |
| FB-HP-05    | `05_large_files`   | False       | F3      | large tier, high-perf off.     |

Restore `HI_PERF_OPT = "True"` and restart the service after this block.

---

## 11. Negative Scenarios

Negative cases assert the fallback is genuinely responsible for recovery, and
that faults surface as real failures when fallback cannot save the transfer.

| Case ID     | Config                                         | Dataset          | Profile | Expected result                                            |
|-------------|------------------------------------------------|------------------|---------|------------------------------------------------------------|
| FB-N-01     | `FALLBACK_ENABLED=False`                       | `03_small_files` | F1      | Transfer **FAILS** (faulted records not recovered).        |
| FB-N-02     | `FALLBACK_ENABLED=False`                       | `02_tiny_files`  | F3      | Transfer **FAILS** — no fallback to carry records.         |
| FB-N-03     | `FALLBACK_ENABLED=False`                       | `04_medium_files`| F6      | Transfer **FAILS** on all-crash, no recovery.              |
| FB-N-04     | `FALLBACK_ENABLED=False`                       | `05_large_files` | F7      | Transfer **FAILS** under saturation, no fallback.          |
| FB-N-05     | `FALLBACK_ENABLED=True`                        | `03_small_files` | F7      | Transfer **COMPLETED** (saturation still rescued) — the positive counterpart to FB-N-01. |
| FB-N-06     | Invalid `cloud_bucket` (nonexistent) + F0      | `01_zero_byte`   | F0      | Clean failure/clear error, no crash/hang.                  |
| FB-N-07     | `FALLBACK_ENABLED=False`, download             | `03_small_files` | F3      | Download **FAILS** with faults and no fallback.            |

> FB-N-01..04/07 are the direct proof of the **case logic**: with fallback
> disabled and faults injected the transfer must **FAIL**; re-enabling fallback
> (same fault profile) must make it **PASS** (FB-N-05 / the §8 rows). Any
> `FALLBACK_ENABLED=False` case that still succeeds is itself a **FAIL** (means
> faults weren't actually injected).

---

## 12. Per-Case Execution Steps

```bash
# --- 1. Generate the dataset (once per tier; skip if already materialised) ---
/home/bryck/rperiyas/datagen --spec CloudCpFallbackTesting/spec_files/03_small_files.yaml

# --- 2. Set cloud_ops.json src/dst for this tier (see §4) ---

# --- 3. Inject the fault profile in /etc/bryck/bryckcloud/config.json ---
#        TEST.FAULT_FAIL_PERCENT / FAULT_CRASH_PERCENT / FAULT_SEED=1337
#        (+ TRANSFER.HI_PERF_OPT / FALLBACK_ENABLED as the case requires)
sudo systemctl restart bcloud.service

# --- 4. Initiate the transfer, capture the transfer_id ---
cd CloudCpReportTesting/bryckclient-cli
python3 bryck_cloud_transfer_initiate.py --mode upload      # or --mode download

# --- 5. Poll to completion; capture live .lst files WHILE IN_PROGRESS (§6) ---
python3 bryck_cloud_transfer_status.py --transfer-id <id>
#   loop; on each IN_PROGRESS poll, SSH-copy:
#     cloud_transfer_<id>/*.txt.lst  and  cloud_transfer_<id>/cloudcp_retry_<id>_*.lst

# --- 6. Download and verify the report (§7) ---
python3 bryck_cloud_transfer_report.py --cloud-transfer-id <id> --report-path ../../CloudCpFallbackTesting/runs/<id>/

# --- 7. Reset the TEST block back to all-zero and restart when done ---
sudo systemctl restart bcloud.service
```

---

## 13. Pass / Fail Criteria

- **Positive (F1–F7, `FALLBACK_ENABLED=True`):** transfer reaches `COMPLETED`;
  every source file is transferred; **≥1 `FALLBACK_OK`** row exists; at least one
  `cloudcp_retry_<id>_*.lst` appeared during the run; no failed uploads; sizes
  (and checksums for `07_fill_files`) match.
- **Control (F0):** transfer `COMPLETED` with all files `SUCCESS`; no
  `FALLBACK_OK` and no retry lists expected.
- **Negative (`FALLBACK_ENABLED=False` + faults):** transfer ends in a terminal
  **failure** state; failures are attributable and reported; **no crash / hang /
  segfault** of the service.
- **Live files:** `*.txt.lst` (and retry lists for faulted profiles) are
  observed during the run and captured before deletion; their records show up as
  transferred in the final report.
- **Minimum acceptance (`HI_PERF_OPT=False`):** the five FB-HP cases behave like
  their `HI_PERF_OPT=True` counterparts (`COMPLETED` + `FALLBACK_OK`).
- **Reproducibility:** with `FAULT_SEED=1337` fixed, the same records are faulted
  across reruns.

---

## 14. Directory Layout

```text
CloudCpFallbackTesting/
  plan_cp_fallback.md          # this document
  spec_files/                  # datagen specs (copied tier catalog, retargeted to /bryck/cloudcp_fallback)
    01_zero_byte.yaml
    02_tiny_files.yaml
    03_small_files.yaml
    04_medium_files.yaml
    05_large_files.yaml
    06_sparse_files.yaml
    07_fill_files.yaml
    08_deep_tree.yaml
    09_unicode_names.yaml
    10_special_char_names.yaml
    11_mixed_realistic.yaml
    12_tiny_2million.yaml
  runs/                        # (generated) per-transfer artifacts:
    <id>/live_lst/             #   captured *.txt.lst + cloudcp_retry_<id>_*.lst
    <id>/cloud_transfer_report_<id>.zip
    <id>/report.json           #   pass/fail summary
```

---

## 15. Planned Test Runner (future work)

A Python harness (`cloudcp_fallback_test.py`) will drive the matrix above. Design
mirrors the existing scheduler/CLI runners:

- **Selection:** `--all`, `--from <ID> --to <ID>` (inclusive range), `--one <ID>`.
- **Per case it will:** generate data (or `--skip-datagen`), patch the `TEST`
  (+ `HI_PERF_OPT` / `FALLBACK_ENABLED`) block, restart `bcloud.service`, initiate
  the transfer, poll status, **capture live `.lst` + retry lists over SSH while
  `IN_PROGRESS`**, download + parse the report, and emit a pass/fail
  `report.json`.
- **Negatives:** `--negative` / `--negative-case <ID>` for the FB-N cases
  (assert terminal failure with fallback disabled), plus the `F0` control and the
  invalid-bucket case.
- **Safety:** always restores the `TEST` block to all-zero and
  `HI_PERF_OPT=True`, `FALLBACK_ENABLED=True` on exit (even on error), and
  restarts the service.
- **Cleanup:** optional `--delete` (materialised data) / `--clear-bucket`
  (uploaded objects) / `--cleanup` (both).

---

## 16. Open Items (please confirm)

1. **`bcloud.service` restart** is the confirmed apply mechanism — confirm the
   service also picks up `TRANSFER.HI_PERF_OPT` / `FALLBACK_ENABLED` changes on
   restart (not just the `TEST` block).
2. **Fault scope:** do `FAULT_FAIL_PERCENT` / `FAULT_CRASH_PERCENT` apply to the
   **download** path as well as upload? (§9 assumes yes.)
3. **`FAULT_CRASH_MODE`:** only `"abort"` is exercised — confirm there are no
   other modes worth covering.
4. **Large/2M scale:** `05_large_files` and `12_tiny_2million` are disk/time
   heavy — confirm they should run in the default matrix or be gated behind a
   `--scale` flag in the runner.
5. **Retry-list guarantee:** confirm that a faulted profile **always** produces
   `cloudcp_retry_<id>_*.lst` (so its absence can be treated as a hard FAIL).
```
