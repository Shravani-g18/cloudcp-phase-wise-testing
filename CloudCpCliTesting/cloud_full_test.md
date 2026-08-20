# `cloud_full_test.py` -- Unified Transfer + Negative-Catalog Test Harness

## 1. What this is

`cloud_full_test.py` is a **copy** of [cloud_transfer_only.py](cloud_transfer_only.py) (that
file is untouched) extended into a single entry point that can run:

1. **Positive transfer cases** -- one case per `dataset x mode`
   (`upload`/`download`/`both`), for every dataset in
   `dataset_cloudcp/spec_files/manifest.json` (54 datasets x 3 modes = 162 cases).
   This is the same real engine as `cloud_transfer_only.py`: datagen -> configure
   cloud -> `bryckcloud transfer add aws` -> pause/resume cycles -> poll to
   terminal -> bryck_state/transfer_status capture at every step -> perf
   capture -> optional cleanup.
2. **The full negative catalog** -- all 308 cases from
   `NEGATIVE_TEST_PLAN.md` (`CLI`, `AUTH`, `TID`, `AWS`, `PATH`, `LIFE`,
   `DATASET`, `XFER`, `DOWNLOAD`, `STATE`, `RACE`, `DUP`, `REPORT`, `FAULT`,
   `REC`, `VERIFY`, `INT`, `CLEAN`, `MGMT`, `SVC`, `SM`, `F`) plus the 3 `MASTER-*`
   end-to-end flows -- 311 negative cases total, exactly matching
   `python3 cloudcpclitesting.py --list-negative`'s ID/name list.

   **These are NOT reimplemented here.** Every negative case is delegated
   in-process to [negative_environment_runner.py](negative_environment_runner.py)'s
   `dispatch()` / `EnvironmentManager` -- **this is the actual environment file
   negative cases run through**, not `cloudcpclitesting.py`. That file's own
   `NEG_CATALOG` is used here **only** as a name/order source (so `--list`/
   `--one`/`--from`/`--to` work without needing the still-missing
   `NEGATIVE_TEST_PLAN.md` that `negative_environment_runner.py`'s own catalog
   loader would otherwise require). `negative_environment_runner.py` has real,
   working handlers (`handle_cli`, `handle_life`, `handle_data`, `handle_xfer`,
   `handle_download`, `handle_state`, `handle_race`, `handle_dup`,
   `handle_report`, `handle_fault`, `handle_rec`, `handle_verify`, `handle_int`,
   `handle_clean`, `handle_mgmt`, `handle_svc`, `handle_statematrix`,
   `handle_combo`) for **every section except `MASTER-*`** -- 308 of the 311
   negative cases are genuinely implemented; only the 3 `MASTER-*` end-to-end
   flows (which live in a separate, not-yet-wired file,
   `cloud_transfer_negative_test_runner.py`) still report `BLOCKED`
   ("no handler registered for this case prefix").

Combined catalog: **473 cases** (162 transfer + 311 negative, of which 308
negative cases are genuinely implemented and only 3 -- the `MASTER-*` flows --
are still unwired).


## 2. Environment preparation before every case

Per request, every single case -- transfer **or** negative -- now runs
`prepare_environment()` first, modeled directly on
[negative_environment_runner.py](negative_environment_runner.py)'s
`EnvironmentManager.snapshot()`/`ensure_mounted()` methods. The returned dict
intentionally matches that file's `snapshot()` shape field-for-field, so
reports read the same way across scripts:

```python
{"bryck_state": ..., "cloud_configured": ..., "info_ok": ..., "cloud_ok": ..., "status_ok": ...}
```

Steps:

1. Query `bryck_info.py` for the current state (`info_ok`).
2. If `Ejected`, mount it (`bryck_mount.py`) before anything else runs --
   never assume a mounted baseline (`bryck_state`).
3. Query `bryck_cloud_show.py` and record whether cloud is already configured
   (`cloud_configured`, `cloud_ok`).
4. Query `bryck_cloud_transfer_status.py` to confirm the status endpoint
   itself is reachable (`status_ok`).
5. Log the full snapshot: `environment snapshot: {'bryck_state': ..., 'cloud_configured': ..., 'info_ok': ..., 'cloud_ok': ..., 'status_ok': ...}`.

This guarantees every case starts from a **known** state instead of an assumed
one -- including the ~180 still-stub negative cases (so if/when they get
implemented, they inherit a sane starting point) and every transfer case
(previously, only `cloud_transfer_only.py`'s single-invocation `main()` did
this baseline mount check; the batch loop here didn't -- now it does, before
dataset generation).

This baseline step does **not** replace fixture-specific setup a negative
case's own handler performs internally (e.g. `STATE-*` handlers already call
`ensure_mounted()`/`configure_cloud()` themselves for their own transfer
fixture) -- it runs *before* that, as a first-line guarantee.

For negative cases, the environment-prep snapshot is written to
`results/full_test/<run-id>/negative/<case-id>/env_prep.json` alongside that
case's own `summary.json`/`summary.html` (written by
`cloudcpclitesting.run_negative_suite()`).

## 3. Important invariant: report retrieval must never trigger pause/resume

For **positive** transfer cases specifically: retrieving/checking a
transfer's report or status -- whether the transfer is `IN_PROGRESS`,
`PAUSED`, or just `resumed` -- is **strictly read-only** and must **never**
cause a pause or resume as a side effect. In this script:

- `capture_transfer_status()` / `poll_until_terminal()` only call
  `bryck_cloud_transfer_status.py` (a query), never pause/resume.
- `find_transfer_report_csv()` only reads a CSV the broker already wrote,
  never touches the transfer's state.
- **Only** `pause_resume_transfer()` (gated behind `--pause-resume`) ever
  issues real `bryck_cloud_transfer_pause.py`/`bryck_cloud_transfer_resume.py`
  commands, and it never reads/downloads a report as part of doing so.

This separation is intentional and documented directly in `run_leg()`'s
docstring: a positive test case downloading/checking a summary report while a
transfer is active or paused must not itself pause or resume anything. That
coupling (report-download-during-various-states) is only deliberately
exercised by the **negative** catalog's own `REPORT-*`/`DOWNLOAD-*` cases,
which are a separate, dedicated set of scenarios -- not something the
positive engine does incidentally.

## 4. CLI reference

Modeled on `CloudCpFallbackTesting/cloudcp_fallback_test.py`'s argument surface.

### Selection
| Flag | Meaning |
|---|---|
| `--all` | Run every case (162 transfer + 311 negative). |
| `--one ID[,ID...]` | Run one case, or a comma-separated list, by `[n]` index or literal case id. |
| `--from ID --to ID` | Run an inclusive range in catalog order (index or id). |
| `--negative` | Run only the negative-catalog cases. |
| `--negative-case ID[,ID...]` | Run one/comma-separated negative case(s) only. |
| `--list` | Print the full catalog with `[n]` index/kind/status/name and exit. |

### Execution
| Flag | Meaning |
|---|---|
| `--dry-run` | Print the plan without executing anything. |
| `--manual` | Interactive: prompt `run/skip/quit` before each selected case. |
| `--skip-datagen` | Reuse already-materialized data. |
| `--skip-seed` | For `download` cases, assume the bucket already has objects (skip the seed upload). |
| `--keep-config` | Do not restore the original `cloud_ops.json` after the run. |
| `--skip-cleanup` | Never clean up, even if `--cleanup` is also given. |
| `--seed N` | Random seed for reproducibility (default `1337`). |
| `--poll-interval` / `--poll-timeout` | Status-poll cadence / overall wait timeout. |
| `--verbose` | Debug-level logging. |

### Paths / hosts
`--cli-dir`, `--spec-dir`, `--out-dir`, `--login`, `--cloud-ops`, `--datagen`,
`--config`, `--service` (reserved for future `SVC-*` implementations),
`--transfer-logs-dir`, `--endpoint-url`, `--src-base`, `--bucket-base`, `--dl-base`.

### Component tests -- **NOT IMPLEMENTED**
`--component`, `--component-one`, `--component-negative`, `--component-list`,
`--heavy`, `--component-bucket`, `--region`, `--venv-python`,
`--batchmeta-dir`, `--pool-size` are accepted (matching
`cloudcp_fallback_test.py`'s surface) but **refuse to run**, printing a clear
error and returning exit code `2`. The `fallback_worker`/`mp_batch_retry`
internal-mechanism tests they refer to have no source anywhere in this repo
-- only their CLI surface was described -- so this script does not fabricate
fake results for them.

## 5. Examples

```bash
# See the full 473-case catalog
python3 cloud_full_test.py --list

# Dry-run everything first
python3 cloud_full_test.py --all --dry-run

# Run one transfer case
python3 cloud_full_test.py --one TRANSFER-DS-P8-01-UPLOAD

# Run one negative case
python3 cloud_full_test.py --negative-case AWS-03

# Run every negative case
python3 cloud_full_test.py --negative

# Run a contiguous range by catalog position
python3 cloud_full_test.py --from 1 --to 9

# Interactive: confirm each case before running it
python3 cloud_full_test.py --manual --negative
```

## 6. Reports

- Transfer cases: `results/full_test/<run-id>/transfer/<case-id>/{upload,download}/`
  -- same `commands.log`/`summary.json`/`summary.md`/`summary.html` +
  perf HTML/JSON/zip as `cloud_transfer_only.py`.
- Negative cases: `results/full_test/<run-id>/negative/<case-id>/env_prep.json`
  (this script's baseline-environment addition) plus a single combined report
  for **all** negative cases selected in the run:
  `results/full_test/<run-id>/negative/combined_results.json` and
  `combined_report.html` -- written with
  `negative_environment_runner.build_html()` (the same rich, searchable/
  filterable/sortable report that script itself produces, with full
  before/after environment snapshots and command/stdout/stderr detail per case).
- Top-level `results/full_test/<run-id>/report.json` -- one row per selected
  case with its final status.

## 7. Appendix: full 473-case catalog

Exact output of `python3 cloud_full_test.py --list`, included in full so
nothing from the catalog is left undocumented. `[n]` is the order number used
by `--from`/`--to`.

```text
   [n] ID                          KIND      STATUS      NAME
--------------------------------------------------------------------------------------------------------------
[   1] TRANSFER-DS-P1-01-UPLOAD    transfer  IMPLEMENTED upload transfer of DS-P1-01
[   2] TRANSFER-DS-P1-01-DOWNLOAD  transfer  IMPLEMENTED download transfer of DS-P1-01
[   3] TRANSFER-DS-P1-01-BOTH      transfer  IMPLEMENTED both transfer of DS-P1-01
[   4] TRANSFER-DS-P1-02-UPLOAD    transfer  IMPLEMENTED upload transfer of DS-P1-02
[   5] TRANSFER-DS-P1-02-DOWNLOAD  transfer  IMPLEMENTED download transfer of DS-P1-02
[   6] TRANSFER-DS-P1-02-BOTH      transfer  IMPLEMENTED both transfer of DS-P1-02
[   7] TRANSFER-DS-P1-03-UPLOAD    transfer  IMPLEMENTED upload transfer of DS-P1-03
[   8] TRANSFER-DS-P1-03-DOWNLOAD  transfer  IMPLEMENTED download transfer of DS-P1-03
[   9] TRANSFER-DS-P1-03-BOTH      transfer  IMPLEMENTED both transfer of DS-P1-03
[  10] TRANSFER-DS-P1-04-UPLOAD    transfer  IMPLEMENTED upload transfer of DS-P1-04
[  11] TRANSFER-DS-P1-04-DOWNLOAD  transfer  IMPLEMENTED download transfer of DS-P1-04
[  12] TRANSFER-DS-P1-04-BOTH      transfer  IMPLEMENTED both transfer of DS-P1-04
[  13] TRANSFER-DS-P1-05-UPLOAD    transfer  IMPLEMENTED upload transfer of DS-P1-05
[  14] TRANSFER-DS-P1-05-DOWNLOAD  transfer  IMPLEMENTED download transfer of DS-P1-05
[  15] TRANSFER-DS-P1-05-BOTH      transfer  IMPLEMENTED both transfer of DS-P1-05
[  16] TRANSFER-DS-P1-06-UPLOAD    transfer  IMPLEMENTED upload transfer of DS-P1-06
[  17] TRANSFER-DS-P1-06-DOWNLOAD  transfer  IMPLEMENTED download transfer of DS-P1-06
[  18] TRANSFER-DS-P1-06-BOTH      transfer  IMPLEMENTED both transfer of DS-P1-06
[  19] TRANSFER-DS-P2-01-UPLOAD    transfer  IMPLEMENTED upload transfer of DS-P2-01
[  20] TRANSFER-DS-P2-01-DOWNLOAD  transfer  IMPLEMENTED download transfer of DS-P2-01
[  21] TRANSFER-DS-P2-01-BOTH      transfer  IMPLEMENTED both transfer of DS-P2-01
[  22] TRANSFER-DS-P2-02-UPLOAD    transfer  IMPLEMENTED upload transfer of DS-P2-02
[  23] TRANSFER-DS-P2-02-DOWNLOAD  transfer  IMPLEMENTED download transfer of DS-P2-02
[  24] TRANSFER-DS-P2-02-BOTH      transfer  IMPLEMENTED both transfer of DS-P2-02
[  25] TRANSFER-DS-P2-03-UPLOAD    transfer  IMPLEMENTED upload transfer of DS-P2-03
[  26] TRANSFER-DS-P2-03-DOWNLOAD  transfer  IMPLEMENTED download transfer of DS-P2-03
[  27] TRANSFER-DS-P2-03-BOTH      transfer  IMPLEMENTED both transfer of DS-P2-03
[  28] TRANSFER-DS-P2-04-UPLOAD    transfer  IMPLEMENTED upload transfer of DS-P2-04
[  29] TRANSFER-DS-P2-04-DOWNLOAD  transfer  IMPLEMENTED download transfer of DS-P2-04
[  30] TRANSFER-DS-P2-04-BOTH      transfer  IMPLEMENTED both transfer of DS-P2-04
[  31] TRANSFER-DS-P2-05-UPLOAD    transfer  IMPLEMENTED upload transfer of DS-P2-05
[  32] TRANSFER-DS-P2-05-DOWNLOAD  transfer  IMPLEMENTED download transfer of DS-P2-05
[  33] TRANSFER-DS-P2-05-BOTH      transfer  IMPLEMENTED both transfer of DS-P2-05
[  34] TRANSFER-DS-P2-06-UPLOAD    transfer  IMPLEMENTED upload transfer of DS-P2-06
[  35] TRANSFER-DS-P2-06-DOWNLOAD  transfer  IMPLEMENTED download transfer of DS-P2-06
[  36] TRANSFER-DS-P2-06-BOTH      transfer  IMPLEMENTED both transfer of DS-P2-06
[  37] TRANSFER-DS-P2-07-UPLOAD    transfer  IMPLEMENTED upload transfer of DS-P2-07
[  38] TRANSFER-DS-P2-07-DOWNLOAD  transfer  IMPLEMENTED download transfer of DS-P2-07
[  39] TRANSFER-DS-P2-07-BOTH      transfer  IMPLEMENTED both transfer of DS-P2-07
[  40] TRANSFER-DS-P3-01-UPLOAD    transfer  IMPLEMENTED upload transfer of DS-P3-01
[  41] TRANSFER-DS-P3-01-DOWNLOAD  transfer  IMPLEMENTED download transfer of DS-P3-01
[  42] TRANSFER-DS-P3-01-BOTH      transfer  IMPLEMENTED both transfer of DS-P3-01
[  43] TRANSFER-DS-P3-02-UPLOAD    transfer  IMPLEMENTED upload transfer of DS-P3-02
[  44] TRANSFER-DS-P3-02-DOWNLOAD  transfer  IMPLEMENTED download transfer of DS-P3-02
[  45] TRANSFER-DS-P3-02-BOTH      transfer  IMPLEMENTED both transfer of DS-P3-02
[  46] TRANSFER-DS-P3-03-UPLOAD    transfer  IMPLEMENTED upload transfer of DS-P3-03
[  47] TRANSFER-DS-P3-03-DOWNLOAD  transfer  IMPLEMENTED download transfer of DS-P3-03
[  48] TRANSFER-DS-P3-03-BOTH      transfer  IMPLEMENTED both transfer of DS-P3-03
[  49] TRANSFER-DS-P3-04-UPLOAD    transfer  IMPLEMENTED upload transfer of DS-P3-04
[  50] TRANSFER-DS-P3-04-DOWNLOAD  transfer  IMPLEMENTED download transfer of DS-P3-04
[  51] TRANSFER-DS-P3-04-BOTH      transfer  IMPLEMENTED both transfer of DS-P3-04
[  52] TRANSFER-DS-P3-05-UPLOAD    transfer  IMPLEMENTED upload transfer of DS-P3-05
[  53] TRANSFER-DS-P3-05-DOWNLOAD  transfer  IMPLEMENTED download transfer of DS-P3-05
[  54] TRANSFER-DS-P3-05-BOTH      transfer  IMPLEMENTED both transfer of DS-P3-05
[  55] TRANSFER-DS-P3-06-UPLOAD    transfer  IMPLEMENTED upload transfer of DS-P3-06
[  56] TRANSFER-DS-P3-06-DOWNLOAD  transfer  IMPLEMENTED download transfer of DS-P3-06
[  57] TRANSFER-DS-P3-06-BOTH      transfer  IMPLEMENTED both transfer of DS-P3-06
[  58] TRANSFER-DS-P4-01-UPLOAD    transfer  IMPLEMENTED upload transfer of DS-P4-01
[  59] TRANSFER-DS-P4-01-DOWNLOAD  transfer  IMPLEMENTED download transfer of DS-P4-01
[  60] TRANSFER-DS-P4-01-BOTH      transfer  IMPLEMENTED both transfer of DS-P4-01
[  61] TRANSFER-DS-P4-02-UPLOAD    transfer  IMPLEMENTED upload transfer of DS-P4-02
[  62] TRANSFER-DS-P4-02-DOWNLOAD  transfer  IMPLEMENTED download transfer of DS-P4-02
[  63] TRANSFER-DS-P4-02-BOTH      transfer  IMPLEMENTED both transfer of DS-P4-02
[  64] TRANSFER-DS-P4-03-UPLOAD    transfer  IMPLEMENTED upload transfer of DS-P4-03
[  65] TRANSFER-DS-P4-03-DOWNLOAD  transfer  IMPLEMENTED download transfer of DS-P4-03
[  66] TRANSFER-DS-P4-03-BOTH      transfer  IMPLEMENTED both transfer of DS-P4-03
[  67] TRANSFER-DS-P4-04-UPLOAD    transfer  IMPLEMENTED upload transfer of DS-P4-04
[  68] TRANSFER-DS-P4-04-DOWNLOAD  transfer  IMPLEMENTED download transfer of DS-P4-04
[  69] TRANSFER-DS-P4-04-BOTH      transfer  IMPLEMENTED both transfer of DS-P4-04
[  70] TRANSFER-DS-P4-05-UPLOAD    transfer  IMPLEMENTED upload transfer of DS-P4-05
[  71] TRANSFER-DS-P4-05-DOWNLOAD  transfer  IMPLEMENTED download transfer of DS-P4-05
[  72] TRANSFER-DS-P4-05-BOTH      transfer  IMPLEMENTED both transfer of DS-P4-05
[  73] TRANSFER-DS-P5-01-UPLOAD    transfer  IMPLEMENTED upload transfer of DS-P5-01
[  74] TRANSFER-DS-P5-01-DOWNLOAD  transfer  IMPLEMENTED download transfer of DS-P5-01
[  75] TRANSFER-DS-P5-01-BOTH      transfer  IMPLEMENTED both transfer of DS-P5-01
[  76] TRANSFER-DS-P6-01-UPLOAD    transfer  IMPLEMENTED upload transfer of DS-P6-01
[  77] TRANSFER-DS-P6-01-DOWNLOAD  transfer  IMPLEMENTED download transfer of DS-P6-01
[  78] TRANSFER-DS-P6-01-BOTH      transfer  IMPLEMENTED both transfer of DS-P6-01
[  79] TRANSFER-DS-P7-01-UPLOAD    transfer  IMPLEMENTED upload transfer of DS-P7-01
[  80] TRANSFER-DS-P7-01-DOWNLOAD  transfer  IMPLEMENTED download transfer of DS-P7-01
[  81] TRANSFER-DS-P7-01-BOTH      transfer  IMPLEMENTED both transfer of DS-P7-01
[  82] TRANSFER-DS-P7-02-UPLOAD    transfer  IMPLEMENTED upload transfer of DS-P7-02
[  83] TRANSFER-DS-P7-02-DOWNLOAD  transfer  IMPLEMENTED download transfer of DS-P7-02
[  84] TRANSFER-DS-P7-02-BOTH      transfer  IMPLEMENTED both transfer of DS-P7-02
[  85] TRANSFER-DS-P7-03-UPLOAD    transfer  IMPLEMENTED upload transfer of DS-P7-03
[  86] TRANSFER-DS-P7-03-DOWNLOAD  transfer  IMPLEMENTED download transfer of DS-P7-03
[  87] TRANSFER-DS-P7-03-BOTH      transfer  IMPLEMENTED both transfer of DS-P7-03
[  88] TRANSFER-DS-P8-01-UPLOAD    transfer  IMPLEMENTED upload transfer of DS-P8-01
[  89] TRANSFER-DS-P8-01-DOWNLOAD  transfer  IMPLEMENTED download transfer of DS-P8-01
[  90] TRANSFER-DS-P8-01-BOTH      transfer  IMPLEMENTED both transfer of DS-P8-01
[  91] TRANSFER-DS-P8-02-UPLOAD    transfer  IMPLEMENTED upload transfer of DS-P8-02
[  92] TRANSFER-DS-P8-02-DOWNLOAD  transfer  IMPLEMENTED download transfer of DS-P8-02
[  93] TRANSFER-DS-P8-02-BOTH      transfer  IMPLEMENTED both transfer of DS-P8-02
[  94] TRANSFER-DS-P8-03-UPLOAD    transfer  IMPLEMENTED upload transfer of DS-P8-03
[  95] TRANSFER-DS-P8-03-DOWNLOAD  transfer  IMPLEMENTED download transfer of DS-P8-03
[  96] TRANSFER-DS-P8-03-BOTH      transfer  IMPLEMENTED both transfer of DS-P8-03
[  97] TRANSFER-DS-P8-04-UPLOAD    transfer  IMPLEMENTED upload transfer of DS-P8-04
[  98] TRANSFER-DS-P8-04-DOWNLOAD  transfer  IMPLEMENTED download transfer of DS-P8-04
[  99] TRANSFER-DS-P8-04-BOTH      transfer  IMPLEMENTED both transfer of DS-P8-04
[ 100] TRANSFER-DS-P8-05-UPLOAD    transfer  IMPLEMENTED upload transfer of DS-P8-05
[ 101] TRANSFER-DS-P8-05-DOWNLOAD  transfer  IMPLEMENTED download transfer of DS-P8-05
[ 102] TRANSFER-DS-P8-05-BOTH      transfer  IMPLEMENTED both transfer of DS-P8-05
[ 103] TRANSFER-DS-P9-01-UPLOAD    transfer  IMPLEMENTED upload transfer of DS-P9-01
[ 104] TRANSFER-DS-P9-01-DOWNLOAD  transfer  IMPLEMENTED download transfer of DS-P9-01
[ 105] TRANSFER-DS-P9-01-BOTH      transfer  IMPLEMENTED both transfer of DS-P9-01
[ 106] TRANSFER-DS-P9-02-UPLOAD    transfer  IMPLEMENTED upload transfer of DS-P9-02
[ 107] TRANSFER-DS-P9-02-DOWNLOAD  transfer  IMPLEMENTED download transfer of DS-P9-02
[ 108] TRANSFER-DS-P9-02-BOTH      transfer  IMPLEMENTED both transfer of DS-P9-02
[ 109] TRANSFER-DS-P9-03-UPLOAD    transfer  IMPLEMENTED upload transfer of DS-P9-03
[ 110] TRANSFER-DS-P9-03-DOWNLOAD  transfer  IMPLEMENTED download transfer of DS-P9-03
[ 111] TRANSFER-DS-P9-03-BOTH      transfer  IMPLEMENTED both transfer of DS-P9-03
[ 112] TRANSFER-DS-P9-04-UPLOAD    transfer  IMPLEMENTED upload transfer of DS-P9-04
[ 113] TRANSFER-DS-P9-04-DOWNLOAD  transfer  IMPLEMENTED download transfer of DS-P9-04
[ 114] TRANSFER-DS-P9-04-BOTH      transfer  IMPLEMENTED both transfer of DS-P9-04
[ 115] TRANSFER-DS-P9-05-UPLOAD    transfer  IMPLEMENTED upload transfer of DS-P9-05
[ 116] TRANSFER-DS-P9-05-DOWNLOAD  transfer  IMPLEMENTED download transfer of DS-P9-05
[ 117] TRANSFER-DS-P9-05-BOTH      transfer  IMPLEMENTED both transfer of DS-P9-05
[ 118] TRANSFER-DS-P9-06-UPLOAD    transfer  IMPLEMENTED upload transfer of DS-P9-06
[ 119] TRANSFER-DS-P9-06-DOWNLOAD  transfer  IMPLEMENTED download transfer of DS-P9-06
[ 120] TRANSFER-DS-P9-06-BOTH      transfer  IMPLEMENTED both transfer of DS-P9-06
[ 121] TRANSFER-DS-P9-07-UPLOAD    transfer  IMPLEMENTED upload transfer of DS-P9-07
[ 122] TRANSFER-DS-P9-07-DOWNLOAD  transfer  IMPLEMENTED download transfer of DS-P9-07
[ 123] TRANSFER-DS-P9-07-BOTH      transfer  IMPLEMENTED both transfer of DS-P9-07
[ 124] TRANSFER-DS-P10-01-UPLOAD   transfer  IMPLEMENTED upload transfer of DS-P10-01
[ 125] TRANSFER-DS-P10-01-DOWNLOAD transfer  IMPLEMENTED download transfer of DS-P10-01
[ 126] TRANSFER-DS-P10-01-BOTH     transfer  IMPLEMENTED both transfer of DS-P10-01
[ 127] TRANSFER-DS-P10-02-UPLOAD   transfer  IMPLEMENTED upload transfer of DS-P10-02
[ 128] TRANSFER-DS-P10-02-DOWNLOAD transfer  IMPLEMENTED download transfer of DS-P10-02
[ 129] TRANSFER-DS-P10-02-BOTH     transfer  IMPLEMENTED both transfer of DS-P10-02
[ 130] TRANSFER-DS-P10-03-UPLOAD   transfer  IMPLEMENTED upload transfer of DS-P10-03
[ 131] TRANSFER-DS-P10-03-DOWNLOAD transfer  IMPLEMENTED download transfer of DS-P10-03
[ 132] TRANSFER-DS-P10-03-BOTH     transfer  IMPLEMENTED both transfer of DS-P10-03
[ 133] TRANSFER-DS-P10-04-UPLOAD   transfer  IMPLEMENTED upload transfer of DS-P10-04
[ 134] TRANSFER-DS-P10-04-DOWNLOAD transfer  IMPLEMENTED download transfer of DS-P10-04
[ 135] TRANSFER-DS-P10-04-BOTH     transfer  IMPLEMENTED both transfer of DS-P10-04
[ 136] TRANSFER-DS-P10-05-UPLOAD   transfer  IMPLEMENTED upload transfer of DS-P10-05
[ 137] TRANSFER-DS-P10-05-DOWNLOAD transfer  IMPLEMENTED download transfer of DS-P10-05
[ 138] TRANSFER-DS-P10-05-BOTH     transfer  IMPLEMENTED both transfer of DS-P10-05
[ 139] TRANSFER-DS-P10-06-UPLOAD   transfer  IMPLEMENTED upload transfer of DS-P10-06
[ 140] TRANSFER-DS-P10-06-DOWNLOAD transfer  IMPLEMENTED download transfer of DS-P10-06
[ 141] TRANSFER-DS-P10-06-BOTH     transfer  IMPLEMENTED both transfer of DS-P10-06
[ 142] TRANSFER-DS-P10-07-UPLOAD   transfer  IMPLEMENTED upload transfer of DS-P10-07
[ 143] TRANSFER-DS-P10-07-DOWNLOAD transfer  IMPLEMENTED download transfer of DS-P10-07
[ 144] TRANSFER-DS-P10-07-BOTH     transfer  IMPLEMENTED both transfer of DS-P10-07
[ 145] TRANSFER-DS-P10-08-UPLOAD   transfer  IMPLEMENTED upload transfer of DS-P10-08
[ 146] TRANSFER-DS-P10-08-DOWNLOAD transfer  IMPLEMENTED download transfer of DS-P10-08
[ 147] TRANSFER-DS-P10-08-BOTH     transfer  IMPLEMENTED both transfer of DS-P10-08
[ 148] TRANSFER-DS-P11-01-UPLOAD   transfer  IMPLEMENTED upload transfer of DS-P11-01
[ 149] TRANSFER-DS-P11-01-DOWNLOAD transfer  IMPLEMENTED download transfer of DS-P11-01
[ 150] TRANSFER-DS-P11-01-BOTH     transfer  IMPLEMENTED both transfer of DS-P11-01
[ 151] TRANSFER-DS-P11-02-UPLOAD   transfer  IMPLEMENTED upload transfer of DS-P11-02
[ 152] TRANSFER-DS-P11-02-DOWNLOAD transfer  IMPLEMENTED download transfer of DS-P11-02
[ 153] TRANSFER-DS-P11-02-BOTH     transfer  IMPLEMENTED both transfer of DS-P11-02
[ 154] TRANSFER-DS-P11-03-UPLOAD   transfer  IMPLEMENTED upload transfer of DS-P11-03
[ 155] TRANSFER-DS-P11-03-DOWNLOAD transfer  IMPLEMENTED download transfer of DS-P11-03
[ 156] TRANSFER-DS-P11-03-BOTH     transfer  IMPLEMENTED both transfer of DS-P11-03
[ 157] TRANSFER-DS-P12-01-UPLOAD   transfer  IMPLEMENTED upload transfer of DS-P12-01
[ 158] TRANSFER-DS-P12-01-DOWNLOAD transfer  IMPLEMENTED download transfer of DS-P12-01
[ 159] TRANSFER-DS-P12-01-BOTH     transfer  IMPLEMENTED both transfer of DS-P12-01
[ 160] TRANSFER-DS-P12-02-UPLOAD   transfer  IMPLEMENTED upload transfer of DS-P12-02
[ 161] TRANSFER-DS-P12-02-DOWNLOAD transfer  IMPLEMENTED download transfer of DS-P12-02
[ 162] TRANSFER-DS-P12-02-BOTH     transfer  IMPLEMENTED both transfer of DS-P12-02
[ 163] CLI-01                      negative  IMPLEMENTED Initiate without --mode
[ 164] CLI-02                      negative  IMPLEMENTED Invalid mode
[ 165] CLI-03                      negative  IMPLEMENTED Upload without bryck_src
[ 166] CLI-04                      negative  IMPLEMENTED Upload without cloud_bucket
[ 167] CLI-05                      negative  IMPLEMENTED Download without bryck_dst
[ 168] CLI-06                      negative  IMPLEMENTED Missing login file
[ 169] CLI-07                      negative  IMPLEMENTED Malformed login JSON
[ 170] CLI-08                      negative  IMPLEMENTED Invalid transfer id operation
[ 171] CLI-09                      negative  IMPLEMENTED Missing dataset spec
[ 172] AUTH-01                     negative  IMPLEMENTED Invalid username
[ 173] AUTH-02                     negative  IMPLEMENTED Invalid password
[ 174] AUTH-03                     negative  IMPLEMENTED Invalid access token
[ 175] AUTH-04                     negative  IMPLEMENTED Expired token
[ 176] AUTH-05                     negative  IMPLEMENTED Missing authentication token
[ 177] AUTH-06                     negative  IMPLEMENTED Request after session expiry
[ 178] AUTH-07                     negative  IMPLEMENTED Transfer operation after expiry
[ 179] AUTH-08                     negative  IMPLEMENTED Pause after expiry
[ 180] AUTH-09                     negative  IMPLEMENTED Resume after expiry
[ 181] AUTH-10                     negative  IMPLEMENTED Cancel after expiry
[ 182] TID-01                      negative  IMPLEMENTED Transfer ID validation ('99999999')
[ 183] TID-02                      negative  IMPLEMENTED Transfer ID validation ('')
[ 184] TID-03                      negative  IMPLEMENTED Transfer ID validation ('-1')
[ 185] TID-04                      negative  IMPLEMENTED Transfer ID validation ('not-a-transfer')
[ 186] TID-05                      negative  IMPLEMENTED Transfer ID validation ('!@#$%^&*')
[ 187] TID-06                      negative  IMPLEMENTED Transfer ID validation ('999999999999999999999999999999999999')
[ 188] TID-07                      negative  IMPLEMENTED Transfer ID validation ('2147483647')
[ 189] TID-08                      negative  IMPLEMENTED Transfer ID validation ('1')
[ 190] TID-09                      negative  IMPLEMENTED Transfer ID validation ('1.2.3')
[ 191] AWS-01                      negative  IMPLEMENTED Missing access key
[ 192] AWS-02                      negative  IMPLEMENTED Missing secret key
[ 193] AWS-03                      negative  IMPLEMENTED Invalid access key
[ 194] AWS-04                      negative  IMPLEMENTED Invalid secret key
[ 195] AWS-05                      negative  IMPLEMENTED Invalid region
[ 196] AWS-06                      negative  IMPLEMENTED Invalid endpoint
[ 197] AWS-07                      negative  IMPLEMENTED Invalid bucket URI
[ 198] AWS-08                      negative  IMPLEMENTED Nonexistent bucket
[ 199] AWS-09                      negative  IMPLEMENTED Inaccessible bucket
[ 200] AWS-10                      negative  IMPLEMENTED Missing List permission
[ 201] AWS-11                      negative  IMPLEMENTED Missing PutObject permission
[ 202] AWS-12                      negative  IMPLEMENTED Missing GetObject permission
[ 203] AWS-13                      negative  IMPLEMENTED Deconfigure when not configured
[ 204] AWS-14                      negative  IMPLEMENTED Deconfigure twice
[ 205] AWS-15                      negative  IMPLEMENTED Deconfigure during active transfer
[ 206] AWS-16                      negative  IMPLEMENTED Deconfigure while paused
[ 207] AWS-17                      negative  IMPLEMENTED Reconfigure during active transfer
[ 208] AWS-18                      negative  IMPLEMENTED Reconfigure during paused transfer
[ 209] PATH-01                     negative  IMPLEMENTED Invalid destination prefix
[ 210] PATH-02                     negative  IMPLEMENTED Empty prefix
[ 211] PATH-03                     negative  IMPLEMENTED Leading slash
[ 212] PATH-04                     negative  IMPLEMENTED Double slash
[ 213] PATH-05                     negative  IMPLEMENTED Special characters/spaces
[ 214] PATH-06                     negative  IMPLEMENTED Unicode prefix
[ 215] PATH-07                     negative  IMPLEMENTED Very long prefix
[ 216] PATH-08                     negative  IMPLEMENTED Parent traversal
[ 217] PATH-09                     negative  IMPLEMENTED Source/config mismatch
[ 218] LIFE-01                     negative  IMPLEMENTED Info unavailable
[ 219] LIFE-02                     negative  IMPLEMENTED Mount before format
[ 220] LIFE-03                     negative  IMPLEMENTED Missing mount params
[ 221] LIFE-04                     negative  IMPLEMENTED Invalid mount params
[ 222] LIFE-05                     negative  IMPLEMENTED Mount already mounted
[ 223] LIFE-06                     negative  IMPLEMENTED Format while mounted
[ 224] LIFE-07                     negative  IMPLEMENTED Invalid format params
[ 225] LIFE-08                     negative  IMPLEMENTED Format unavailable
[ 226] LIFE-09                     negative  IMPLEMENTED Eject already ejected
[ 227] LIFE-10                     negative  IMPLEMENTED Eject active transfer
[ 228] LIFE-11                     negative  IMPLEMENTED Eject paused transfer
[ 229] LIFE-12                     negative  IMPLEMENTED Erase mounted
[ 230] LIFE-13                     negative  IMPLEMENTED Format/erase/remove during transfer
[ 231] LIFE-14                     negative  IMPLEMENTED Mount during transfer
[ 232] LIFE-15                     negative  IMPLEMENTED Format during verification
[ 233] LIFE-16                     negative  IMPLEMENTED Eject during cancellation
[ 234] DATA-01                     negative  IMPLEMENTED Generate while ejected
[ 235] DATA-02                     negative  IMPLEMENTED Generate while unmounted
[ 236] DATA-03                     negative  IMPLEMENTED Generate while active
[ 237] DATA-04                     negative  IMPLEMENTED Generate while paused
[ 238] DATA-05                     negative  IMPLEMENTED Missing specification
[ 239] DATA-06                     negative  IMPLEMENTED Invalid specification
[ 240] DATA-07                     negative  IMPLEMENTED Invalid/negative size
[ 241] DATA-08                     negative  IMPLEMENTED Insufficient storage
[ 242] DATA-09                     negative  IMPLEMENTED Outside /bryck
[ 243] DATA-10                     negative  IMPLEMENTED Inaccessible files
[ 244] DATA-11                     negative  IMPLEMENTED Interrupted generation
[ 245] DATA-12                     negative  IMPLEMENTED Duplicate generation
[ 246] XFER-01                     negative  IMPLEMENTED Upload while ejected/unmounted
[ 247] XFER-02                     negative  IMPLEMENTED Cloud not configured
[ 248] XFER-03                     negative  IMPLEMENTED Invalid source path
[ 249] XFER-04                     negative  IMPLEMENTED Empty source directory
[ 250] XFER-05                     negative  IMPLEMENTED Inaccessible source
[ 251] XFER-06                     negative  IMPLEMENTED Nonexistent bucket
[ 252] XFER-07                     negative  IMPLEMENTED Invalid cloud object path
[ 253] XFER-08                     negative  IMPLEMENTED Invalid download destination
[ 254] XFER-09                     negative  IMPLEMENTED Upload while download active
[ 255] XFER-10                     negative  IMPLEMENTED Download while upload active
[ 256] XFER-11                     negative  IMPLEMENTED Pause immediately
[ 257] XFER-12                     negative  IMPLEMENTED Resume before pause
[ 258] XFER-13                     negative  IMPLEMENTED Pause twice
[ 259] XFER-14                     negative  IMPLEMENTED Resume twice
[ 260] XFER-15                     negative  IMPLEMENTED Cancel twice
[ 261] XFER-16                     negative  IMPLEMENTED Lifecycle action during transfer
[ 262] XFER-17                     negative  IMPLEMENTED Cloud change during transfer
[ 263] XFER-18                     negative  IMPLEMENTED Download missing object
[ 264] DOWNLOAD-01                 negative  IMPLEMENTED Ejected/unmounted destination
[ 265] DOWNLOAD-02                 negative  IMPLEMENTED Missing object
[ 266] DOWNLOAD-03                 negative  IMPLEMENTED Invalid destination
[ 267] DOWNLOAD-04                 negative  IMPLEMENTED Cloud permission denied
[ 268] DOWNLOAD-05                 negative  IMPLEMENTED Download while upload active
[ 269] DOWNLOAD-06                 negative  IMPLEMENTED Cancel/pause/resume duplicate
[ 270] STATE-01                    negative  IMPLEMENTED IN_PROGRESS -> PAUSE -> PAUSE
[ 271] STATE-02                    negative  IMPLEMENTED IN_PROGRESS -> PAUSE -> RESUME -> RESUME
[ 272] STATE-03                    negative  IMPLEMENTED IN_PROGRESS -> PAUSE -> CANCEL -> CANCEL
[ 273] STATE-04                    negative  IMPLEMENTED IN_PROGRESS -> RESUME
[ 274] STATE-05                    negative  IMPLEMENTED IN_PROGRESS -> CANCEL -> CANCEL
[ 275] STATE-06                    negative  IMPLEMENTED PAUSED -> PAUSE
[ 276] STATE-07                    negative  IMPLEMENTED PAUSED -> RESUME -> RESUME
[ 277] STATE-08                    negative  IMPLEMENTED PAUSED -> CANCEL -> CANCEL
[ 278] STATE-09                    negative  IMPLEMENTED PAUSED -> EJECT/FORMAT/ERASE
[ 279] STATE-10                    negative  IMPLEMENTED COMPLETED -> PAUSE/RESUME/CANCEL
[ 280] STATE-11                    negative  IMPLEMENTED CANCELLED -> PAUSE/RESUME/CANCEL
[ 281] STATE-12                    negative  IMPLEMENTED Unknown transfer state
[ 282] STATE-13                    negative  IMPLEMENTED Rejected operation state audit
[ 283] RACE-01                     negative  IMPLEMENTED Pause + cancel
[ 284] RACE-02                     negative  IMPLEMENTED Resume + cancel
[ 285] RACE-03                     negative  IMPLEMENTED Pause + pause
[ 286] RACE-04                     negative  IMPLEMENTED Resume + resume
[ 287] RACE-05                     negative  IMPLEMENTED Cancel + cancel
[ 288] RACE-06                     negative  IMPLEMENTED Transfer + lifecycle
[ 289] RACE-07                     negative  IMPLEMENTED Upload + download
[ 290] RACE-08                     negative  IMPLEMENTED Upload + upload
[ 291] RACE-09                     negative  IMPLEMENTED Download + download
[ 292] RACE-10                     negative  IMPLEMENTED Operation + deconfigure
[ 293] RACE-11                     negative  IMPLEMENTED Invalid-ID ops + live transfer
[ 294] DUP-01                      negative  IMPLEMENTED Duplicate configure
[ 295] DUP-02                      negative  IMPLEMENTED Duplicate deconfigure
[ 296] DUP-03                      negative  IMPLEMENTED Duplicate mount/eject
[ 297] DUP-04                      negative  IMPLEMENTED Duplicate report
[ 298] DUP-05                      negative  IMPLEMENTED Repeated status
[ 299] REPORT-01                   negative  IMPLEMENTED Invalid ID/missing directory
[ 300] REPORT-02                   negative  IMPLEMENTED Empty ID
[ 301] REPORT-03                   negative  IMPLEMENTED Before transfer
[ 302] REPORT-04                   negative  IMPLEMENTED During IN_PROGRESS
[ 303] REPORT-05                   negative  IMPLEMENTED During PAUSED
[ 304] REPORT-06                   negative  IMPLEMENTED During cancellation
[ 305] REPORT-07                   negative  IMPLEMENTED After CANCELLED
[ 306] REPORT-08                   negative  IMPLEMENTED After COMPLETED
[ 307] REPORT-09                   negative  IMPLEMENTED Output is unwritable/file
[ 308] REPORT-10                   negative  IMPLEMENTED Duplicate generation
[ 309] REPORT-11                   negative  IMPLEMENTED During transition
[ 310] FAULT-01                    negative  IMPLEMENTED API unavailable/timeout/reset
[ 311] FAULT-02                    negative  IMPLEMENTED HTTP 400/401/403/404/409/500
[ 312] FAULT-03                    negative  IMPLEMENTED Malformed API response
[ 313] FAULT-04                    negative  IMPLEMENTED SSH unavailable/timeout
[ 314] FAULT-05                    negative  IMPLEMENTED SSH connection drop
[ 315] REC-01                      negative  IMPLEMENTED Restart service during upload/download
[ 316] REC-02                      negative  IMPLEMENTED Kill runner during transfer
[ 317] REC-03                      negative  IMPLEMENTED Network interruption and restore
[ 318] REC-04                      negative  IMPLEMENTED Status after restart/reboot (excluded)
[ 319] REC-05                      negative  IMPLEMENTED Configure after recovery
[ 320] VERIFY-01                   negative  IMPLEMENTED Missing objects after completion
[ 321] VERIFY-02                   negative  IMPLEMENTED Partial objects after failure
[ 322] VERIFY-03                   negative  IMPLEMENTED Incorrect transferred size
[ 323] VERIFY-04                   negative  IMPLEMENTED Remains active after completion
[ 324] VERIFY-05                   negative  IMPLEMENTED Remains paused after resume
[ 325] INT-01                      negative  IMPLEMENTED Objects missing after completed upload
[ 326] INT-02                      negative  IMPLEMENTED Partial objects after failed upload
[ 327] INT-03                      negative  IMPLEMENTED Incorrect transferred size
[ 328] INT-04                      negative  IMPLEMENTED Source deleted during upload
[ 329] INT-05                      negative  IMPLEMENTED Source modified during upload
[ 330] INT-06                      negative  IMPLEMENTED Source directory renamed
[ 331] INT-07                      negative  IMPLEMENTED Object deleted during download
[ 332] INT-08                      negative  IMPLEMENTED Destination removed during download
[ 333] INT-09                      negative  IMPLEMENTED Partial upload/download then resume
[ 334] INT-10                      negative  IMPLEMENTED Cancel then new transfer
[ 335] INT-11                      negative  IMPLEMENTED Interrupted transfer then status/report
[ 336] CLEAN-01                    negative  IMPLEMENTED Cancel then eject
[ 337] CLEAN-02                    negative  IMPLEMENTED Cancel then format
[ 338] CLEAN-03                    negative  IMPLEMENTED Cancel then mount
[ 339] CLEAN-04                    negative  IMPLEMENTED Cancel then deconfigure
[ 340] CLEAN-05                    negative  IMPLEMENTED Failed transfer follow-up
[ 341] CLEAN-06                    negative  IMPLEMENTED New transfer after failed/cancelled
[ 342] CLEAN-07                    negative  IMPLEMENTED Deconfigure after completion
[ 343] CLEAN-08                    negative  IMPLEMENTED Eject after completion
[ 344] CLEAN-09                    negative  IMPLEMENTED Final transfer audit
[ 345] CLEAN-10                    negative  IMPLEMENTED Final device audit
[ 346] CLEAN-11                    negative  IMPLEMENTED Dataset audit
[ 347] CLEAN-12                    negative  IMPLEMENTED Process audit
[ 348] MGMT-01                     negative  IMPLEMENTED Network info while ejected/unmounted
[ 349] MGMT-02                     negative  IMPLEMENTED Invalid IP address
[ 350] MGMT-03                     negative  IMPLEMENTED Invalid netmask
[ 351] MGMT-04                     negative  IMPLEMENTED Invalid/unreachable NTP server
[ 352] MGMT-05                     negative  IMPLEMENTED Invalid calendar date
[ 353] MGMT-06                     negative  IMPLEMENTED Invalid time-of-day
[ 354] MGMT-07                     negative  IMPLEMENTED Report while ejected/unmounted
[ 355] MGMT-08                     negative  IMPLEMENTED Remove while mounted
[ 356] MGMT-09                     negative  IMPLEMENTED Remove then rescan recovery
[ 357] MGMT-10                     negative  IMPLEMENTED Duplicate NTP configuration
[ 358] SVC-01                      negative  IMPLEMENTED stop_active_transfer (bcloud.service)
[ 359] SVC-02                      negative  IMPLEMENTED restart_active_transfer (bcloud.service)
[ 360] SVC-03                      negative  IMPLEMENTED stop_before_mgmt_op (bcloud.service)
[ 361] SVC-04                      negative  IMPLEMENTED stop_active_transfer (bryckcp.service)
[ 362] SVC-05                      negative  IMPLEMENTED restart_active_transfer (bryckcp.service)
[ 363] SVC-06                      negative  IMPLEMENTED stop_before_mgmt_op (bryckcp.service)
[ 364] SVC-07                      negative  IMPLEMENTED stop_active_transfer (bryckmonitor.service)
[ 365] SVC-08                      negative  IMPLEMENTED restart_active_transfer (bryckmonitor.service)
[ 366] SVC-09                      negative  IMPLEMENTED stop_before_mgmt_op (bryckmonitor.service)
[ 367] SVC-10                      negative  IMPLEMENTED stop_active_transfer (bryckobjectstore.service.new)
[ 368] SVC-11                      negative  IMPLEMENTED restart_active_transfer (bryckobjectstore.service.new)
[ 369] SVC-12                      negative  IMPLEMENTED stop_before_mgmt_op (bryckobjectstore.service.new)
[ 370] SVC-13                      negative  IMPLEMENTED stop_active_transfer (bryckagentbsmb.service)
[ 371] SVC-14                      negative  IMPLEMENTED restart_active_transfer (bryckagentbsmb.service)
[ 372] SVC-15                      negative  IMPLEMENTED stop_before_mgmt_op (bryckagentbsmb.service)
[ 373] SVC-16                      negative  IMPLEMENTED stop_active_transfer (bryck-info-trigger.service)
[ 374] SVC-17                      negative  IMPLEMENTED restart_active_transfer (bryck-info-trigger.service)
[ 375] SVC-18                      negative  IMPLEMENTED stop_before_mgmt_op (bryck-info-trigger.service)
[ 376] SVC-19                      negative  IMPLEMENTED stop_active_transfer (bryckmonitor_worker.service)
[ 377] SVC-20                      negative  IMPLEMENTED restart_active_transfer (bryckmonitor_worker.service)
[ 378] SVC-21                      negative  IMPLEMENTED stop_before_mgmt_op (bryckmonitor_worker.service)
[ 379] SVC-22                      negative  IMPLEMENTED stop_active_transfer (bstream.service)
[ 380] SVC-23                      negative  IMPLEMENTED restart_active_transfer (bstream.service)
[ 381] SVC-24                      negative  IMPLEMENTED stop_before_mgmt_op (bstream.service)
[ 382] SVC-25                      negative  IMPLEMENTED stop_active_transfer (bryckagentlc.service)
[ 383] SVC-26                      negative  IMPLEMENTED restart_active_transfer (bryckagentlc.service)
[ 384] SVC-27                      negative  IMPLEMENTED stop_before_mgmt_op (bryckagentlc.service)
[ 385] SVC-28                      negative  IMPLEMENTED stop_active_transfer (bryckmonitor_alert.service)
[ 386] SVC-29                      negative  IMPLEMENTED restart_active_transfer (bryckmonitor_alert.service)
[ 387] SVC-30                      negative  IMPLEMENTED stop_before_mgmt_op (bryckmonitor_alert.service)
[ 388] SVC-31                      negative  IMPLEMENTED stop_active_transfer (bryckobjectstore.service)
[ 389] SVC-32                      negative  IMPLEMENTED restart_active_transfer (bryckobjectstore.service)
[ 390] SVC-33                      negative  IMPLEMENTED stop_before_mgmt_op (bryckobjectstore.service)
[ 391] SVC-34                      negative  IMPLEMENTED stop_active_transfer (bryckapi.service)
[ 392] SVC-35                      negative  IMPLEMENTED restart_active_transfer (bryckapi.service)
[ 393] SVC-36                      negative  IMPLEMENTED stop_before_mgmt_op (bryckapi.service)
[ 394] SVC-37                      negative  IMPLEMENTED stop_active_transfer (bryckmonitor_prune_db.service)
[ 395] SVC-38                      negative  IMPLEMENTED restart_active_transfer (bryckmonitor_prune_db.service)
[ 396] SVC-39                      negative  IMPLEMENTED stop_before_mgmt_op (bryckmonitor_prune_db.service)
[ 397] SVC-40                      negative  IMPLEMENTED stop_active_transfer (redis.service)
[ 398] SVC-41                      negative  IMPLEMENTED restart_active_transfer (redis.service)
[ 399] SVC-42                      negative  IMPLEMENTED stop_before_mgmt_op (redis.service)
[ 400] SVC-43                      negative  IMPLEMENTED stop_active_transfer (minio.service)
[ 401] SVC-44                      negative  IMPLEMENTED restart_active_transfer (minio.service)
[ 402] SVC-45                      negative  IMPLEMENTED stop_before_mgmt_op (minio.service)
[ 403] SM-01                       negative  IMPLEMENTED CREATED -> Status
[ 404] SM-02                       negative  IMPLEMENTED CREATED -> Pause
[ 405] SM-03                       negative  IMPLEMENTED CREATED -> Resume
[ 406] SM-04                       negative  IMPLEMENTED CREATED -> Cancel
[ 407] SM-05                       negative  IMPLEMENTED IN_PROGRESS -> Status
[ 408] SM-06                       negative  IMPLEMENTED IN_PROGRESS -> Pause
[ 409] SM-07                       negative  IMPLEMENTED IN_PROGRESS -> Resume
[ 410] SM-08                       negative  IMPLEMENTED IN_PROGRESS -> Cancel
[ 411] SM-09                       negative  IMPLEMENTED IN_PROGRESS -> Eject
[ 412] SM-10                       negative  IMPLEMENTED IN_PROGRESS -> Format
[ 413] SM-11                       negative  IMPLEMENTED IN_PROGRESS -> Mount
[ 414] SM-12                       negative  IMPLEMENTED IN_PROGRESS -> Deconfigure
[ 415] SM-13                       negative  IMPLEMENTED PAUSED -> Status
[ 416] SM-14                       negative  IMPLEMENTED PAUSED -> Pause
[ 417] SM-15                       negative  IMPLEMENTED PAUSED -> Resume
[ 418] SM-16                       negative  IMPLEMENTED PAUSED -> Cancel
[ 419] SM-17                       negative  IMPLEMENTED PAUSED -> Eject
[ 420] SM-18                       negative  IMPLEMENTED PAUSED -> Format
[ 421] SM-19                       negative  IMPLEMENTED PAUSED -> Mount
[ 422] SM-20                       negative  IMPLEMENTED PAUSED -> Deconfigure
[ 423] SM-21                       negative  IMPLEMENTED COMPLETED -> Status
[ 424] SM-22                       negative  IMPLEMENTED COMPLETED -> Pause
[ 425] SM-23                       negative  IMPLEMENTED COMPLETED -> Resume
[ 426] SM-24                       negative  IMPLEMENTED COMPLETED -> Cancel
[ 427] SM-25                       negative  IMPLEMENTED CANCELLED -> Status
[ 428] SM-26                       negative  IMPLEMENTED CANCELLED -> Pause
[ 429] SM-27                       negative  IMPLEMENTED CANCELLED -> Resume
[ 430] SM-28                       negative  IMPLEMENTED CANCELLED -> Cancel
[ 431] F-01                        negative  IMPLEMENTED P0 IP Change -> Format -> Mount -> Upload Negative Matrix
[ 432] F-02                        negative  IMPLEMENTED P0 IP Change -> Format -> Mount -> Download Negative Matrix
[ 433] F-03                        negative  IMPLEMENTED Format Without Eject -> Recovery
[ 434] F-04                        negative  IMPLEMENTED Active Upload -> All Management Conflicts
[ 435] F-05                        negative  IMPLEMENTED Paused Upload -> All Management Conflicts
[ 436] F-06                        negative  IMPLEMENTED Resume Race -> Management Conflicts
[ 437] F-07                        negative  IMPLEMENTED Active Download -> All Management Conflicts
[ 438] F-08                        negative  IMPLEMENTED Upload Pause/Resume Repetition
[ 439] F-09                        negative  IMPLEMENTED Upload Pause -> Cancel -> Cleanup
[ 440] F-10                        negative  IMPLEMENTED Upload Active -> Cancel Immediately -> New Upload
[ 441] F-11                        negative  IMPLEMENTED Completed Upload -> Invalid Operations
[ 442] F-12                        negative  IMPLEMENTED Completed Download -> Invalid Operations
[ 443] F-13                        negative  IMPLEMENTED Upload + Upload Concurrent
[ 444] F-14                        negative  IMPLEMENTED Upload + Download Concurrent
[ 445] F-15                        negative  IMPLEMENTED Pause + Deconfigure Race
[ 446] F-16                        negative  IMPLEMENTED Resume + Deconfigure Race
[ 447] F-17                        negative  IMPLEMENTED Pause + Cancel Race
[ 448] F-18                        negative  IMPLEMENTED Resume + Cancel Race
[ 449] F-19                        negative  IMPLEMENTED Eject + Cancel Race
[ 450] F-20                        negative  IMPLEMENTED Format + Cancel Race
[ 451] F-21                        negative  IMPLEMENTED API Failure During Active Upload
[ 452] F-22                        negative  IMPLEMENTED SSH Failure During Dataset Generation
[ 453] F-23                        negative  IMPLEMENTED Service Restart During Upload
[ 454] F-24                        negative  IMPLEMENTED Service Restart During Pause
[ 455] F-25                        negative  IMPLEMENTED Token Expiry During Upload
[ 456] F-26                        negative  IMPLEMENTED Token Expiry During Paused Transfer
[ 457] F-27                        negative  IMPLEMENTED Network Loss During Upload
[ 458] F-28                        negative  IMPLEMENTED Network Loss During Download
[ 459] F-29                        negative  IMPLEMENTED Report At Every Transfer State
[ 460] F-30                        negative  IMPLEMENTED Format/Eject/Mount State Cycle With Transfer Attempts
[ 461] F-31                        negative  IMPLEMENTED Dataset Path Mismatch Flow
[ 462] F-32                        negative  IMPLEMENTED Insufficient Space Flow
[ 463] F-33                        negative  IMPLEMENTED Invalid AWS Permission Flow
[ 464] F-34                        negative  IMPLEMENTED Cancel -> Deconfigure -> Eject -> Reconfigure -> New Transfer
[ 465] F-35                        negative  IMPLEMENTED Completed -> Deconfigure -> Eject -> Reconfigure -> New Transfer
[ 466] F-36                        negative  IMPLEMENTED Failed Transfer -> Recovery -> New Transfer
[ 467] F-37                        negative  IMPLEMENTED System Reboot During Active Transfer (excluded)
[ 468] F-38                        negative  IMPLEMENTED System Reboot During Paused Transfer (excluded)
[ 469] F-39                        negative  IMPLEMENTED Full Upload Negative Regression
[ 470] F-40                        negative  IMPLEMENTED Full Download Negative Regression
[ 471] MASTER-UPLOAD               negative  stub        P0 end-to-end upload flow (format/mount/configure/upload/pause/resume/blocked-destructive-attempts/completion/cleanup)
[ 472] MASTER-DOWNLOAD             negative  stub        P0 end-to-end download flow (seed upload, then format/mount/configure/download/pause/resume/blocked-destructive-attempts/completion/cleanup)
[ 473] MASTER-BOTH                 negative  stub        P0 end-to-end both flow (upload leg then download leg in one continuous session)

473 total case(s): 162 transfer + 311 negative.
```

Note some individual IDs within a section may still report `BLOCKED` at
runtime even though the section itself is `IMPLEMENTED` here (e.g. some
`F-*`/`FAULT-*` cases need `--confirm-destructive`/`fault_proxy`/specific
fixtures unavailable in a given environment) -- `IMPLEMENTED` means a real
handler exists and will attempt the case, not that every individual ID is
guaranteed to execute in every environment.
