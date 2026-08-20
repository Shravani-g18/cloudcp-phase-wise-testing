# CloudCP CLI Test Plan

## Decisions Fixed for This Plan

These answers were confirmed before writing this plan and constrain every
section below. The execution script must not deviate from them without a
plan update.

| # | Question | Decision |
|---|---|---|
| 1 | Execution target | One Bryck system at a time. |
| 2 | Transfer modes | Every dataset is run for upload, download, **and** both — not user-selected per run. |
| 3 | Dataset selection | Default (`--dataset-catalog all`) runs every dataset in `dataset_cloudcp/spec_files/manifest.json` (`DS-P1-01`..`DS-P12-02`, 54 datasets) as its own transfer round; no per-run manual pick, and no size-tier (ZERO/TINY/SMALL/MEDIUM/LARGE/SPARSE) datasets are used unless `--dataset-catalog tiers` is explicitly requested. |
| 4 | Mount behavior | If Bryck is ejected, the script mounts it automatically (no extra prompt beyond the single top-level confirmation gate). |
| 5 | Eject during transfer | Included as an intentional negative test. |
| 6 | Format/erase/remove during transfer | Actually executed (not just checked for rejection) — these are real destructive lifecycle tests. |
| 7 | Service restart | Both `bcloud` and `bryckapi` are restarted during an active transfer. |
| 8 | Re-transfer | After cancel, a new transfer is started automatically using the same dataset. |
| 9 | Confirmation level | **One** confirmation before the entire run (the Plan → Execute gate in §13); no per-operation prompts once confirmed. |
| 10 | Results format | All three: JSON, HTML, and Markdown. |
| 11 | Logs | Per-test-case directories under `results/<RUN_ID>/<TEST_ID>/`, which also pull in/reference the host's own `/opt/bryck/bryckapi/downloads/cloud_transfer_logs/cloud_transfer_<id>/` artifacts for that test. |
| 12 | Cleanup | Auto-cleanup (dataset + cloud objects) after every test, unless a `--keep` / `--no-cleanup` flag is passed for debugging. |
| 13 | `cloud_ops.json` | Dynamically updated per test case (`bryck_src` / `cloud_bucket` / `bryck_dst` rewritten per dataset/mode), never edited by hand mid-run. |
| 14 | `config.json` | Fully read-only reference — the framework only reads tier definitions from it, never writes to it. |
| 15 | SPARSE dataset | Sourced from a separate YAML/spec file under `datasets/spec_files/` (e.g. the sparse spec used by `CloudCpFallbackTesting`), since `/etc/bryck/bryckcloud/config.json` only defines ZERO/TINY/SMALL/MEDIUM/LARGE. |

Because destructive operations (§8) are executed for real and cleanup is
automatic, the confirmation gate in §13 is the **only** safety checkpoint —
it must clearly list every destructive step before the user approves.

---

## 1. Purpose

Validate the CloudCP CLI (`bryckclient-cli` runners: mount/eject/format/erase,
cloud configure, transfer initiate/status/pause/resume/cancel/report) against a
real Bryck appliance across the full dataset size range, including disruptive
conditions (service restarts, ejects, format/erase attempts, cancel +
re-transfer) — and produce evidence-backed PASS/FAIL results for each
operation.

## 2. Scope

- Bryck mount/eject lifecycle
- Dataset generation
- AWS/cloud configuration
- Upload/download
- Pause/resume/cancel
- Re-transfer
- Service restart/recovery
- Transfer verification
- Data integrity
- Logs and reports

Out of scope: GCP/Azure cloud types (AWS only, per current `cloud_ops.json`
usage), multi-Bryck parallel execution.

## 3. Required Files & Paths

| Path | Role |
|---|---|
| `bryckclient-cli/login.json` | Bryck REST + SSH credentials (read-only). |
| `bryckclient-cli/cloud_ops.json` | Cloud provider + src/dst paths — rewritten per test case (see §6, §13 decision). |
| `bryckclient-cli/format_mount_params.json` | Format/mount parameters (read-only unless a case targets a param change). |
| `dataset_cloudcp/spec_files/*.yaml` (and `CloudCpFallbackTesting/spec_files/*.yaml` for SPARSE) | Datagen spec catalog. |
| `/etc/bryck/bryckcloud/config.json` | Tier + TEST/TRANSFER reference config — read-only (decision #14). |
| `/etc/bryck/bryckcloud/transfer_summary_files.json` | Transfer summary reference for verification. |
| `/opt/bryck/bryckapi/downloads/cloud_transfer_logs/` | Per-transfer report/log root. |
| `/opt/bryck/bryckapi/downloads/cloud_transfer_logs/cloudcp.log` | Engine log, checked for errors/crashes. |
| `/opt/bryck/bryckapi/downloads/bcloud_batchmeta` | Broker batch metadata root. |
| `/home/bryck/shravani/<RUN_ID>/<TEST_ID>/` | **Downloaded** transfer reports (`bryck_cloud_transfer_report.py --report-path`) land here, not under `results/` (which stays Windows/host-portable). Overridable with `--report-save-dir`. |
| `/home/bryck/shravani/<RUN_ID>/_final_diagnostic_report/` | `bryck_report.py --output-dir` diagnostic dump, downloaded once at the end of every `--execute` run. |

## 4. Pre-Execution Validation

Performed in `--plan` mode (read-only, no side effects):

1. Validate JSON syntax of `login.json`, `cloud_ops.json`, `format_mount_params.json`.
2. Validate SSH connectivity (paramiko) and REST API connectivity (`ApiSession.login()`).
3. Run `bryck_info.py` to read current Bryck state.
4. Determine whether Bryck is mounted.
   - If **ejected**, plan includes an explicit "Mount Bryck" step (auto-mount per decision #4) — the plan output must call this out.
   - **Never generate data while Bryck is ejected.** If ejected, datagen is scheduled *after* the mount step, never before.
5. Validate the dataset specification(s): each selected tier's YAML exists, parses, and its target root is under the mounted Bryck path.
6. **Strictly validate `/etc/bryck/bryckcloud/config.json`** (`validate_bryck_config_json()`): on a JSON parse
   error (e.g. "Extra data"), the plan output prints a dedicated, impossible-to-miss
   `CONFIG ERROR` banner — with an `nl -ba`-style context snippet around the failing
   line/column — instead of burying it in a generic warning line. This file is read-only
   reference config (decision #14) that no test case currently depends on operationally,
   so the run is **not** blocked by it, but the error can no longer be silently ignored.

Any failure in steps 1-5 aborts `--plan` before any confirmation prompt is shown.

## 5. Dataset Selection

By default (`--dataset-catalog all`, decision #3) the runner uses **every**
dataset in the authoritative catalog (`dataset_cloudcp/spec_files/manifest.json`
+ `dataset_map.json`, `DS-P1-01`..`DS-P12-02`, 54 datasets total) — one
transfer round per dataset x mode, no size-tier names (ZERO/TINY/SMALL/MEDIUM/
LARGE/SPARSE) involved. The size-tier catalog described below is only used
when `--dataset-catalog tiers` is explicitly passed (e.g. for a faster smoke
run); it maps to the same underlying `DS-P1-0x` datasets plus a separate
SPARSE spec.

| Tier | Primary dataset (default run) | Stress alternate (`--full-scale`) | Spec location | Notes |
|---|---|---|---|---|
| `ZERO` | `DS-P1-01` | `DS-P8-02` (single zero-byte smoke) | `dataset_cloudcp/spec_files/DS-P1-01/` | 5,000,000 zero-byte files; `DS-P8-02` is the 1-file smoke variant. |
| `TINY` | `DS-P1-02` | `DS-P12-01` (tiny/small heavy, 1 M files) | `dataset_cloudcp/spec_files/DS-P1-02/` | 1 B – 1 MB, count-seal/byte-seal mix. |
| `SMALL` | `DS-P1-03` | `DS-P2-01` (11 exact size boundaries incl. 64 MB multipart edge) | `dataset_cloudcp/spec_files/DS-P1-03/` | 1 MB – 100 MB, crosses the 64 MB multipart threshold. |
| `MEDIUM` | `DS-P1-04` | `DS-P2-05` (medium count-seal trigger) | `dataset_cloudcp/spec_files/DS-P1-04/` | 100 MB – 1 GB, always multipart. |
| `LARGE` | `DS-P1-05` (20 files, 5–50 GB smoke) | `DS-P1-06` (200 files, 5–100 GB perf baseline) | `dataset_cloudcp/spec_files/DS-P1-05/` (`DS-P1-06/` for stress) | Multipart + bandwidth stress; `DS-P1-05` is the fast default. |
| `SPARSE` | `06_sparse_files` | `12_tiny_2million` (scale) | `CloudCpFallbackTesting/spec_files/06_sparse_files.yaml` (separate catalog — decision #15) | Sparse/logical-vs-physical content; no equivalent tier in `/etc/bryck/bryckcloud/config.json`. |

Selection logic: the runner reads `/etc/bryck/bryckcloud/config.json` for the
tier definitions that exist there (ZERO/TINY/SMALL/MEDIUM/LARGE) and maps each
to its primary dataset id above via `dataset_cloudcp/spec_files/manifest.json`;
SPARSE is appended from the separate `CloudCpFallbackTesting/spec_files/`
catalog since it has no `config.json` tier entry. `dataset_map.json`'s
category/subcategory fields are used only for the run summary annotation, not
for selection.

### 5.1 Additional supporting datasets (used by specific test cases below)

| Dataset | Used by | Reason |
|---|---|---|
| `DS-P8-01` | `CLI-EDGE-01` | Empty source directory — zero-file transfer edge case. |
| `DS-P8-04` | `CLI-EDGE-02` | 14-level deep directory tree — scanner/resume stress. |
| `DS-P9-04` | `CLI-EDGE-03` | Single 64 MB file — first size that must go multipart. |
| `DS-P4-01` | `CLI-EDGE-04` | Filename/encoding stress (20 filename variants) at tiny tier. |

### 5.2 Full-catalog round (`--dataset-catalog all`, default)

This is the default mode: the runner runs **every** dataset in
`dataset_cloudcp/spec_files/manifest.json` (54 datasets, `DS-P1-01`..`DS-P12-02`,
per §9 of `dataset_generation_plan.md`) as its own transfer round — one
`CLI-<mode>-<dataset-id>` test case per dataset x mode, each going through the
same mandatory mount -> generate -> configure -> upload -> download -> report
flow (§6-§7). Narrow it to a specific subset with `--datasets DS-P1-01 DS-P2-03 ...`,
or fall back to the old size-tier behavior with `--dataset-catalog tiers`.

```bash
# default: every dataset in the manifest, upload + download + both, as one confirmed plan
python3 cloud_cli_runner.py --plan --yes
python3 cloud_cli_runner.py --execute --plan-file results/<RUN_ID>/plan.json

# only a specific subset of datasets
python3 cloud_cli_runner.py --plan --datasets DS-P2-01 DS-P4-05 DS-P9-07 --yes

# old size-tier behavior (ZERO/TINY/SMALL/MEDIUM/LARGE/SPARSE), if ever needed
python3 cloud_cli_runner.py --plan --dataset-catalog tiers --yes
```

### 5.3 Local spec catalog (`--dataset-catalog specfiles`)

`CloudCpCliTesting/spec_files/` holds its own single-spec YAML catalog (the
same shape as the fallback-suite specs — one `root:` line per file, rewritten
by the runner at generation time):

| Spec | Focus |
|---|---|
| `01_zero_byte.yaml` | 0-byte files. |
| `02_tiny_files.yaml` | Many small files. |
| `03_small_files.yaml` | 1–16 MiB, multipart boundary. |
| `04_medium_files.yaml` | 64–512 MiB. |
| `05_large_files.yaml` | 1–5 GiB, sparse. |
| `06_sparse_files.yaml` | Sparse content across tiers. |
| `07_fill_files.yaml` | Deterministic fill (checksum-stable). |
| `08_deep_tree.yaml` | Deep nested paths. |
| `09_unicode_names.yaml` | Unicode/emoji/CJK names. |
| `10_special_char_names.yaml` | ASCII special chars/spaces. |
| `11_mixed_realistic.yaml` | Weighted realistic mix. |
| `12_tiny_2million.yaml` | Scale — ~2M tiny files. |

`--dataset-catalog specfiles` runs one `CLI-<mode>-<spec-name>` transfer round
per file here (again through the mandatory mount -> generate -> configure ->
upload -> download -> report flow), instead of the tier or manifest catalogs.
Narrow it with `--datasets 01_zero_byte 09_unicode_names ...`.

```bash
# every spec_files/*.yaml dataset, upload + download
python3 cloud_cli_runner.py --plan --dataset-catalog specfiles --yes
python3 cloud_cli_runner.py --execute --plan-file results/<RUN_ID>/plan.json

# only a specific subset
python3 cloud_cli_runner.py --plan --dataset-catalog specfiles --datasets 01_zero_byte 07_fill_files 12_tiny_2million --yes
```

## 6. Cloud Configuration

Per test case (per dataset x per mode), from `CloudCpCliTesting/bryckclient-cli/`:

1. Rewrite `cloud_ops.json` `bryck_src` / `cloud_bucket` / `bryck_dst` to a
   dataset+mode-specific path/prefix, e.g. for tier `SMALL`:

   ```jsonc
   {
     "cloud_type": "aws",
     "bryck_src": "/bryck/cloudcp_cli/SMALL",
     "cloud_bucket": "s3://shravani/cloudcp-cli/SMALL",
     "bryck_dst": "/bryck/cloudcp_cli_dl/SMALL"
   }
   ```

   This is the only file the framework dynamically edits (decision #13); a
   backup (`cloud_ops.json.bak`) is written to `results/<RUN_ID>/` and restored
   at the end of the run.

2. Configure the provider on the Bryck:

   ```bash
   python3 bryck_cloud_configure.py --login login.json --params cloud_ops.json
   ```

3. Verify the configuration before starting the transfer:

   ```bash
   python3 bryck_cloud_show.py --login login.json
   ```

## 7. Transfer Execution

Per test case, still from `CloudCpCliTesting/bryckclient-cli/`:

0. **Mandatory precondition — Bryck must be mounted.** Before dataset
   generation or transfer initiation, check `bryck_info.py`; if the state
   does not contain `Mounted`, run `bryck_mount.py` and poll until it does.
   The runner performs this automatically before **every** transfer,
   lifecycle, service, and edge test case (`Executor.ensure_mounted()`), not
   just once at the start of the whole run — a prior test's eject/format
   action must never leave a later test generating data against an
   unmounted device.

1. Initiate the transfer (decision #2 — every dataset gets all three modes,
   run as three separate test cases per tier):

   ```bash
   # upload:   bryck_src -> cloud_bucket
   python3 bryck_cloud_transfer_initiate.py --login login.json --params cloud_ops.json --mode upload

   # download: cloud_bucket -> bryck_dst  (requires a prior clean upload of the same tier)
   python3 bryck_cloud_transfer_initiate.py --login login.json --params cloud_ops.json --mode download

   # both:     upload then download in one call
   python3 bryck_cloud_transfer_initiate.py --login login.json --params cloud_ops.json --mode both
   ```

   This prints/returns the created `transfer_id` — capture it.

2. Poll until terminal state (or until a live-intervention test in §8 needs to
   fire mid-transfer):

   ```bash
   python3 bryck_cloud_transfer_status.py --login login.json --transfer-id <id>
   # or, to see every transfer in a given state:
   python3 bryck_cloud_transfer_status.py --login login.json --state IN_PROGRESS
   ```

   Valid states returned: `IN_PROGRESS`, `COMPLETED`, `PAUSED`, `FAILED`,
   `STOPPED`, `CANCELLED`.

3. Every command invocation, its raw response, and a timestamp are appended to
   that test case's evidence log (§11).

## 8. Live Transfer Intervention Tests

For each active transfer, the following are exercised for real (decisions
#6/#7 — not merely checked for rejection):

| Action | Command |
|---|---|
| Pause | `python3 bryck_cloud_transfer_pause.py --login login.json --transfer-id <id>` |
| Resume | `python3 bryck_cloud_transfer_resume.py --login login.json --transfer-id <id>` |
| Cancel | `python3 bryck_cloud_transfer_cancel.py --login login.json --transfer-id <id>` |
| Re-transfer | After cancel, re-run the §7 initiate command for the same dataset/mode (decision #8) and capture the new `transfer_id` |
| Mount | `python3 bryck_mount.py --login login.json --params format_mount_params.json` |
| Eject | `python3 bryck_eject_unmount.py --login login.json` (intentionally run mid-transfer — negative test, decision #5) |
| Attempt format | `python3 bryck_format.py --login login.json --params format_mount_params.json` |
| Attempt erase | `python3 bryck_erase.py --login login.json` |
| Attempt remove | `python3 bryck_remove.py --login login.json` |
| Restart `bcloud` | SSH: `sudo systemctl restart bcloud.service` (via `ssh_runner.py` / login.json SSH creds) |
| Restart `bryckapi` | SSH: `sudo systemctl restart bryckapi.service` |

Each operation records, in this exact order:

```
Before State -> Action -> API/CLI Response -> After State -> Expected Result -> Actual Result -> PASS/FAIL
```

Before/after state is captured with `bryck_info.py --login login.json` and
`bryck_cloud_transfer_status.py --login login.json --transfer-id <id>`.

## 9. Test Case Catalog

Every row below is one executable test case (`TEST_ID` is the results-directory
name, §16). All test cases run against a **single Bryck** (decision #1), with
all destructive steps executed for real (decisions #5–#8) and cleaned up
automatically afterward (decision #12).

### 9.1 Transfer Matrix — one case per tier x per mode (18 cases)

Dataset ids and spec paths from §5. Expected result for every row: transfer
reaches `COMPLETED`, object count/sizes match source, `final_report.csv` row
count matches expected file count.

| Test ID | Tier | Dataset | Mode | Command (from §7) |
|---|---|---|---|---|
| `CLI-U-ZERO` | ZERO | `DS-P1-01` | upload | `bryck_cloud_transfer_initiate.py --mode upload` |
| `CLI-D-ZERO` | ZERO | `DS-P1-01` | download | `bryck_cloud_transfer_initiate.py --mode download` |
| `CLI-B-ZERO` | ZERO | `DS-P1-01` | both | `bryck_cloud_transfer_initiate.py --mode both` |
| `CLI-U-TINY` | TINY | `DS-P1-02` | upload | `bryck_cloud_transfer_initiate.py --mode upload` |
| `CLI-D-TINY` | TINY | `DS-P1-02` | download | `bryck_cloud_transfer_initiate.py --mode download` |
| `CLI-B-TINY` | TINY | `DS-P1-02` | both | `bryck_cloud_transfer_initiate.py --mode both` |
| `CLI-U-SMALL` | SMALL | `DS-P1-03` | upload | `bryck_cloud_transfer_initiate.py --mode upload` |
| `CLI-D-SMALL` | SMALL | `DS-P1-03` | download | `bryck_cloud_transfer_initiate.py --mode download` |
| `CLI-B-SMALL` | SMALL | `DS-P1-03` | both | `bryck_cloud_transfer_initiate.py --mode both` |
| `CLI-U-MEDIUM` | MEDIUM | `DS-P1-04` | upload | `bryck_cloud_transfer_initiate.py --mode upload` |
| `CLI-D-MEDIUM` | MEDIUM | `DS-P1-04` | download | `bryck_cloud_transfer_initiate.py --mode download` |
| `CLI-B-MEDIUM` | MEDIUM | `DS-P1-04` | both | `bryck_cloud_transfer_initiate.py --mode both` |
| `CLI-U-LARGE` | LARGE | `DS-P1-05` | upload | `bryck_cloud_transfer_initiate.py --mode upload` |
| `CLI-D-LARGE` | LARGE | `DS-P1-05` | download | `bryck_cloud_transfer_initiate.py --mode download` |
| `CLI-B-LARGE` | LARGE | `DS-P1-05` | both | `bryck_cloud_transfer_initiate.py --mode both` |
| `CLI-U-SPARSE` | SPARSE | `06_sparse_files.yaml` | upload | `bryck_cloud_transfer_initiate.py --mode upload` |
| `CLI-D-SPARSE` | SPARSE | `06_sparse_files.yaml` | download | `bryck_cloud_transfer_initiate.py --mode download` |
| `CLI-B-SPARSE` | SPARSE | `06_sparse_files.yaml` | both | `bryck_cloud_transfer_initiate.py --mode both` |

`download` cases require the tier's objects to already exist in the bucket —
the runner satisfies this by always executing `CLI-U-<TIER>` before
`CLI-D-<TIER>` in the plan order (§14).

### 9.2 Live Intervention Matrix — one case per tier (6 cases x 10 actions)

Run against the `CLI-B-<TIER>` transfer while it is `IN_PROGRESS`. Each action
row is logged with the full before/after state per §8.

| Test ID | Tier | Actions exercised (in order) |
|---|---|---|
| `CLI-LC-ZERO` | ZERO | pause -> resume -> cancel -> re-transfer -> mount -> eject -> format attempt -> erase attempt -> remove attempt -> restart bcloud -> restart bryckapi |
| `CLI-LC-TINY` | TINY | same 10-action sequence |
| `CLI-LC-SMALL` | SMALL | same 10-action sequence |
| `CLI-LC-MEDIUM` | MEDIUM | same 10-action sequence |
| `CLI-LC-LARGE` | LARGE | same 10-action sequence |
| `CLI-LC-SPARSE` | SPARSE | same 10-action sequence |

Expected results per action:

| Action | Expected result |
|---|---|
| Pause | Status transitions to `PAUSED`; no data loss on resume. |
| Resume | Status returns to `IN_PROGRESS` and eventually `COMPLETED`. |
| Cancel | Status transitions to `CANCELLED`; no further progress. |
| Re-transfer | A new `transfer_id` is created and reaches `COMPLETED` independently of the cancelled one. |
| Mount | Bryck state becomes `Mounted`; no-op if already mounted. |
| Eject (mid-transfer) | Negative test — transfer must surface a failure/stopped state, not hang or corrupt the report; Bryck ejects cleanly. |
| Format attempt | Bryck refuses or fully reformats depending on state; if it proceeds, the run's dataset is regenerated afterward before continuing. |
| Erase attempt | Cloud config/transfer history on the Bryck is reset; runner reconfigures cloud (§6) afterward. |
| Remove attempt | Bryck is deregistered from `bryckapi`; runner re-adds it (out of band, manual) if needed to continue — flagged as a run-ending case if remove succeeds. |
| Restart bcloud / bryckapi | Service comes back up within a bounded wait; any `IN_PROGRESS` transfer either resumes or is cleanly marked `FAILED`/`STOPPED` (never silently lost). |

### 9.3 Service Restart Matrix (2 cases, cross-tier)

| Test ID | Restart target | Timing |
|---|---|---|
| `CLI-SVC-BCLOUD` | `bcloud.service` | Mid-transfer on `CLI-B-MEDIUM` (multipart in progress). |
| `CLI-SVC-BRYCKAPI` | `bryckapi.service` | Mid-transfer on `CLI-B-MEDIUM` (multipart in progress). |

### 9.4 Negative / Edge Cases (4 cases)

| Test ID | Dataset | Scenario | Expected result |
|---|---|---|---|
| `CLI-EDGE-01` | `DS-P8-01` | Empty source directory upload. | Transfer completes with 0 objects transferred; no error. |
| `CLI-EDGE-02` | `DS-P8-04` | 14-level deep directory tree upload. | All paths preserved; no scanner stack overflow. |
| `CLI-EDGE-03` | `DS-P9-04` | Single 64 MB file upload (first multipart size). | Uses multipart upload; single object in report. |
| `CLI-EDGE-04` | `DS-P4-01` | Tiny tier, 20 filename variants upload. | Every filename variant round-trips byte-for-byte. |

### 9.5 CLI / Input-Validation Negative Cases (9 cases, `CLI-01`..`CLI-09`)

Every row expects the operation to be **rejected** (`expect_fail=True`) before
any real mutation happens — no fixture setup, no transfer, no partial config.
Runs against `bryck_cloud_transfer_initiate.py`/`bryck_cloud_show.py`/`bryck_cloud_transfer_pause.py`/`datagen`.

| Test ID | Scenario | Expected result |
|---|---|---|
| `CLI-01` | Initiate transfer without `--mode`. | argparse rejects; no transfer created. |
| `CLI-02` | Initiate transfer with `--mode copy` (invalid choice). | argparse rejects; no transfer created. |
| `CLI-03` | Upload with empty `bryck_src` in `cloud_ops.json`. | Rejected before any API mutation. |
| `CLI-04` | Upload with empty `cloud_bucket`. | Rejected before any API mutation. |
| `CLI-05` | Download with empty `bryck_dst`. | Rejected before any API mutation. |
| `CLI-06` | `bryck_cloud_show.py` with a missing `login.json`. | Readable file error; no API call. |
| `CLI-07` | `bryck_cloud_show.py` with a malformed `login.json` (`"{"`). | Readable JSON error. |
| `CLI-08` | Pause with `--transfer-id not-a-transfer-id`. | Controlled rejection; no state change. |
| `CLI-09` | `datagen --spec <nonexistent.yaml>`. | Fails before any host mutation. |

### 9.6 Cloud / AWS Configuration Negative Cases (8 cases, `AWS-01`..`AWS-08`)

Each mutates a private per-case copy of `cloud_ops.json` (never the shared
file) and runs `bryck_cloud_configure.py`/`bryck_cloud_deconfigure.py`.

| Test ID | Scenario | Expected result |
|---|---|---|
| `AWS-01` | Configure with empty `access_key_id`. | Rejected; no partial provider config. |
| `AWS-02` | Configure with empty `secret_access_key`. | Rejected; no partial provider config. |
| `AWS-03` | Configure with `access_key_id=invalid-access-key`. | Provider rejects; no partial config. |
| `AWS-04` | Configure with `secret_access_key=invalid-secret-key`. | Provider rejects. |
| `AWS-05` | Configure with `region=invalid-region`. | Controlled provider error. |
| `AWS-06` | Configure with `cloud_bucket=not-a-valid-bucket`. | Validation failure. |
| `AWS-07` | Deconfigure when nothing is configured. | Observational — rc recorded, not forced pass/fail (documented idempotence). |
| `AWS-08` | Deconfigure twice in a row. | Observational — both rcs recorded; must be deterministic. |

**Total: 18 (transfer) + 60 (6 x 10 intervention actions, tracked as sub-rows
of the 6 `CLI-LC-*` cases) + 2 (service) + 4 (edge) + 9 (CLI-input) + 8
(AWS-negative) = 41 top-level test cases, 60 intervention action sub-results.**

### 9.7 Environment-Aware Pipeline (CLI-input / AWS-negative execution model)

`CLI-*`/`AWS-*` cases run through a shared 10-step pipeline
(`Executor._run_negative_pipeline()`), not a flat "build args, run one
command, done" execution — this mirrors the inspect/prepare/validate/
execute/cleanup/verify architecture of the reference `negative_environment_runner.py`:

1. **Inspect environment** — `bryck_info.py`, current Bryck state.
2. **Validate configuration** — `login.json`/`cloud_ops.json` must already have
   parsed successfully at `Executor` init; otherwise the case is `BLOCKED`
   immediately (never falsely reported PASS).
3. **Establish Bryck mounted state** — `SKIPPED` for these cases (pure
   input/config validation needs no dataset access); real transfer/lifecycle
   cases call `ensure_mounted()` here instead.
4. **Capture baseline** — Bryck state snapshot before the operation.
5. **Create negative fixture** — a private per-case JSON copy (e.g.
   `CLI-03/cli03_cloud_ops.json`), never the shared `login.json`/`cloud_ops.json`.
6. **Execute operation** — the one command under test; labeled
   `EXPECTED FAILURE` when `expect_fail=True`.
7. **Validate rejection/success** — return code checked against the expected
   outcome.
8. **Verify no unintended state change** — Bryck state re-checked; a
   before/after mismatch is a `FAIL` even if step 7 passed.
9. **Cleanup** — no shared config was touched; the per-case fixture is left
   in place as evidence.
10. **Verify final environment** — final Bryck state snapshot.

Each step is printed live during `--execute` as a numbered block
(`TestCaseResult.render_block()`) and persisted into that case's
`report.json` (`steps`, `expected`, `actual`, `state_change`), for example:

```
============================================================
CLI-03 | Upload with empty bryck_src in cloud_ops.json
============================================================
[1] Inspect environment                    PASS  (bryck_state=Mounted)
[2] Validate configuration                 PASS  (login.json/cloud_ops.json parse OK)
[3] Establish Bryck mounted state          SKIPPED  (not required — pure input/config validation, no dataset access)
[4] Capture baseline                       PASS  (state=Mounted)
[5] Create negative fixture                PASS  (cli03_cloud_ops.json: bryck_src='')
[6] Execute operation                      EXPECTED FAILURE  (rc=2)
[7] Validate rejection                     PASS
[8] Verify no unintended state change      PASS  (before='Mounted' after='Mounted')
[9] Cleanup                                PASS  (no shared login.json/cloud_ops.json modified; per-case fixture retained as evidence)
[10] Verify final environment              PASS  (state=Mounted)

Expected:     Upload must be rejected because bryck_src is empty.
Actual:       rc=2
State change: Mounted -> Mounted
RESULT:       PASS
```

`AWS-07`/`AWS-08` are explicitly **observational**: both return codes are
captured for evidence but never used to force PASS/FAIL, matching "documented
idempotence, not a hard rc check" from `NEGATIVE_TEST_PLAN.md`.

## 10. Verification

- Transfer status (terminal state reached, matches expectation for the case)
- Transfer summary (`transfer_summary_files.json` cross-check)
- Object count (source file count vs. transferred/reported count)
- Source/destination size comparison
- Missing/partial objects
- Transfer report (`final_report.csv`, `upload_report.*.csv`)
- Integrity checks (checksum comparison where the dataset spec supports it)

## 11. Evidence Collection

Every operation (mount/eject/format/erase/service restart/transfer command)
records:

- Exact command executed
- Timestamp
- Return code
- stdout
- stderr
- API response with secrets removed (credentials/keys redacted before write)
- Transfer ID (where applicable)
- Before/after Bryck state
- Relevant log excerpts (`cloudcp.log`, batch metadata)
- Relevant report file(s) — **downloaded transfer reports and the final diagnostic
  report are saved on the Bryck host under `--report-save-dir`
  (default `/home/bryck/shravani/<RUN_ID>/<TEST_ID>/`)**, separate from the
  `results/<RUN_ID>/` evidence tree (which stays portable/host-agnostic).
- For CLI-input/AWS-negative cases (§9.5/§9.6): the full numbered pipeline
  (`steps`), plus `expected`/`actual`/`state_change`, per §9.7.

## 12. Recovery & Cleanup

At the end of each test case (auto, per decision #12, unless `--keep`/`--no-cleanup` given):

1. Complete or cancel any transfer left active by that case.
2. Restore Bryck to the expected state for the next case (mounted, unless the
   next case specifically starts from ejected).
3. Remove the case's generated dataset from the Bryck path.
4. Remove the case's uploaded cloud objects.
5. Verify `bcloud` and `bryckapi` services are up and responsive.
6. Confirm no orphan processes/transfers remain (`bryck_cloud_transfer_status.py`
   shows nothing `IN_PROGRESS` that belongs to this run).
7. Restore `cloud_ops.json` to its pre-run contents once the whole run ends.

## 13. Confirmation Gate — VERY IMPORTANT

Before `--execute` runs anything, it prints the full built plan and requires
explicit confirmation. Example:

```
CloudCP CLI Test Plan
=====================
Target System : <system>
Dataset(s)    : ZERO, TINY, SMALL, MEDIUM, LARGE, SPARSE   (all sizes — automatic)
Transfer Mode : upload + download + both                   (all modes — automatic)
Cloud         : <provider>
Source        : <path>
Destination   : <bucket/path>

Planned Operations:
  [1] Validate Bryck state
  [2] Mount Bryck if required (AUTO-MOUNT)
  [3] Generate datasets (ZERO/TINY/SMALL/MEDIUM/LARGE/SPARSE)
  [4] Configure cloud (cloud_ops.json will be rewritten per case, then restored)
  [5] Start transfers (upload/download/both x each dataset)
  [6] Pause/resume/cancel + auto re-transfer tests
  [7] Mount/eject lifecycle tests (INCLUDES eject-during-active-transfer)
  [8] Format/erase/remove attempts (EXECUTED FOR REAL, not just rejection checks)
  [9] Service restart tests (bcloud AND bryckapi, during active transfers)
 [10] Transfer verification (§10)
 [11] Live intervention + service restart tests (§9.2/§9.3)
 [12] Auto-cleanup datasets + cloud objects after each test
 [13] Generate reports (JSON + HTML + Markdown)

WARNING:
These operations WILL modify Bryck state, interrupt active transfers,
restart services, and execute real format/erase/remove commands. Data
generated per case is deleted automatically after that case completes.

Proceed with execution? [yes/no]:
```

The script must not proceed until the user explicitly types `yes`.

## 14. Two-Phase Workflow

**Phase 1 — Plan / Confirmation**

```bash
python3 cloud_cli_runner.py --plan
```

- Reads all configuration (`login.json`, `cloud_ops.json`, `format_mount_params.json`, `/etc/bryck/bryckcloud/config.json`).
- Checks paths exist and are valid JSON/YAML.
- Checks current Bryck state.
- Resolves all dataset tiers (§5) including SPARSE from its separate catalog.
- Builds the complete execution plan (every test case from §9, in order).
- Renders the confirmation screen from §13.
- Asks for confirmation.
- **Does not modify anything** — no mount, no datagen, no config writes.

**Phase 2 — Execute**

```bash
python3 cloud_cli_runner.py --execute --plan-file <plan.json>
```

- Loads the exact plan produced and confirmed in Phase 1 (no re-derivation, no
  new assumptions).
- Executes only the confirmed steps, in the confirmed order, actually
  performing every test case in §9 (transfers, live interventions, service
  restarts, edge cases) — this phase does real work, it does not simulate.
- Writes evidence (§11) to `results/<RUN_ID>/<TEST_ID>/` as it goes.
- Performs recovery/cleanup (§12) after each case and at run end.
- Emits final JSON + HTML + Markdown reports (decision #10) to `results/<RUN_ID>/`.

## 15. Directory Layout

```text
CloudCpCliTesting/
  cloud_cli_plan.md            # this document
  cloud_cli_runner.py          # two-phase runner: --plan / --execute
  bryckclient-cli/
    login.json
    cloud_ops.json             # rewritten per case during --execute, restored after
    format_mount_params.json
  results/
    <RUN_ID>/
      plan.json                # frozen plan from --plan, consumed by --execute
      cloud_ops.json.bak       # pre-run backup, restored at end
      <TEST_ID>/
        commands.log           # exact commands, timestamps, return codes, stdout/stderr
        report.json            # status, notes, steps[]/expected/actual/state_change (§9.7)
      summary.json
      summary.html
      summary.md

/home/bryck/shravani/            # --report-save-dir (Bryck-host only; NOT under results/)
  <RUN_ID>/
    <TEST_ID>/
      cloud_transfer_report_<transfer_id>.zip   # bryck_cloud_transfer_report.py output
    _final_diagnostic_report/                    # bryck_report.py --output-dir, once per run
```

## 16. Step-by-Step Execution Walkthrough (Worked Example: `CLI-B-SMALL`)

This is the exact command sequence the runner performs for one test case —
tier `SMALL`, dataset `DS-P1-03`, mode `both` — from a mounted, idle Bryck to
a cleaned-up result. All commands run on the Linux Bryck host from
`CloudCpCliTesting/bryckclient-cli/` unless noted.

```bash
# --- 0. Pre-flight (read-only, part of --plan) -----------------------------
python3 bryck_info.py --login login.json
#   -> confirm state; if "Ejected", plan schedules step 2 before step 3.

# --- 1. Mount (only if ejected; auto-mount per decision #4) ----------------
python3 bryck_mount.py --login login.json --params format_mount_params.json
python3 bryck_info.py --login login.json   # confirm "Mounted"

# --- 2. Generate the dataset (never while ejected) -------------------------
/home/bryck/rperiyas/datagen --spec dataset_cloudcp/spec_files/DS-P1-03/<spec>.yaml
#   repeat for every spec file listed under DS-P1-03 in manifest.json
#   -> materializes files under /bryck/cloudcp_cli/SMALL

# --- 3. Configure cloud_ops.json for this tier (dynamically rewritten) -----
#   bryck_src    = /bryck/cloudcp_cli/SMALL
#   cloud_bucket = s3://shravani/cloudcp-cli/SMALL
#   bryck_dst    = /bryck/cloudcp_cli_dl/SMALL
python3 bryck_cloud_configure.py --login login.json --params cloud_ops.json
python3 bryck_cloud_show.py --login login.json

# --- 4. Initiate the transfer (mode=both -> upload then download) ---------
python3 bryck_cloud_transfer_initiate.py --login login.json --params cloud_ops.json --mode both
#   -> capture transfer_id, e.g. 4821

# --- 5. Poll status; run live interventions from §9.2 while IN_PROGRESS ----
python3 bryck_cloud_transfer_status.py --login login.json --transfer-id 4821
python3 bryck_cloud_transfer_pause.py  --login login.json --transfer-id 4821
python3 bryck_cloud_transfer_status.py --login login.json --transfer-id 4821   # expect PAUSED
python3 bryck_cloud_transfer_resume.py --login login.json --transfer-id 4821
python3 bryck_cloud_transfer_status.py --login login.json --transfer-id 4821   # expect IN_PROGRESS
python3 bryck_cloud_transfer_cancel.py --login login.json --transfer-id 4821
python3 bryck_cloud_transfer_status.py --login login.json --transfer-id 4821   # expect CANCELLED

# --- 6. Re-transfer with the same dataset (decision #8) --------------------
python3 bryck_cloud_transfer_initiate.py --login login.json --params cloud_ops.json --mode both
#   -> capture new transfer_id, e.g. 4830; poll to COMPLETED

# --- 7. Mount/eject lifecycle + format/erase/remove attempts (§9.2) --------
python3 bryck_eject_unmount.py --login login.json          # negative test, mid-transfer on a parallel case
python3 bryck_mount.py --login login.json --params format_mount_params.json
python3 bryck_format.py --login login.json --params format_mount_params.json
python3 bryck_erase.py --login login.json
python3 bryck_remove.py --login login.json

# --- 8. Service restarts (§9.3), via SSH from login.json credentials -------
ssh <bryckserver_username>@<bryckapi_host> "sudo systemctl restart bcloud.service"
ssh <bryckserver_username>@<bryckapi_host> "sudo systemctl restart bryckapi.service"

# --- 9. Download the report and verify (§10) -------------------------------
python3 bryck_cloud_transfer_report.py --login login.json \
  --cloud-transfer-id 4830 \
  --report-path /home/bryck/shravani/<RUN_ID>/CLI-B-SMALL/cloud_transfer_report_4830.zip
#   (default --report-save-dir; override with --report-save-dir <path>)

# --- 10. Cleanup (auto, decision #12) --------------------------------------
#   - remove /bryck/cloudcp_cli/SMALL and /bryck/cloudcp_cli_dl/SMALL
#   - delete s3://shravani/cloudcp-cli/SMALL objects
#   - restore cloud_ops.json from results/<RUN_ID>/cloud_ops.json.bak (at run end)
```

Every command above is captured verbatim (with timestamp, return code,
stdout/stderr, and redacted API response) into
`results/<RUN_ID>/CLI-B-SMALL/commands.log` per §11.

## 17. Open Items

1. Confirm the exact SPARSE spec file path to standardize on (currently
   pointing at `CloudCpFallbackTesting/spec_files/06_sparse_files.yaml`) versus
   adding a dedicated sparse spec under `dataset_cloudcp/spec_files/`.
2. Confirm `bcloud`/`bryckapi` restart commands require passwordless `sudo` on
   the Bryck host for the SSH runner to execute them non-interactively.
3. Confirm the exact destination bucket/prefix naming convention to avoid
   collisions with other test suites (`CloudCpFallbackTesting`, `CloudCpBinaryTesting`)
   running against the same bucket.

## 18. Selecting Test Cases (`--suite` / `--only`)

`--suite <name>...` filters the built test-case list by **kind**, so you can
run a named group instead of remembering individual test IDs:

| Suite name | Test-case kind included |
|---|---|
| `transfer` | Upload/download/both transfer cases across all dataset tiers |
| `lifecycle` | Mount/eject/format/erase/remove lifecycle cases |
| `service` | `bcloud`/`bryckapi` restart-during-transfer cases |
| `edge` | Live-intervention edge cases (eject/cancel/pause/resume during transfer) |
| `cli-input` | `CLI-01..CLI-09` input-validation negative cases |
| `aws-negative` | `AWS-01..AWS-08` cloud/AWS configuration negative cases |
| `all` | No filtering — every case built from `--tiers`/`--modes`/`--include-*` |

Example: `python cloud_cli_runner.py --plan --suite cli-input aws-negative`
builds a plan containing only the 17 negative-test cases. `--only <ID> <ID>...`
still works for picking exact test-case IDs and takes precedence over
`--suite` if both are given.

## 19. Fixed: Mount-State Always `UNKNOWN` on Real Host

`bryck_info.py` prints the **unwrapped** `bryck_info` dict to stdout (i.e.
`"State"` is a top-level key), but `get_bryck_state()` in
`cloud_cli_runner.py` originally looked for it nested under a `"bryck_info"`
key (`payload["bryck_info"]["State"]`), which never existed in the actual
output. This meant `get_bryck_state()` always returned `"UNKNOWN"` on the real
Bryck host, which made `Executor.ensure_mounted()` believe Bryck was never
mounted and call `bryck_mount.py` before every single test case — failing
with rc=2 each time (since it actually was already mounted) and cascading to
`BLOCKED` for every transfer/lifecycle/service/edge test.

Fixed by reading `payload.get("State")` first (matching `bryck_info.py`'s
actual output shape), with a fallback to the nested `payload["bryck_info"]`
shape for resilience if that script's output format changes again. CLI-input
and AWS-negative cases were unaffected by this bug since they don't call
`ensure_mounted()`.

## 20. Size-Tier Catalog Removed; Transfers Now Use the `bryckcloud` CLI Directly

Two behavior changes supersede everything above that still mentions
ZERO/TINY/SMALL/MEDIUM/LARGE/SPARSE tiers or `bryck_cloud_transfer_initiate.py`:

1. **Size-tier dataset catalog removed.** `--dataset-catalog tiers`, `--tiers`,
   `TIER_DATASET_MAP`, and the size-tier local spec files
   (`CloudCpCliTesting/spec_files/02_tiny_files.yaml` through
   `06_sparse_files.yaml`) no longer exist. The **only** dataset sources are:
   - `--dataset-catalog all` (default) — every dataset in
     `dataset_cloudcp/spec_files/manifest.json` (`DS-P1-01`..`DS-P12-02`, 54
     datasets, each with its own `datagen` YAML specs under
     `dataset_cloudcp/spec_files/<dataset-id>/`).
   - `--dataset-catalog specfiles` — the remaining non-tier local specs under
     `CloudCpCliTesting/spec_files/` (`01_zero_byte`, `07_fill_files`,
     `08_deep_tree`, `09_unicode_names`, `10_special_char_names`,
     `11_mixed_realistic`, `12_tiny_2million`).

   Lifecycle (`CLI-LC-*`) and service-restart (`CLI-SVC-*`) test cases now run
   against one fixed representative dataset, `DS-P1-04`
   (`LIFECYCLE_DATASET` in `cloud_cli_runner.py`), instead of looping over
   tiers.

2. **Transfers are initiated via the `bryckcloud` CLI directly**, not the
   API-based `bryck_cloud_transfer_initiate.py` wrapper:
   ```bash
   /opt/bryck/.venv/bryck/bin/bryckcloud transfer add aws --src <bryck-path> --dst s3://<bucket>/<prefix>
   ```
   `Executor.initiate_transfer()` builds this command with `--src`/`--dst`
   swapped for `upload` vs `download` mode (`both` runs the upload command
   then the download command, tracking the upload's transfer id as primary
   and recording the download's id in that test case's notes). The transfer
   id is parsed from the command's stdout/stderr, falling back to diffing
   `bcloud_batchmeta`/`cloud_transfer_logs` directory listings
   (`cloudcpclitesting.py`'s `collect_transfer_ids`/`detect_transfer_id`,
   the same mechanism already used by the older single-dataset runner).
   `bryck_cloud_configure.py`/`bryck_cloud_deconfigure.py`/`bryck_cloud_show.py`
   are unchanged — they still handle cloud-provider credential setup via the
   REST API before the CLI transfer command runs. Override the CLI path with
   `--bryckcloud-bin` (default `/opt/bryck/.venv/bryck/bin/bryckcloud`).

## 21. Negative-Test Suite (`cloudcpclitesting.py --negative`)

This is the script actually being run for the ~300-case negative catalog
(distinct from `cloud_cli_runner.py`'s positive transfer matrix in §1-20
above). It consolidates the negative-test framework into one file with five
pieces:

| Piece | What it does |
|---|---|
| **Dataset Manager** (`NegDatasetManager`) | Picks a fixture dataset from `dataset_cloudcp/spec_files/manifest.json` by shorthand (`small_fast`=`DS-P2-06`/9 files, `empty`=`DS-P8-01`/0 files, `single_file`=`DS-P9-01`/1 file) or an explicit `DS-P*` id — or, with `--spec-file`, materializes one exact YAML spec (or a whole directory of specs) instead. |
| **Environment Manager** (`NegEnvironmentManager`) | Runs the real `bryckclient-cli/*.py` scripts (mount/eject/configure/deconfigure/initiate/pause/resume/cancel/status/report) and records every command, return code, stdout/stderr and duration. |
| **Test Case Manager** (`NEG_CATALOG` / `NEG_CATALOG_ORDER`) | Every ID from `NEGATIVE_TEST_PLAN.md` §7-28 (CLI, AUTH, TID, AWS, PATH, LIFE, DATASET, XFER, DOWNLOAD, STATE, RACE, DUP, REPORT, FAULT, REC, VERIFY, INT, CLEAN, MGMT, SVC, SM, F — 308 total) plus a `MASTER` section (`MASTER-UPLOAD`/`MASTER-DOWNLOAD`/`MASTER-BOTH`, 3 more — see §21.7), each with a stable `[n]` order number. |
| **Executor** (`_neg_run_case`, `run_negative_suite`) | Selects which IDs to run (`--test`/`--section`/`--range`/`--all-negative`), executes them in order, and never lets one case's exception abort the rest. |
| **Report Generator** (`_neg_write_reports`) | Writes `summary.json` + `summary.html` under `results/negative/<run-id>/`, in the same style as `cloud_transfer_only.py`'s reports. |

### 21.1 Listing test cases

```bash
python3 cloudcpclitesting.py --list-negative
```
Prints every case with its `[n]` order number, section, implementation status
(`IMPLEMENTED` vs `stub`), and name — the `[n]` number is what `--range` uses:
```
  [  1] CLI-01     [CLI     ] IMPLEMENTED  Initiate without --mode
  [  2] CLI-02     [CLI     ] IMPLEMENTED  Invalid mode
  ...
  [308] F-40        [F       ] stub         Full Download Negative Regression
```

### 21.2 Selecting what to run

| Flag | Selects |
|---|---|
| `--test AWS-03` or `--test AWS-03,CLI-01,TID-05` | One case, or an explicit comma-separated list — run a single test case or a hand-picked series. |
| `--section AWS` | Every case registered under one section (e.g. all 18 `AWS-*`). |
| `--range 1-9` | A contiguous **series by order/position**, using the `[n]` numbers from `--list-negative` (e.g. `--range 1-9` = the first 9 cases = all of CLI; `--range 42-42` runs exactly one case by position). |
| `--all-negative` | Every registered case (308). |

`--test`/`--section`/`--range`/`--all-negative` are mutually selected in that
priority order — pick exactly one per run.

### 21.3 Choosing the fixture dataset

- `--dataset-requirement small_fast|empty|single_file|<DS-P*-id>` (default
  `small_fast`) resolves a dataset from `dataset_cloudcp/spec_files/manifest.json`.
- `--spec-file <path>` **overrides** `--dataset-requirement` when given: point
  it at one spec YAML or a whole spec directory (e.g.
  `--spec-file dataset_cloudcp/spec_files/DS-P9-01` or
  `--spec-file CloudCpSchedulerTesting/spec_files/SCH-DEEP-01`) to materialize
  exactly that fixture instead of a manifest-resolved dataset id.

### 21.4 Dry-run vs. live, and where results land

- Default is **dry-run** (no `--live`): every command is logged and reported
  but never executed — used to validate the framework's own mechanics
  (argument construction, fixture generation, report shape).
- `--live` actually executes against the Bryck host in `--login`/`--cloud-ops`.
- Results always land under `--results-dir` (default
  `CloudCpCliTesting/results/negative/`) `/<run-id>/summary.json` +
  `summary.html`, where `<run-id>` defaults to a timestamp
  (`--run-id` to name it yourself, e.g. per CI job).
- **If the run is cancelled midway (Ctrl+C) or crashes unexpectedly, a report
  is still written** for exactly the cases that ran before the interruption;
  every case that never got to run is recorded with status `SKIP` and
  `summary.json`'s top-level `"interrupted": true` flag is set (the HTML
  report also shows a red "RUN WAS CANCELLED / INTERRUPTED — PARTIAL" banner).
  You never lose evidence from a long `--all-negative --live` run that gets
  interrupted partway through.

### 21.5 Performance recording

Every case records its own `duration` (seconds). The report additionally
aggregates a **performance** block:
- total run duration,
- per-case average duration,
- per-section case count / total duration / average duration,

shown in `summary.json`'s `"performance"` key and as a "Performance by
section" table at the top of `summary.html`.

### 21.6 Example runs

```bash
# See every case and its order number
python3 cloudcpclitesting.py --list-negative

# Run one case
python3 cloudcpclitesting.py --test AWS-03 --live

# Run a hand-picked series
python3 cloudcpclitesting.py --test CLI-01,AWS-03,TID-05 --live

# Run the first 9 cases (all of CLI) by position
python3 cloudcpclitesting.py --range 1-9 --live

# Run one whole section
python3 cloudcpclitesting.py --section AWS --live

# Run everything, naming the run and using an explicit spec-file fixture
python3 cloudcpclitesting.py --all-negative --live \
  --spec-file dataset_cloudcp/spec_files/DS-P9-01 \
  --run-id nightly_2026_08_20
```

### 21.7 P0 master end-to-end flows (`MASTER-UPLOAD` / `MASTER-DOWNLOAD` / `MASTER-BOTH`)

Ported from `cloud_transfer_negative_test_runner.py`'s `run_master_flow()`/
`run_master_flow_both()`. Unlike every other section (independent, isolated
cases), these three run one continuous narrative in a single test case:

```
eject (if mounted) -> format -> mount -> configure AWS -> generate dataset ->
initiate transfer -> wait IN_PROGRESS -> report -> pause -> verify PAUSED ->
report -> pause again (idempotence check) -> resume -> wait IN_PROGRESS ->
report -> attempt FORMAT/EJECT/MOUNT/DECONFIGURE while active (expected to be
rejected or at least not corrupt state) -> wait COMPLETED -> report ->
deconfigure -> eject -> cleanup
```

- `MASTER-UPLOAD` runs the upload leg only.
- `MASTER-DOWNLOAD` seeds the bucket with an upload leg first, then runs the
  download leg (a download needs source data to already exist remotely).
- `MASTER-BOTH` runs the upload leg then the download leg in the same
  mounted+configured session.

Every step is recorded as one command in that case's `commands` list (so the
whole narrative is visible in `summary.html`/`summary.json` for that single
`MASTER-*` row), and `expected`/`actual`/`reason` summarize the full flow.
Like every other case, `--live` is required (dry-run reports `BLOCKED`):

```bash
python3 cloudcpclitesting.py --test MASTER-UPLOAD --live
python3 cloudcpclitesting.py --section MASTER --live
```


