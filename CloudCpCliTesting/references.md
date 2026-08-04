# References and Integration — CloudCP CLI Testing

This document records the origin of every piece of information used to build this CLI
testing package.  If a fact is marked **"assumption"**, it was inferred from context
because the relevant private repository was not directly accessible.

---

## 1. This Repository (`Shravani-g18/cloudcp-phase-wise-testing`)

The following materials from this repo are directly incorporated:

### 1.1 Design docs (`docs/`)

| File | What was taken |
|---|---|
| `docs/bcloud_final_design.md` | System architecture, on-disk layout, broker/scheduler description, cloudcp contract (§11), exit-code semantics, verification/report schema, config reference |
| `docs/config_reference.md` | Full config.json key reference (SERVICE, DATABASE, CLOUD, TRANSFER, BATCH, CLOUDCP, SYNC, LOGGING, VERIFICATION, TEST sections) |

Key facts extracted:

- `cloudcp` binary lives at `/opt/bryck/aws/lib/` + `/opt/bryck/aws/bin/cloudcp`
- `TRANSFER_CMD` = `export LD_LIBRARY_PATH=/opt/bryck/aws/lib/; /opt/bryck/aws/bin/cloudcp`
- Multipart threshold = `CHUNK_SIZE_MB` (default 64 MB from `CLOUDCP.MULTIPART_THRESHOLD_MB`)
- S3 key rule: `key = PREFIX + strip(FS_PREFIX, absolute_path)`
- Exit codes: 0 = all success, 1 = whole batch failed, 2 = partial + `.lst` written
- Batch metadata dir: `/opt/bryck/bryckapi/downloads/bcloud_batchmeta`
- Transfer logs dir: `/opt/bryck/bryckapi/downloads/cloud_transfer_logs`
- Transfer report CSV path: `cloud_transfer_<id>/transfer_report_<id>.csv`
- Config file: `/etc/bryck/bryckcloud/config.json`

### 1.2 Test plan (`TestPlan/`)

| File | What was taken |
|---|---|
| `TestPlan/complete_plan.md` | Phase overview, priority model (P0/P2), scope, system under test baseline |
| `TestPlan/phases/03_cloudcp_binary.md` | Invocation pattern, exit-code table, test case groupings (upload correctness, tier/multipart, encoding, negative, config, performance) |

### 1.3 Dataset catalog (`dataset_cloudcp/spec_files/`)

| File | What was taken |
|---|---|
| `dataset_cloudcp/spec_files/dataset_map.json` | Full dataset catalogue: 54 datasets across 12 categories; file counts, size ranges, tier coverage |
| `dataset_cloudcp/spec_files/dataset_generation_plan.md` | Dataset design rationale, tier boundaries |

Tier boundaries (from design docs and dataset_map):

| Tier | Size range | Notes |
|---|---|---|
| Zero | 0 B | Empty objects |
| Tiny | 1 B – <1 MiB | Single-part PUT; count-seal dominant |
| Small | 1 MiB – <64 MiB | Straddles multipart threshold at 64 MiB |
| Medium | 64 MiB – <1 GiB | All multipart; byte-seal dominant |
| Large | ≥1 GiB | All multipart; long-running transfers |

Datasets used in CLI test cases:

| Dataset | Category | CLI Test Groups |
|---|---|---|
| DS-P7-01 | Mixed Full-Pipeline (~300 GB, 91,320 files) | Smoke, Config, Rerun/Skip, Report, Mixed |
| DS-P9-01…DS-P9-07 | Single-File Transfer (1 B … 100 GB) | Boundary |
| DS-P2-01 | Batch Builder Mechanics (110 boundary files) | Boundary |
| DS-P4-01 | Filename & Encoding Stress (tiny, 20 variants) | Encoding |
| DS-P4-05 | Filename & Encoding Stress (cross-tier, 20 variants) | Encoding, Mixed |
| DS-P1-04 | Single-Tier Medium (~5 TB) | Config (CHUNK_SIZE_MB tests) |
| DS-P8-01 | Configuration Edge Cases (empty source) | Report |
| DS-P8-02 | Configuration Edge Cases (single 0-byte file) | Boundary |
| DS-P8-03 | Configuration Edge Cases (single 100 GB file) | Boundary |
| DS-P8-04 | Configuration Edge Cases (14-level deep tree) | Encoding |
| DS-P3-01 | Batch Exhaustion (large exhausts first) | Config (network profile) |
| DS-P1-02 | Single-Tier Tiny (~500 GB) | Performance |
| DS-P1-06 | Single-Tier Large (~10 TB) | Performance |
| DS-P7-03 | Mixed Full-Pipeline (~10 TB) | Performance |

### 1.4 Binary testing runner (`CloudCpBinaryTesting/run_cloudcp_tests.py`)

The CLI runner (`run_cli_tests.py`) is modelled after `run_cloudcp_tests.py`.  Shared
patterns taken from that file:

- Pipeline: datagen → make_batches → stage → cloudcp → validate → clear → report
- `--list`, `--dry-run`, `--all`, `--tag`, `--case` CLI flags pattern
- Dataset spec discovery via numeric prefix (e.g. `01_zero_byte.yaml`)
- Transfer ID auto-increment from `max(transfer_*)` in batchmeta dir
- Report format: per-run JSON + Markdown summary

### 1.5 Scheduler testing (`CloudCpSchedulerTesting/schedular_test.py`)

| Pattern taken | Used in |
|---|---|
| `--spec-dir`, `--data-root`, `--batchmeta-dir`, `--transfer-logs-dir` flags | `run_cli_tests.py` flag design |
| `--scheduler-python`, `--scheduler-script` override flags | `cli_config.py` overrideable paths |
| journalctl follower for log capture | `run_cli_tests.py` log-capture model |
| HTML / zip report output | `run_cli_tests.py` report format design |

### 1.6 Log file (`cloudcplogs.txt`)

The file `cloudcplogs.txt` in the repo root is a real `cloudcp` log tail. It confirms:

- Log format: `[timestamp] [level] [module] message`
- Per-file status lines are emitted during upload
- Timing lines (preprocess/upload/postprocess) appear when `PERF_STATS=True`

---

## 2. `tsecb/bryck` Repository

> **Access status:** Repository returned 404 during automated inspection (private). The
> following facts were inferred from the `config.json` shared by the user in the problem
> statement, and from this repo's design docs which reference `tsecb/bryck` paths.

### 2.1 config.json (provided directly by user)

Full config.json at `/etc/bryck/bryckcloud/config.json`. Key facts:

| Key | Value | Used in |
|---|---|---|
| `TRANSFER.TRANSFER_CMD` | `export LD_LIBRARY_PATH=/opt/bryck/aws/lib/; /opt/bryck/aws/bin/cloudcp` | All CLI test case invocations; `cli_config.py` CLOUDCP_INVOKE |
| `TRANSFER.PARALLEL_WORKERS` | `15` | Default parallel workers; CLI-CFG-03, CLI-CFG-04 |
| `CLOUDCP.MULTIPART_THRESHOLD_MB` | `64` | Boundary test cases CLI-BOUND-03, CLI-BOUND-04 |
| `CLOUDCP.MULTIPART_CHUNKSIZE_MB` | `64` | Config knob test CLI-CFG-01, CLI-CFG-02 |
| `CLOUDCP.SKIP_EXISTING` | `true` | Rerun/Skip test group |
| `CLOUD.LOCAL_AWS` | `https://10.10.10.103:9000` | MinIO endpoint used in all test runs |
| `LOGGING.DEBUG_LOG_FILE` | `/opt/bryck/bryckapi/downloads/cloud_transfer_logs/cloudcp.log` | Log-capture in runner |
| `LOGGING.LOGS_DIR` | `/opt/bryck/bryckapi/downloads/cloud_transfer_logs` | Report path computation |
| `BATCH_FILE_DIR` | `/opt/bryck/bryckapi/downloads/bcloud_batchmeta` | Transfer staging path |
| `VERIFICATION.REPORT_FORMAT` | `json` | Report validation (CLI-RPT-06) |
| `VERIFICATION.TRANSFER_SUMMARY_FILES` | `/etc/bryck/bryckcloud/transfer_summary_files.json` | Report path (CLI-RPT-06) |
| `NETWORK_PROFILE` | `dt2_100gbe` | Network profile test CLI-CFG-09, CLI-MIX-03 |

### 2.2 Assumed paths (not directly inspected)

> These paths are referenced in this repo's docs and the provided config; assumed correct.

| Path | Purpose |
|---|---|
| `/opt/bryck/aws/bin/cloudcp` | C++ uploader binary |
| `/opt/bryck/aws/lib/` | Shared libraries for cloudcp |
| `/opt/bryck/.venv/bryck/bin/python3` | Python venv for broker |
| `/home/bryck/.aws/config` | AWS credentials file (`AWS_CONFIG_FILE`) |
| `/run/bryck/bcloud_transfer.pid` | PID file for transfer daemon |

---

## 3. `tsecb/cloud` Repository

> **Access status:** Repository returned 404 during automated inspection (private). The
> following facts were inferred from this repo's design docs, scheduler test scripts, and
> the paths referenced in `schedular_test.py`.

### 3.1 Broker / Scheduler

The `bryckcloud` Python package inside `tsecb/cloud` provides:

| Component | Path (assumed) | CLI relevance |
|---|---|---|
| `batch_scheduler.py` | `/opt/bryck/.venv/bryck/lib/python3.x/site-packages/bryckcloud/lib/cloud/batch_scheduler.py` | Main broker entry point; invoked by all CLI tests |
| `cloud/` module dir | `/opt/bryck/.venv/bryck/lib/python3.x/site-packages/bryckcloud/lib/cloud/` | `--dir-path` override flag in scheduler |

Broker CLI flags (from `schedular_test.py` defaults):

| Flag | Default | CLI test use |
|---|---|---|
| `--config` | `/etc/bryck/bryckcloud/config.json` | All CLI tests |
| `--transfer-id` | auto-incremented | Passed by `run_cli_tests.py` |
| `--poll-interval` | unset (scheduler default) | Optional; CLI-CFG tests |

### 3.2 Transfer report schema

From `docs/bcloud_final_design.md §16` (which references the `tsecb/cloud` implementation):

| Field | Type | Description |
|---|---|---|
| `file_path` | string | Absolute source path |
| `size` | int | File size in bytes |
| `status` | enum | `SUCCESS`, `SKIPPED`, `FAILED`, `MISMATCH`, `PARTIAL` |
| `s3_key` | string | Destination S3 object key |
| `transfer_id` | int | Transfer run identifier |

### 3.3 Verification engine

From `docs/bcloud_final_design.md §15`:

- Per-batch reconciliation: source index vs upload report
- Final summary: all per-batch statuses merged into transfer-level summary
- `VERIFY_S3_WORKERS=16` (from config): parallel S3 HeadObject workers
- `VERIFY_STAT_THREADS=32` (from config): parallel stat threads

---

## 4. API-Adjacent Concepts

These concepts apply to the CLI tests even though they are not CLI-layer items:

### 4.1 S3 multipart upload contract

- `CreateMultipartUpload` / `UploadPart` / `CompleteMultipartUpload` sequence
- Parts must be ≥5 MiB each (AWS S3 minimum; last part may be smaller)
- Orphaned multipart uploads must be absent after any successful or failed transfer
- `CHUNK_SIZE_MB=64` → parts ≤64 MiB

### 4.2 HeadObject verification

- Used post-transfer to confirm object exists and `Content-Length` matches source
- Part of `VERIFICATION.VERIFY_S3_WORKERS` parallel verification

### 4.3 NUL-framed batch format

- Batch files are NUL (`\0`) separated records: `<absolute_path>\0<size>\0…`
- cloudcp reads these directly; the broker stages them into
  `transfer_<id>/batches/inprogress/<tier>/`

---

## 5. Assumptions and Gaps

| Assumption | Basis | Risk if wrong |
|---|---|---|
| `batch_scheduler.py` accepts `--transfer-id` as a flag | `schedular_test.py` passes it as a positional or flag | Runner may need a different invocation pattern |
| Transfer report is a CSV at `cloud_transfer_<id>/transfer_report_<id>.csv` | `schedular_test.py` line 6; design doc §16 | Report validator path would need updating |
| `tsecb/bryck` and `tsecb/cloud` are private and not publicly accessible | 404 from GitHub API | No risk to test design; paths/contracts well-documented in this repo |
| MinIO endpoint `https://10.10.10.103:9000` is the test environment endpoint | User-provided config.json | Different endpoint in your environment → update `cli_config.py` `DEFAULT_ENDPOINT` |
| `datagen` binary at `/home/bryck/rperiyas/datagen` | `run_cloudcp_tests.py` default | Update `cli_config.py` `DEFAULT_DATAGEN_BIN` if installed elsewhere |
