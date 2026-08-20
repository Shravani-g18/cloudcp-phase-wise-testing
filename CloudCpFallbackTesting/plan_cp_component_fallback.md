# CloudCp Component-Level Fallback Test Plan

Direct, module-level tests for the **two internal fallback mechanisms** of the
cloudcp transfer pipeline. Unlike `plan_cp_fallback.md` (which drives the whole
pipeline through the Bryck REST API and injects faults via `config.json`), this
plan exercises the two components **in isolation**, exactly the way they are
invoked inside the running service:

1. **Fallback worker daemon** — `bryckcloud.lib.cloud.fallback_worker`
2. **Whole-batch boto3 retry** — `bryckcloud.lib.cloud.mp_batch_retry.retry_whole_batch()`

The target code under test lives on the Bryck at:

```
/opt/bryck/.venv/bryck/lib/python3.10/site-packages/bryckcloud/lib/cloud/
    fallback_worker.py
    mp_batch_retry.py
    batch_state.py
    upload_report.py
    net_profile.py
    aws.py
```

Runner: `cloudcp_component_fallback_test.py` (invoked **from**
`cloudcp_fallback_test.py` via its `--component*` flags; it reuses that harness's
SSH session, step recorder and datagen helpers).

---

## 1. Why these two are tested separately

Inside a real transfer both mechanisms are triggered by cloudcp exit codes:

| cloudcp rc | meaning                    | response                                             |
|-----------:|----------------------------|------------------------------------------------------|
| `0`        | batch fully succeeded      | complete the batch                                   |
| `1`        | **whole batch failed**     | `mp_batch_retry.retry_whole_batch()` (boto3 ProcessPool) |
| `2`        | **partial failure**        | cloudcp writes a per-batch `*.lst`; the **fallback worker** drains it |

Reproducing rc==1 / rc==2 through the live pipeline is non-deterministic. This
plan instead **stages the exact on-disk inputs** each component consumes and
calls the component directly, so the test is deterministic and fast.

---

## 2. On-disk contract (what each component consumes)

### 2.1 Batch file (both mechanisms)

A **NUL-framed** list of absolute source paths, one record per file:

```
/bryck/<dataset>/a.bin\0/bryck/<dataset>/b.bin\0...
```

- Built by walking the datagen source directory (same framing as
  `make_batches.py` / `batch_state.publish`).
- Placed in the tier-partitioned batch-state tree:

```
<BATCH_FILE_DIR>/transfer_<id>/batches/inprogress/<tier>/batch_000000.txt
```

- `<BATCH_FILE_DIR>` comes from `config.json` and points to
  `/opt/bryck/bryckapi/downloads/bcloud_batchmeta`.
- `<tier>` ∈ {zero, tiny, small, medium, large}. The worker locates a batch by
  **name across all tiers**, so the tier only needs to be plausible.

### 2.2 Retry list `.lst` (fallback worker only)

The worker does **not** read the batch file directly — it globs cloudcp's
per-batch retry list. Format (NUL-framed, 4 fields per record):

```
<local_path>\0<s3_path>\0<size>\0<last_error>\0
```

- Filename (from `upload_report.retry_list_path`):
  `cloudcp_retry_<id>_<batch_stem>.txt.lst`
  (e.g. `cloudcp_retry_7_batch_000000.txt.lst`).
- **Location:** the transfer **log** directory, NOT batch-meta:
  `<LOGS_DIR>/cloud_transfer_<id>/`
  (default `LOGS_DIR = /opt/bryck/bryckapi/downloads/cloud_transfer_logs`).
- The `<s3_path>` is pre-composed exactly like cloudcp does
  (`mp_batch_retry.compose_s3_key`): `s3://<bucket>/<prefix>/<relpath>` where
  `relpath` = the abspath with `fs_prefix` stripped.

### 2.3 Done marker (fallback worker only)

The worker loops until a `_fallback_done` marker exists (written by `aws.py`
after the pipeline). The test writes it after staging so the worker drains the
list and then exits on its own:

```
<BATCH_FILE_DIR>/transfer_<id>/_fallback_done
```

### 2.4 Credentials / config

Both components resolve AWS auth from the ini files referenced by
`AWS_CONFIG_FILE` in `config.json` (region + keys/role). **These are assumed to
already be configured on the target** (the bcloud service uses them). No cloud
provider is (re)configured and the service is **not** restarted — the components
are run as fresh short-lived processes that read `config.json` at startup.

---

## 3. Test flow per case

### 3.1 Fallback worker (`mechanism = worker`)

```
1. datagen  --spec <spec>                 -> /bryck/cloudcp_fallback/<dataset>
2. alloc transfer_id = max(transfer_<N> in batchmeta) + 1
3. stage batch  -> transfer_<id>/batches/inprogress/<tier>/batch_000000.txt
4. make .lst    -> cloud_transfer_<id>/cloudcp_retry_<id>_batch_000000.txt.lst
                   (s3 dst = s3://omicron/<dataset>/<relpath>)
5. write        -> transfer_<id>/_fallback_done
6. run          -> python -m bryckcloud.lib.cloud.fallback_worker
                     --transfer-id <id> --transfer-type upload
                     --transfer-dir <batchmeta>/transfer_<id> --pool-size <N>
7. verify       -> batch moved to completed/<tier>/, .lst -> .lst.done,
                   transfer_report_<id>.csv rows all FALLBACK_OK == file count,
                   objects present under s3://omicron/<dataset>/
8. cleanup      -> empty prefix, rm data + transfer dir + log dir
```

### 3.2 Whole-batch retry (`mechanism = mp`)

```
1. datagen  --spec <spec>                 -> /bryck/cloudcp_fallback/<dataset>
2. alloc transfer_id
3. stage batch  -> transfer_<id>/batches/inprogress/<tier>/batch_000000.txt
4. run driver that calls:
     ok, failed, ok_bytes = mp_batch_retry.retry_whole_batch(
         transfer_id, "upload", batch_file, bucket, prefix,
         fs_prefix, endpoint, region, local_aws, txlog=None)
   with:
     bucket   = omicron
     prefix   = <dataset>          (composed key = <dataset>/<relpath>)
     fs_prefix= /bryck/cloudcp_fallback/<dataset>
     endpoint = https://10.10.10.103:9000
     region   = us-west-1
     local_aws= CloudConfig().bcloud   (full config.json)
5. verify   -> ok == file count, failed == 0,
               transfer_report_<id>.csv rows all MP_OK == file count,
               objects present under s3://omicron/<dataset>/
6. cleanup  -> empty prefix, rm data + transfer dir + log dir
```

---

## 4. Datasets

Reuses the spec files in `spec_files/` (identical to the API-driven harness):

| key     | spec                        | why it matters for a fallback path                         |
|---------|-----------------------------|------------------------------------------------------------|
| zero    | 01_zero_byte.yaml           | zero-length object upload via boto3                        |
| tiny    | 02_tiny_files.yaml          | high file-count, retry-list volume                         |
| small   | 03_small_files.yaml         | straddles the multipart cutoff                             |
| medium  | 04_medium_files.yaml        | multi-chunk multipart via boto3                            |
| large   | 05_large_files.yaml         | long-running large multipart (**heavy**)                   |
| sparse  | 06_sparse_files.yaml        | logical-vs-physical size handling                          |
| fill    | 07_fill_files.yaml          | deterministic content — byte-exact verify                  |
| deep    | 08_deep_tree.yaml           | long keys from deep trees survive the retry                |
| unicode | 09_unicode_names.yaml       | UTF-8 key round-trip (`clean_s3_key`)                      |
| special | 10_special_char_names.yaml  | ASCII special-char keys                                    |
| mixed   | 11_mixed_realistic.yaml     | realistic mix across tiers                                 |
| scale   | 12_tiny_2million.yaml       | retry-list handling at ~2M records (**heavy**)             |

Heavy datasets (`large`, `scale`) run only with `--heavy`.

---

## 5. Case catalog

### 5.1 Worker matrix (`CFW-U-*`), mechanism = worker, upload, expect `ok`

One case per dataset: stage `.lst`, run the daemon, expect the batch drained
clean (all `FALLBACK_OK`, batch → `completed/`, `.lst` → `.lst.done`).

| id        | dataset | notes                                   |
|-----------|---------|-----------------------------------------|
| CFW-U-01  | zero    | empty-object uploads via fallback       |
| CFW-U-02  | tiny    | many small records drained              |
| CFW-U-03  | small   | single-part + multipart boundary        |
| CFW-U-04  | medium  | multipart via boto3                     |
| CFW-U-05  | large   | large multipart (heavy)                 |
| CFW-U-06  | sparse  | sparse content                          |
| CFW-U-07  | fill    | byte-exact content                      |
| CFW-U-08  | deep    | long keys                               |
| CFW-U-09  | unicode | UTF-8 key round-trip                    |
| CFW-U-10  | special | special-char keys                       |
| CFW-U-11  | mixed   | mixed tiers                             |
| CFW-U-12  | scale   | ~2M-record retry list (heavy)           |

### 5.2 Whole-batch retry matrix (`CMP-U-*`), mechanism = mp, upload, expect `ok`

One case per dataset: stage the batch, call `retry_whole_batch`, expect
`ok == file_count`, `failed == 0`, all rows `MP_OK`.

| id        | dataset | notes                                   |
|-----------|---------|-----------------------------------------|
| CMP-U-01  | zero    | zero-byte ProcessPool retry             |
| CMP-U-02  | tiny    | chunked across processes                |
| CMP-U-03  | small   | multipart boundary                      |
| CMP-U-04  | medium  | multipart                               |
| CMP-U-05  | large   | large multipart (heavy)                 |
| CMP-U-06  | sparse  | logical-vs-physical                     |
| CMP-U-07  | fill    | byte-exact (checksum-stable)            |
| CMP-U-08  | deep    | long keys                               |
| CMP-U-09  | unicode | UTF-8 key round-trip                    |
| CMP-U-10  | special | special-char keys                       |
| CMP-U-11  | mixed   | mixed tiers                             |
| CMP-U-12  | scale   | ~2M records chunked (heavy)             |

### 5.3 Negative cases (`*-N-*`), expect `fail` (graceful, no hang/crash)

| id        | mechanism | dataset | injected fault                    | expected                                            |
|-----------|-----------|---------|-----------------------------------|-----------------------------------------------------|
| CFW-N-01  | worker    | tiny    | source files deleted after stage  | worker records terminal failures; batch left `inprogress`; `.lst` NOT retired |
| CMP-N-01  | mp        | tiny    | `transfer_type = download`        | returns `(0, N, 0)` (download not handled inline); no crash |
| CMP-N-02  | mp        | tiny    | source files deleted after stage  | `failed == N`, `ok == 0`; per-file `stat` failure recorded |

---

## 6. Verdicts

| verdict | when                                                                    |
|---------|-------------------------------------------------------------------------|
| PASS    | all expectations met (counts + on-disk state as per §5)                 |
| FAIL    | wrong counts / batch not in expected state / objects missing            |
| ERROR   | staging or SSH failure, unexpected exception                            |
| PLANNED | `--dry-run` (plan printed, nothing executed)                            |

Evidence captured per case: every SSH command + its rc/stdout, the allocated
transfer id, staged batch/`.lst` paths, worker stdout, `retry_whole_batch`
return tuple, report status tallies, and object-count from `aws s3 ls`.

---

## 7. Invocation (from the existing harness)

```bash
# list component cases
python3 cloudcp_fallback_test.py --component-list

# dry-run the plan (no SSH/execution)
python3 cloudcp_fallback_test.py --component --dry-run

# run the full component suite (heavy datasets excluded)
python3 cloudcp_fallback_test.py --component

# include heavy datasets (large, scale)
python3 cloudcp_fallback_test.py --component --heavy

# run one / a comma list (by id or catalog #)
python3 cloudcp_fallback_test.py --component-one CFW-U-02,CMP-U-04

# only the negative cases
python3 cloudcp_fallback_test.py --component-negative
```

Component-specific knobs (all have sane defaults):

| flag                 | default                                            | meaning                              |
|----------------------|----------------------------------------------------|--------------------------------------|
| `--component-bucket` | `omicron`                                           | destination bucket for both          |
| `--region`           | `us-west-1`                                          | region passed to `retry_whole_batch` |
| `--venv-python`      | `/opt/bryck/.venv/bryck/bin/python3`                | interpreter that imports bryckcloud  |
| `--batchmeta-dir`    | `/opt/bryck/bryckapi/downloads/bcloud_batchmeta`    | `BATCH_FILE_DIR`                      |
| `--pool-size`        | `16`                                                | worker `--pool-size`                 |

Outputs: `runs/component_report.json` and `runs/component_report.html`
(plus a per-case `runs/<case-id>/report.json`).

---

## 8. Assumptions

- AWS credentials/region are already configured on the target (verified with a
  non-fatal `aws s3 ls` preflight against the bucket).
- Bucket `omicron` already exists (boto3 `upload_file` does not create buckets).
- `config.json` sets `BATCH_FILE_DIR` to the batch-meta path above and a
  reachable `LOCAL_AWS` endpoint.
- Passwordless SSH (reusing `bryckclient-cli/login.json`) is available; no
  `sudo` is required for these tests (they write under paths owned by the
  service account / the test user).
