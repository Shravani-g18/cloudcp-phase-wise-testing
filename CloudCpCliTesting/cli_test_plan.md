# CLI Test Plan — CloudCP

**Version:** 1.0
**Phase:** CLI (operator-visible surface, config-knob behaviour, report validation)
**Priority:** P0 unless noted
**Related docs:**
- [`../docs/config_reference.md`](../docs/config_reference.md)
- [`../docs/bcloud_final_design.md`](../docs/bcloud_final_design.md)
- [`../TestPlan/phases/03_cloudcp_binary.md`](../TestPlan/phases/03_cloudcp_binary.md)
- [`../dataset_cloudcp/spec_files/dataset_map.json`](../dataset_cloudcp/spec_files/dataset_map.json)

---

## 1. Invocation Under Test

The CLI layer under test is the broker/scheduler, not `cloudcp` directly.

```bash
# Full broker invocation (from tsecb/bryck, tsecb/cloud)
/opt/bryck/.venv/bryck/bin/python3 \
    /opt/bryck/.venv/bryck/lib/python3.x/site-packages/bryckcloud/lib/cloud/batch_scheduler.py \
    --config /etc/bryck/bryckcloud/config.json \
    --transfer-id <id> \
    [--poll-interval <sec>]
```

`cloudcp` itself is invoked by the broker using the `TRANSFER_CMD` config key:

```bash
export LD_LIBRARY_PATH=/opt/bryck/aws/lib/
/opt/bryck/aws/bin/cloudcp \
    "<batch_file>.txt" \
    --bucket <BUCKET> \
    --fs-prefix <source_root> \
    --transfer-id <id> \
    --prefix <prefix> \
    --endpoint-url <LOCAL_AWS>
```

Key composition rule: `s3_key = PREFIX + strip(FS_PREFIX, absolute_path)`

---

## 2. Exit-Code Contract (cloudcp)

| rc | Meaning | Broker action |
|---|---|---|
| 0 | All files uploaded | Batch → `completed/` |
| 1 | Whole batch failed | Broker ProcessPool boto3 retry |
| 2 | Partial failure + `.lst` written | Fallback worker drains `.lst` |

---

## 3. Test Case Groups

### Group A — Smoke (P0, fast gate)

> Dataset: **DS-P7-01** (~300 GB, 91,320 files, all tiers). Fastest mixed-workload gate.
> Estimated duration: 15–30 min on a 100 GbE link.

| ID | Test Case | Dataset | Config overrides | Pass Criteria |
|---|---|---|---|---|
| CLI-SMOKE-01 | Basic end-to-end transfer completes | DS-P7-01 | none (baseline) | Exit 0; `transfer_report_<id>.csv` has 91,320 `SUCCESS` rows; no `FAILED` or `MISMATCH` rows |
| CLI-SMOKE-02 | Transfer report exists and is well-formed | DS-P7-01 | none | CSV has headers `[file_path, size, status, s3_key, transfer_id]`; all `status` values are valid enum members |
| CLI-SMOKE-03 | All uploaded objects reachable via HeadObject | DS-P7-01 | none | Random sample of 100 keys: `HeadObject` HTTP 200; `Content-Length` matches source file size |
| CLI-SMOKE-04 | Transfer completes with PARALLEL_WORKERS=15 | DS-P7-01 | `PARALLEL_WORKERS=15` | Same pass criteria as SMOKE-01; no worker deadlock; broker exits cleanly |
| CLI-SMOKE-05 | Broker log captures per-batch timing | DS-P7-01 | `PERF_STATS=True` | `cloudcp.log` contains `preprocess`, `upload`, `postprocess` timing lines for each batch |

---

### Group B — Boundary (P0)

> Tests file-size tier transitions and multipart threshold.
> Dataset: **DS-P2-01** (110 files at exact boundary values) or single-file datasets DS-P9-01…DS-P9-07.

| ID | Test Case | Dataset | Config overrides | Pass Criteria |
|---|---|---|---|---|
| CLI-BOUND-01 | Zero-byte file transfers to S3 as empty object | DS-P9-01 (1 B) or DS-P8-02 (0 B) | none | `HeadObject` returns `Content-Length: 0`; status `SUCCESS`; no error |
| CLI-BOUND-02 | 1-byte file (absolute minimum tiny) | DS-P9-01 | none | `HeadObject` HTTP 200; `Content-Length: 1`; single-part PUT (not multipart) confirmed in S3 access log |
| CLI-BOUND-03 | 63 MB file uses single-part PUT | DS-P9-03 | `CHUNK_SIZE_MB=64` (default) | S3 access log shows `PutObject`, not `CreateMultipartUpload` |
| CLI-BOUND-04 | 64 MB file triggers multipart upload | DS-P9-04 | `CHUNK_SIZE_MB=64` (default) | S3 access log shows `CreateMultipartUpload`; zero incomplete multipart uploads remain after transfer |
| CLI-BOUND-05 | 100 MB file (small→medium boundary) uses multipart | DS-P9-05 | none | Multipart confirmed; `HeadObject Content-Length` matches source |
| CLI-BOUND-06 | 1 GB file (medium→large boundary) completes cleanly | DS-P9-06 | none | Status `SUCCESS`; `Content-Length` matches; no orphaned multipart |
| CLI-BOUND-07 | 100 GB single file (large-tier extreme) | DS-P9-07 | none | Transfer completes; `Content-Length` matches; zero incomplete multipart uploads |
| CLI-BOUND-08 | Exact boundary dataset (all 11 boundary sizes) | DS-P2-01 | none | Each of the 110 boundary files has `SUCCESS`; files ≥64 MB use multipart; files <64 MB use single-part |

---

### Group C — Encoding (P0)

> Tests filename encoding round-trip through the broker → cloudcp → S3 pipeline.
> Dataset: **DS-P4-01** (tiny tier, all 20 filename variants).

| ID | Test Case | Dataset | Config overrides | Pass Criteria |
|---|---|---|---|---|
| CLI-ENC-01 | Unicode filenames (emoji, CJK, Cyrillic) round-trip | DS-P4-01 or DS-P4-05 | none | `HeadObject` key matches source filename byte-for-byte after UTF-8 encoding; status `SUCCESS` for all variant rows |
| CLI-ENC-02 | ASCII special chars (spaces, parens, ampersands) round-trip | DS-P4-01 (FN-11, FN-12) | none | No mangling in S3 key; `Content-Length` correct |
| CLI-ENC-03 | Filename with embedded spaces | DS-P4-01 (FN-02) | none | S3 key contains literal space (URL-encoded in HTTP but stored as raw UTF-8 key); `HeadObject` 200 |
| CLI-ENC-04 | Long filename (~240 chars) | DS-P8-03 (FN-07) | none | Key length ≤1024 bytes; `HeadObject` 200; no truncation |
| CLI-ENC-05 | Deep path (~14 levels) yields correct key | DS-P8-04 | none | Key = `PREFIX + strip(FS_PREFIX, path)`; no path separator lost |
| CLI-ENC-06 | Cross-tier encoding (all 20 variants, all tiers) | DS-P4-05 | none | 12,550 `SUCCESS` rows; sample of each variant at each tier passes `HeadObject` |

---

### Group D — Config Knobs (P0)

> Validates that changing individual config.json keys produces the stated effect.
> Dataset: **DS-P7-01** (~300 GB mixed) unless noted.

| ID | Test Case | Dataset | Config overrides | Pass Criteria |
|---|---|---|---|---|
| CLI-CFG-01 | CHUNK_SIZE_MB=8 (smaller chunks → more parts) | DS-P1-04 (medium) | `CHUNK_SIZE_MB=8` | S3 access log shows parts of ≤8 MiB for medium files; transfer completes; `SUCCESS` for all rows |
| CLI-CFG-02 | CHUNK_SIZE_MB=128 (larger chunks → fewer parts) | DS-P1-04 (medium) | `CHUNK_SIZE_MB=128` | Parts are ≤128 MiB; transfer completes; same file count and sizes |
| CLI-CFG-03 | PARALLEL_WORKERS=1 (serial transfer) | DS-P7-01 | `PARALLEL_WORKERS=1` | Transfer completes; no concurrency-related errors; all rows `SUCCESS` |
| CLI-CFG-04 | PARALLEL_WORKERS=32 (high concurrency) | DS-P7-01 | `PARALLEL_WORKERS=32` | Transfer completes; log shows ≤32 concurrent workers; all rows `SUCCESS` |
| CLI-CFG-05 | HI_PERF_OPT=False disables optimizations | DS-P7-01 | `HI_PERF_OPT=False` | Transfer still completes correctly; no crash or hang |
| CLI-CFG-06 | PERF_STATS=False suppresses timing logs | DS-P7-01 | `PERF_STATS=False` | `cloudcp.log` does not contain timing lines; transfer still succeeds |
| CLI-CFG-07 | TM_THREAD_POOL_SIZE=4 (cloudcp thread pool) | DS-P1-04 | `TM_THREAD_POOL_SIZE=4` | Transfer completes; parts are still correctly assembled |
| CLI-CFG-08 | LOCAL_AWS endpoint (MinIO target) | DS-P7-01 | `LOCAL_AWS=https://10.10.10.103:9000` | All S3 API calls hit the MinIO endpoint; no calls to public AWS |
| CLI-CFG-09 | Network profile dt2_100gbe (large-heavy weights) | DS-P3-01 | `NETWORK_PROFILE=dt2_100gbe` | Large-tier worker slots allocated proportionally; log confirms tier weights |
| CLI-CFG-10 | MAX_CONCURRENT_TRANSFERS=3 (low concurrency cap) | DS-P7-01 | `MAX_CONCURRENT_TRANSFERS=3` | Never more than 3 active cloudcp processes; transfer completes |

---

### Group E — Rerun / Skip / Resume (P0)

> Validates SKIP_EXISTING, intra-batch resume, and re-run idempotency.
> Dataset: **DS-P7-01** (~300 GB mixed).

| ID | Test Case | Dataset | Config overrides | Pass Criteria |
|---|---|---|---|---|
| CLI-SKIP-01 | SKIP_EXISTING=true skips already-uploaded files on re-run | DS-P7-01 (2nd run) | `SKIP_EXISTING=true` | Second run report shows ≥90% rows as `SKIPPED`; no `FAILED`; all files present on S3 |
| CLI-SKIP-02 | SKIP_EXISTING=false re-uploads all files | DS-P7-01 (2nd run) | `SKIP_EXISTING=false` | Report shows `SUCCESS` for all rows (no `SKIPPED`); `Content-Length` matches for all |
| CLI-SKIP-03 | Interrupt mid-transfer, resume completes remainder | DS-P7-01 | none | Kill broker at ~50% progress; restart; combined first+second-run reports cover 100% of files with `SUCCESS` or `SKIPPED` |
| CLI-SKIP-04 | Re-run with identical source produces idempotent report | DS-P7-01 | `SKIP_EXISTING=true` | Report row count matches file count; no duplicates; all `SKIPPED` or `SUCCESS` |
| CLI-SKIP-05 | AZURE_RESUME=False (resume disabled; clean restart) | DS-P7-01 | `AZURE_RESUME=False` | Restart processes all files from scratch; `SKIPPED` count depends on SKIP_EXISTING |

---

### Group F — Transfer Report Validation (P0)

> Validates the structure and correctness of the transfer report produced by the broker.
> Dataset: **DS-P7-01** (for realistic row count); see also dataset mapping below.

| ID | Test Case | Dataset | Config overrides | Pass Criteria |
|---|---|---|---|---|
| CLI-RPT-01 | Report CSV is well-formed (headers, encoding, row count) | DS-P7-01 | none | UTF-8; headers present; row count = 91,320; no BOM issues |
| CLI-RPT-02 | All status values are valid enum members | DS-P7-01 | none | `status` column contains only: `SUCCESS`, `SKIPPED`, `FAILED`, `MISMATCH`, `PARTIAL` |
| CLI-RPT-03 | `s3_key` matches expected composition rule | DS-P7-01 | none | For a sample of 200 rows: `s3_key == PREFIX + strip(FS_PREFIX, file_path)` |
| CLI-RPT-04 | `size` in report matches `HeadObject Content-Length` | DS-P7-01 | none | Random sample of 50 rows: `size` field == actual S3 object size |
| CLI-RPT-05 | Report written to expected path | DS-P7-01 | none | Report at `/opt/bryck/bryckapi/downloads/cloud_transfer_logs/cloud_transfer_<id>/transfer_report_<id>.csv` |
| CLI-RPT-06 | JSON summary file structure | DS-P7-01 | none | `transfer_summary_files.json` present at `TRANSFER_SUMMARY_FILES` path; contains `transfer_id`, `status`, `total`, `success`, `failed` fields |
| CLI-RPT-07 | Zero-file transfer produces empty-but-valid report | DS-P8-01 (empty source) | none | CSV has headers only; `scan_state=complete`; no batch files; no error |

---

### Group G — Mixed Workload (P0)

> A combined realistic dataset (~4 GB, fast), suitable for regular regression.
> Uses a small slice from DS-P7-01 or a custom mixed prep.

| ID | Test Case | Dataset | Config overrides | Pass Criteria |
|---|---|---|---|---|
| CLI-MIX-01 | Mixed 4 GB transfer (zero+tiny+small+medium files) | DS-P7-01 subset or custom 4 GB mix | none | All rows `SUCCESS`; report well-formed; no `FAILED` |
| CLI-MIX-02 | Mixed dataset with all 20 filename variants | DS-P4-05 (12,550 files) | none | All 12,550 rows `SUCCESS`; encoding round-trip correct |
| CLI-MIX-03 | Mixed dataset under low-bandwidth profile | DS-P7-01 subset | `NETWORK_PROFILE=wan_lowbw` | Transfer completes; batch hashes identical to dt2_100gbe run of same dataset |

---

### Group H — Performance (P2, not a release gate)

| ID | Test Case | Dataset | Config overrides | Measure |
|---|---|---|---|---|
| CLI-PERF-01 | Tiny-file throughput (files/sec) | DS-P1-02 (~500 GB, 1M tiny) | `PARALLEL_WORKERS=15` | Files/sec and PUT/sec logged; baseline recorded |
| CLI-PERF-02 | Large-file bandwidth saturation | DS-P1-06 (~10 TB, 200 large) | `PARALLEL_WORKERS=15` | Sustained MB/s; target ≥80% of 100 GbE |
| CLI-PERF-03 | Mixed full-pipeline scale | DS-P7-03 (~10 TB, 1.17M files) | `PARALLEL_WORKERS=15` | Wall time; per-tier completion order recorded |

---

## 4. Dataset Mapping Summary

| Group | Dataset(s) | Approx Size | Rationale |
|---|---|---|---|
| Smoke | DS-P7-01 | ~300 GB, 91,320 files | Fastest mixed-workload gate; all tiers present |
| Boundary | DS-P9-01…DS-P9-07, DS-P2-01, DS-P8-02 | 1 B … 100 GB (individual files) | Single-file tier probes at every boundary |
| Encoding | DS-P4-01, DS-P4-05 | ~10 GB, ~32,500 files | All 20 filename variants; all tiers |
| Config knobs | DS-P7-01, DS-P1-04 | ~300 GB; ~5 TB | Mixed base; medium-only for multipart chunk tests |
| Rerun/Skip | DS-P7-01 | ~300 GB | Idempotency and resume need realistic file count |
| Report validation | DS-P7-01, DS-P8-01 | ~300 GB; empty | Report row count and structure |
| Mixed 4 GB | DS-P7-01 (subset) or custom | ~4 GB | Fast CI regression; all tiers, any filename variant |
| Performance | DS-P1-02, DS-P1-06, DS-P7-03 | ~500 GB … ~10 TB | Throughput baselines |

### Mixed 4 GB dataset composition guidance

For **CLI-MIX-01** and similar quick-regression cases, the recommended ~4 GB mixed
dataset includes:

| Tier | File count | Size range | Approx contribution |
|---|---|---|---|
| Zero | 500 | 0 B | 0 GB |
| Tiny | 10,000 | 100 KB – 500 KB | ~1.5 GB |
| Small | 50 | 10 MB – 30 MB | ~1 GB |
| Medium | 5 | 100 MB – 200 MB | ~750 MB |
| Large | 1 | 500 MB | ~500 MB |

Generate with:

```bash
python3 scripts/dataset_prep.py --suggest mixed --total-gb 4
```

---

## 5. Pass / Fail Criteria Summary

| Criterion | Pass | Fail |
|---|---|---|
| Transfer exit code | Broker exits 0 | Broker exits non-zero |
| Report row count | Equals source file count | Any discrepancy |
| Report status values | All `SUCCESS` (or `SKIPPED` on re-run) | Any `FAILED` or `MISMATCH` without expected cause |
| S3 object presence | `HeadObject` 200 for sampled keys | Any 404 or size mismatch |
| No orphaned multipart uploads | `ListMultipartUploads` returns empty | Any incomplete upload listed |
| Config knob effect | Observed behaviour matches documented knob effect | Discrepancy between config and observed behaviour |
| Encoding round-trip | S3 key byte-equals UTF-8 encoding of source filename | Any truncation, substitution, or mangling |
| Report file location | CSV at documented path | Missing or at unexpected path |

---

## 6. Test Execution Order (Recommended)

1. **Smoke** (CLI-SMOKE-01…05) — gates everything else.
2. **Report validation** (CLI-RPT-01…07) — confirms observability before deeper tests.
3. **Boundary** (CLI-BOUND-01…08) — tier and multipart correctness.
4. **Encoding** (CLI-ENC-01…06) — filename safety.
5. **Config knobs** (CLI-CFG-01…10) — one knob at a time, restore between cases.
6. **Rerun/Skip** (CLI-SKIP-01…05) — needs a clean bucket before first run.
7. **Mixed workload** (CLI-MIX-01…03) — combined regression.
8. **Performance** (CLI-PERF-01…03) — only after all P0 cases pass.

---

## 7. Prerequisites

- `cloudcp` binary at `/opt/bryck/aws/lib/` and `/opt/bryck/aws/bin/cloudcp`
- `datagen` binary at `/home/bryck/rperiyas/datagen`
- `batch_scheduler.py` at
  `/opt/bryck/.venv/bryck/lib/python3.x/site-packages/bryckcloud/lib/cloud/batch_scheduler.py`
- MinIO (or AWS S3) reachable at the configured `LOCAL_AWS` endpoint
- Bucket `aditya` exists and is writable (or override via `--bucket`)
- `/etc/bryck/bryckcloud/config.json` writable by the test runner user
- Python ≥3.8 with `boto3` and `pyyaml` available in the venv

---

## 8. Config Snapshot (Baseline)

All tests run against this baseline unless the test case specifies an override.
Full reference: [`../docs/config_reference.md`](../docs/config_reference.md).

```json
{
  "TRANSFER": {
    "PARALLEL_WORKERS": 15,
    "TRANSFER_CMD": "export LD_LIBRARY_PATH=/opt/bryck/aws/lib/; /opt/bryck/aws/bin/cloudcp",
    "TRANSFER_CLIENT_TYPE": "transfermanager",
    "TM_THREAD_POOL_SIZE": 4,
    "CHUNK_SIZE_MB": 64,
    "HI_PERF_OPT": "True",
    "PERF_STATS": "True",
    "SKIP_EXISTING": false
  },
  "CLOUDCP": {
    "MULTIPART_THRESHOLD_MB": 64,
    "MULTIPART_CHUNKSIZE_MB": 64,
    "SKIP_EXISTING": true,
    "TRANSFER_STATS": true,
    "STATS_INTERVAL_SEC": 0
  },
  "CLOUD": {
    "LOCAL_AWS": "https://10.10.10.103:9000",
    "PROVIDER": "minio"
  },
  "NETWORK_PROFILE": "dt2_100gbe"
}
```
