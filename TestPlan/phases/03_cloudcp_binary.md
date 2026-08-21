# Phase 3 — CloudCP Binary

[← Back to master plan](../complete_plan.md)

> **What this phase is:** `cloudcp` is the C++ upload engine. It reads a **batch file**
> (NUL-framed list of absolute source paths + sizes), strips `--fs-prefix`, prepends
> `--prefix`, and uploads each file to the object store. This phase validates **upload
> correctness, key composition, tier/multipart behaviour, encoding round-trip, and the
> exit-code contract** — including a full **negative / hostile** suite.

**Priority:** P0 (upload correctness). Throughput is P2.
**Status:** This phase already has a **complete, runnable suite** in
[../../CloudCpBinaryTesting/](../../CloudCpBinaryTesting/) — see
[plan_cp_binary.md](../../CloudCpBinaryTesting/plan_cp_binary.md).

---

## 1. Invocation Under Test

```bash
LD_LIBRARY_PATH=/opt/bryck/aws/lib/ /opt/bryck/aws/bin/cloudcp \
  "<batch_file>.txt" \
  --bucket aditya \
  --fs-prefix /bryck/1mb_halfmill \
  --transfer-id 103 \
  --prefix cloudcp_test2 \
  --endpoint-url https://10.10.10.103:9000
```

Key rule: `key = <prefix> + strip(<fs-prefix>, <absolute path>)`. cloudcp opens the absolute
path directly and owns key composition (including UTF-8 normalization at upload time).

**Size tiers (cloudcp `classify_bucket`):** `zero` (0 B), `tiny` (1 B–<1 MiB), `small`
(1–<64 MiB), `medium` (64–<1024 MiB), `large` (≥1 GiB). Multipart begins at the 8 MiB
`upload_cutoff` (chunk size 8 MiB).

---

## 2. Exit-Code Contract

| rc | Meaning | Downstream |
|---|---|---|
| 0 | All files uploaded | Batch → `completed/` |
| 1 | Whole batch failed | Broker inline ProcessPool boto3 retry ([Phase 5](05_fallback.md)) |
| 2 | Partial failure + `.lst` written | Fallback worker drains `.lst` ([Phase 5](05_fallback.md)) |

---

## 3. Test Cases

> **Test-case register (Excel):** the full binary case list — positive datasets, plan
> correctness/tier/encoding cases, the negative/hostile suite, and the pause/resume suite —
> is maintained as a shareable workbook at
> [../../CloudCpBinaryTesting/CloudCpBinary_TestCases.xlsx](../../CloudCpBinaryTesting/CloudCpBinary_TestCases.xlsx)
> (sheets: *Overview*, *Plan Cases (P3)*, *Positive Datasets*, *Negative Suite*, *Pause & Resume*).

### 3.1 Upload Correctness (P0)

| ID | Case | Dataset | Pass when |
|---|---|---|---|
| P3-01 | Successful batch, all on S3 | positive specs 01–04 | Every `HeadObject` HTTP 200, size matches; keys = prefix+stripped path; rc 0 |
| P3-02 | Intra-batch resume | any | Kill at 50%, restart → ~50% `SKIPPED`; `SUCCESS+SKIPPED` = total |
| P3-03 | HeadObject size confirm before SUCCESS | mocked wrong size | Size-mismatch files never `SUCCESS`; surface as `MISMATCH` |
| P3-04 | Key composition, all filename variants | 09_unicode, 10_special, DS-P4-* | HeadObject 200 for every variant; bytes unchanged (space/CR/newline/non-UTF-8) |
| P3-05 | Single-file transfers across boundaries | DS-P9-01 … DS-P9-07 | Each single file routes to correct tier; correct single-part vs multipart path |

### 3.2 Tier & Multipart (P0)

| ID | Case | Dataset | Pass when |
|---|---|---|---|
| P3-T1 | Below cutoff → single PUT | small band <8 MiB | Single-part PUT confirmed in S3 access log |
| P3-T2 | Above cutoff → multipart | 04_medium / 05_large | Multipart used; zero incomplete multipart uploads left behind |
| P3-T3 | 64 MiB boundary | DS-P9-04 (64 MB) | First size that must use multipart |
| P3-T4 | Sparse/large logical size | 06_sparse, 05_large | Correct logical size uploaded without consuming full disk |

### 3.3 Encoding Round-Trip (P0)

| ID | Case | Dataset | Pass when |
|---|---|---|---|
| P3-EN1 | Unicode/CJK/emoji/Cyrillic + spaces | 09_unicode_names | Byte-exact key round-trip |
| P3-EN2 | ASCII special chars | 10_special_char_names | No shell/key mangling |
| P3-EN3 | Embedded newline/CR/non-UTF-8 | N08/N09 negative | NUL framing preserved; byte-exact |

### 3.4 Negative / Hostile Suite (P0)

Built by `make_batches.py --negative`. No crash/hang/segfault; per-record attributable
errors; clean exit-code contract.

| Group | IDs | Coverage |
|---|---|---|
| Hostile filesystem objects | N01–N11 | broken symlink, symlink→file/dir, unreadable (`chmod 000`), FIFO, 0-byte, spaces, newline/CR, non-UTF-8, ~255-byte name, ~PATH_MAX path |
| Hostile extended attributes | N12–N16 | valid user xattr, oversized (>64 KiB) value, non-UTF-8/binary value, many (64) xattrs, corrupted checksum-style attr (Linux-only; via `batch_xattr.txt`) |
| Malformed batch framing | B01–B12 | empty, missing terminator, double/leading/only NULs, dangling paths, directory entry, CRLF paths, non-UTF-8, over-long path, whitespace-only, mixed valid/invalid |
| Corrupted batch over **real data** (Scenario B) | C01–C06 | truncated tail, missing terminator, double NUL, leading NUL, whitespace record, mixed valid/dangling — valid records **before** the corruption must still upload |

Pass: valid records in a mixed batch (B12) still succeed; invalid ones reported; exit code
reflects partial vs total failure. **Xattr (N12–N16):** per the confirmed policy — preserved
metadata round-trips byte-exact with size limits enforced and bad checksums caught/ignored, **or**
xattrs are ignored and object bytes still upload cleanly; no case crashes reading an attribute
(see [plan_cp_binary.md §4c](../../CloudCpBinaryTesting/plan_cp_binary.md)).

### 3.5 Pause / Resume (P0)

The orchestrator kills the running `cloudcp` process mid-transfer and later restarts it with
**identical arguments** (same batch file, same `--transfer-id`). On restart `cloudcp` reads
`/opt/bryck/bryckapi/downloads/cloud_transfer_logs/cloudcp.log` as the source of truth for
which files are already durably uploaded, then continues from the remaining records. The
suite (`run_cloudcp_tests.py --pause-resume`) reproduces this SIGKILL kill–restart cycle.

| ID | Dataset | Kill after | Cycles | Pass when |
|---|---|---|---|---|
| PR01 | `tiny_files` | 5 s (~32%) | 1 | Basic resume from log; final report all `SUCCESS`; count == expected |
| PR02 | `small_files` | 8 s | 1 | Resume that crosses the 8 MiB multipart cutoff; partial multipart objects resumed |
| PR03 | `tiny_files` | 2 s (~13%) | 1 | Very early kill still completes the full dataset on resume |
| PR04 | `tiny_files` | 12 s (~75%) | 1 | Only the tail of the batch is uploaded on resume; report still complete |
| PR05 | `tiny_files` | 5 s × 2 | 2 | Double kill/resume; `cloudcp.log` accumulates state; committed files never re-uploaded |
| PR06 | `unicode_names` | 5 s | 1 | Non-ASCII paths in `cloudcp.log` round-trip byte-exact on resume |

**Pass (all PR):** final `transfer_report_<id>.csv` lists every expected file as `SUCCESS`;
row count == spec's expected count; `cloudcp` never crashes/hangs on resume (exit 0 or a
meaningful non-zero, never a signal kill); the pre-resume `cloudcp.log` baseline shows some
files were committed before the kill (confirming the kill landed mid-transfer).

> Deferred (out of scope this iteration): **PR07** tampered/truncated `cloudcp.log`, and
> pause/resume over a malformed batch — see
> [plan_cp_binary.md §9.6](../../CloudCpBinaryTesting/plan_cp_binary.md).

### 3.6 Configuration (P0)

| Setting | Value | Validate |
|---|---|---|
| `CHUNK_SIZE_MB` | `8` | Multipart parts are 8 MiB (S3 access log) |
| `LOCAL_AWS` | MinIO endpoint | All S3 calls hit the configured endpoint |

### 3.7 Performance (P2)

| ID | Case | Dataset | Measure |
|---|---|---|---|
| P3-PERF1 | Tiny-file enumeration/upload scale | 12_tiny_2million / DS-P1-02 | Files/sec, PUT/sec |
| P3-PERF2 | Large-file bandwidth | 05_large / DS-P1-06 | Sustained bandwidth |

---

## 4. Datasets Used

- **Positive specs (binary suite):** `01_zero_byte` … `12_tiny_2million` in
  [../../CloudCpBinaryTesting/data/specs/](../../CloudCpBinaryTesting/data/specs/).
- **Catalog datasets:** category 9 (DS-P9-01…07 single-file), category 1 (single-tier),
  category 4 (encoding).
- **Negative:** generated by `make_batches.py --negative` (no datagen equivalent).

---

## 5. Tools

- `make_batches.py` — build positive batches + negative/hostile suite.
- `run_cloudcp_tests.py` — end-to-end orchestrator (datagen → batch → cloudcp → validate →
  clear bucket → report).
- `cloudcp` — the binary under test.

Full `--help`: [../tools_guide.md](../tools_guide.md).
Runnable plan: [../../CloudCpBinaryTesting/plan_cp_binary.md](../../CloudCpBinaryTesting/plan_cp_binary.md).

---

## 6. To Be Added

- Wire the binary suite to **broker-produced** batch files (currently uses
  `make_batches.py`-staged batches).
- Integrate `run_cloudcp_tests.py` results into the binary test-case register
  [../../CloudCpBinaryTesting/CloudCpBinary_TestCases.xlsx](../../CloudCpBinaryTesting/CloudCpBinary_TestCases.xlsx).
- Automated S3-access-log assertions for single-part vs multipart.
- Pause/resume follow-ups: **PR07** tampered/truncated `cloudcp.log`, and pause/resume over a
  malformed batch (deferred — see [plan_cp_binary.md §9.6](../../CloudCpBinaryTesting/plan_cp_binary.md)).

Existing today: complete binary suite (plan + runner + positive/negative datasets +
pause/resume suite) and the test-case register
[../../CloudCpBinaryTesting/CloudCpBinary_TestCases.xlsx](../../CloudCpBinaryTesting/CloudCpBinary_TestCases.xlsx).
Narrative reference: [../../docs/planv2.md](../../docs/planv2.md) Phase 2.1.
