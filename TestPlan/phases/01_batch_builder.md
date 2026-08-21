# Phase 1 — Batch Builder

[← Back to master plan](../complete_plan.md)

> **What this phase is:** Before any upload, the Batch Builder (`bcloud_src_enum.py`) scans
> the source directory, classifies every file into a size tier, and groups files into
> **batches** — right-sized lists handed to cloudcp one at a time. It also writes
> `source.index` (verification source of truth) and `batch_summary.csv`. This phase verifies
> that grouping is **correct, complete, crash-safe, and byte-exact** — with **no upload**.

**Priority:** P0 (correctness). Performance of enumeration is P2.
**Driven through:** the broker enumerator in `--batch-only` mode.
**Config:** `/etc/bryck/bryckcloud/config.json` (`BATCH.*` tier seals — see master §4).

---

## 1. Canonical Per-Dataset Flow

1. **Select one dataset** (e.g. `DS-P2-02`) from
   [../../dataset_cloudcp/spec_files/dataset_map.json](../../dataset_cloudcp/spec_files/dataset_map.json).
2. **Materialize the data** on the mount path (`datagen --spec <spec>.yaml`, or download from
   0.71).
3. **Compute expected `batch_summary`** from
   [../../dataset_cloudcp/spec_files/manifest.json](../../dataset_cloudcp/spec_files/manifest.json)
   + the active `BATCH.*` config (per-tier count-seal / byte-seal). Tool: `batch_summary_expect.py` (**TBA**).
4. **Run the batch builder** (no upload):
   ```bash
   /opt/bryck/.venv/bryck/bin/python3 \
     /opt/bryck/.venv/bryck/lib/python3.10/site-packages/bryckcloud/lib/cloud/bcloud_src_enum.py \
     -i <transfer-id> </bryck/mount/path/with/data> --batch-only
   ```
   Generated summary:
   ```
   /opt/bryck/bryckapi/downloads/bcloud_batchmeta/transfer_<id>/batch_summary.csv
   ```
5. **Compare** expected vs generated `batch_summary.csv`.
6. **Declare PASS / FAIL** (exact match; tolerance only where a dataset spec is approximate).

Repeat across all in-scope datasets, and repeat selected datasets under alternative
`BATCH.*` sizes and `NETWORK_PROFILE` values.

---

## 2. Expected Seal Behaviour (from baseline config)

| Tier | Count-seal | Byte-seal | Open slots |
|---|---|---|---|
| `zero`   | 2000 | — | 4 |
| `tiny`   | 511  | 256 MB | 8 |
| `small`  | 317  | 2048 MB | 8 |
| `medium` | 50   | 10240 MB | 8 |
| `large`  | 5    | 51200 MB | 8 |

A batch seals the instant **either** its count limit **or** its byte target is crossed; the
triggering file opens a new batch (never overflows the sealed one).

---

## 3. Test Cases

> **Test-case register (Excel):** the full Phase 1 case list below — plus the currently
> automated unit tests and assertion functions — is maintained as a shareable workbook at
> [../../CloudCpBatchBuilderTesting/BatchBuilder_TestCases.xlsx](../../CloudCpBatchBuilderTesting/BatchBuilder_TestCases.xlsx)
> (sheets: *Overview*, *Plan Test Cases*, *Automated Checks*).

### 3.1 Functional (P0)

| ID | Case | Dataset(s) | Pass when |
|---|---|---|---|
| P1-01 | Correct size-tier assignment (all boundaries) | DS-P2-01 (110 boundary files) | Every file's tier matches its exact byte size at 0 B / 1 B / 1 MB / 63 MB / 64 MB / 100 MB / 1 GB boundaries; no file missing/double-counted |
| P1-02 | Count-seal fires (tiny) | DS-P2-02 (2001×100 B) | Tiny batch seals at `BATCH_SIZE`; file after the limit starts a new batch; byte-seal never fires |
| P1-03 | Byte-seal fires (tiny) | DS-P2-03 (260×1 MB) | Tiny batch byte-seals at 256 MB before count limit |
| P1-04 | Count-seal fires (small) | DS-P2-04 (513×1 MB) | Small batch seals at count `BATCH_SIZE` |
| P1-05 | Count-seal fires (medium) | DS-P2-05 (65×100 MB) | Medium batch seals at count `BATCH_SIZE` |
| P1-06 | Count-seal fires (large) | DS-P2-06 (9×2 GB) | Large batch seals at count `BATCH_SIZE` |
| P1-07 | Round-robin slot fill | DS-P2-07 (800×100 KB) | Files spread ~evenly across the tier's open slots (±1); staggered close times |
| P1-08 | Filename bytes survive into batch | DS-P4-01..05 (20 variants) | Every path recovered from the NUL-framed batch == original filesystem bytes; no strip/decode of spaces/CR/newline/Latin-1/Unicode |
| P1-09 | `source.index` completeness | any mixed dataset | Record count == input file count; no duplicates; correct size + mtime |
| P1-10 | Sub-range tier isolation | DS-P10-01..08 | Each sub-range lands wholly in its expected tier with expected seal mix |

### 3.2 Resume / Crash-safety (P0)

| ID | Case | How | Pass when |
|---|---|---|---|
| P1-R1 | Resume mid-scan | Kill enum at 25/50/75%, restart same transfer id | Final `source.index` count exact in all three; pre-kill batches intact |
| P1-R2 | Resume after scan complete | Kill after `scan_state=complete` | Restart does no tree walk; no re-`stat`/`readdir`; completed batches not re-run |
| P1-R3 | Batch-id uniqueness across restarts | 3 restart cycles | All batch ids globally unique; first new id strictly > `seq_high_water` |
| P1-R4 | No-xattr skip-set | partial run + restart | Zero `getxattr`/`setxattr`; already-done files not re-batched |
| P1-R5 | Atomic publish (no partial batch) | SIGKILL during flush | No `*.tmp` visible in `pending/`; no partial records on restart |

### 3.3 Configuration (P0)

| ID | Case | Pass when |
|---|---|---|
| P1-C1 | Flat key overrides nested | Flat `TINYFILE_BATCH_SIZE` wins over nested `BATCH.TINY.BATCH_SIZE` |
| P1-C2 | Preflight free-space (<10%) | Refuses to start; clear error; non-zero exit |
| P1-C3 | Checkpoint every N files | Checkpoint written at N mark; resume ≤ N behind |
| P1-C4 | Symlink skip / loop safety | Symlink loop completes without hang; targets absent from index |
| P1-C5 | Batch config matrix | Re-run one dataset under changed `BATCH.*` seals → summary changes deterministically to the new expected |

### 3.4 Edge Cases (P0)

| ID | Setup | Dataset | Pass when |
|---|---|---|---|
| P1-E1 | Empty source dir | DS-P8-01 | `scan_state=complete`; no batch files; exit 0 |
| P1-E2 | Single 0-byte file | DS-P8-02 | One `zero` batch; one index record |
| P1-E3 | Single 100 GB file | DS-P8-03 | One `large` batch; one record |
| P1-E4 | Deep tree (14 levels) | DS-P8-04 | Correct assignment at all depths; no stack overflow |
| P1-E5 | Unreadable subdir | DS-P8-05 | Logged to `scan_errors.log`; readable files fully indexed; no crash |
| P1-E6 | 255-byte filename | DS-P4-* long-name variant | Path stored exactly; round-trips |

---

## 4. Datasets Used

| Category | Datasets | Purpose here |
|---|---|---|
| 2 — Batch Builder Mechanics | DS-P2-01 … DS-P2-07 | Boundary, count/byte seal, round-robin |
| 1 — Single-Tier Isolation | DS-P1-01 … DS-P1-06 | Pure-tier mass enumeration |
| 4 — Filename & Encoding | DS-P4-01 … DS-P4-05 | Byte-exact framing |
| 8 — Configuration Edge | DS-P8-01 … DS-P8-05 | Empty / single / deep / unreadable |
| 10 — Sub-Range Isolation | DS-P10-01 … DS-P10-08 | Fine-grained tier bands |

Expected counts per dataset/spec: [../../dataset_cloudcp/spec_files/manifest.json](../../dataset_cloudcp/spec_files/manifest.json).

---

## 5. Tools

- `datagen` — materialize data (or download from 0.71).
- `generate_specs.py` — build spec files from the plan.
- `bcloud_src_enum.py --batch-only` — the tool under test; produces `batch_summary.csv`.
- `dataset_validator.py` — generate + validate emitted file counts vs manifest.
- `batch_summary_expect.py` — expected-vs-actual `batch_summary.csv` comparator (**TBA**).

See [../tools_guide.md](../tools_guide.md) for full `--help`.

---

## 6. To Be Added

- Automated `batch_summary_expect.py` (expected summary from manifest + config; diff vs
  generated CSV; tolerance + PASS/FAIL exit code).
- Harness to run all 54 datasets unattended and roll results into the Phase 1 test-case
  register
  [../../CloudCpBatchBuilderTesting/BatchBuilder_TestCases.xlsx](../../CloudCpBatchBuilderTesting/BatchBuilder_TestCases.xlsx).
- Config-matrix driver (sweep `BATCH.*` seals and `NETWORK_PROFILE`).
- Resume/crash-injection automation (kill points at 25/50/75%).

Existing today: this plan, the Phase 1 test-case register
[../../CloudCpBatchBuilderTesting/BatchBuilder_TestCases.xlsx](../../CloudCpBatchBuilderTesting/BatchBuilder_TestCases.xlsx),
the `CloudCpBatchBuilderTesting` validation suite (remote `bcloud_src_enum.py --batch-only`
runner + assertions + reports), the dataset catalog + manifest, and
[../../dataset_cloudcp/spec_files/dataset_validator.py](../../dataset_cloudcp/spec_files/dataset_validator.py)
for generation/validation. Narrative reference: [../../docs/planv2.md](../../docs/planv2.md) Phase 1.
