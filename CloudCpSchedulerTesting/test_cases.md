# Phase 2 — Scheduler Test Cases (Deterministic Enumeration)

[← Phase plan](../TestPlan/phases/02_scheduler.md) ·
Design: [deterministic_enumeration_datasets.md](deterministic_enumeration_datasets.md) ·
Dataset catalog: [spec_files/manifest.json](spec_files/manifest.json)

> **Ground truth vs. under test.** The `SCH-*` datasets fix *what work exists and in what
> **enumeration** order* (deterministic — the oracle). The scheduler/broker decides ***dispatch**
> order* (under test). A passing scheduler may dispatch in a completely different order than files
> were enumerated — that is expected (design doc §9).
>
> - **Group A** (enumeration + batch shapes) is a **P0 precondition**: it proves the pending set the
>   scheduler tests stand on. Deterministic; independent of the scheduler and of `scandir`.
> - **Group B** (dispatch/weight/work-stealing) is the **scheduler under test**, run against the
>   known pending backlog the `SCH-DEEP-*` datasets guarantee exist.
> - **Group C** is configuration/robustness.
> - **Group E** is pause/resume: kill the broker + workers mid-run and restart on the same
>   `transfer-id`/`transfer-dir`, verifying already-done work is not re-uploaded (positive) and
>   that faulty resumes fail cleanly (negative).

Datasets (see [manifest.json](spec_files/manifest.json) for full per-level detail):

| Group | Datasets | Profile | Per dataset |
|---|---|---|---|
| ORDER (enumeration breadth) | `SCH-ORD-01 … 12` | ORDER | 29 688 files, 56 batches |
| DEEP (scheduling stress) | `SCH-DEEP-01 … 03` | DEEP | 1 068 680 files, 1032 batches |

Per-tier constants used by the oracle (design doc §2): `M`=BATCH_SIZE, `K`=OPEN_BATCHES,
`block = M×K`, full-batch `nbytes = M × file_size`.

| Tier | file size | M | K | block | ORDER batches/level | DEEP batches/level |
|---|---:|---:|---:|---:|---:|---:|
| ZERO   | 0 B      | 2000 | 4 | 8000 | 8  | 400 |
| TINY   | 16 KiB   | 511  | 8 | 4088 | 16 | 400 |
| SMALL  | 2 MiB    | 317  | 8 | 2536 | 16 | 200 |
| MEDIUM | 100 MiB  | 50   | 8 | 400  | 8  | 16  |
| LARGE  | 1 GiB    | 5    | 8 | 40   | 8  | 16  |

---

## Group A — Enumeration & BatchBuilder Oracle (P0, deterministic precondition)

Run with `BATCH_BUILDER_ONLY=true` (build to `pending/` without dispatch) so the full batch set is
observable. Applies to every `SCH-ORD-01 … 12` unless noted.

| ID | Case | Dataset(s) | Procedure | Pass when |
|---|---|---|---|---|
| SCH-EN-01 | Enumeration order is chain-contiguous | SCH-ORD-01 … 12 | Read `source.index`; group by tier | `source.index` is exactly `[n1×C1, n2×C2, n3×C3, n4×C4, n5×C5]` in the dataset's `C1..C5` chain order, with per-tier counts from manifest (`enumeration_order` + `num_files`) |
| SCH-EN-02 | Single-chain walk order | SCH-ORD-01 … 12 | Inspect `scan.discovered` / `scan.completed` | Exactly one directory per level (frontier depth 1 throughout); child dir descended only after parent's files complete (design doc F1) |
| SCH-EN-03 | `scandir` invariance | SCH-ORD-01 (repeat ×3) | Rebuild 3× (same seed); diff batch metadata | Batch **composition** (per-tier count, size, tier, sequence) is byte-identical across runs; only filenames *inside* a batch may differ (design doc R1) |
| SCH-BA-01 | Batch count per tier | SCH-ORD-01 … 12 | Count `batches.created` per tier | Matches ORDER column above (Z 8, T 16, S 16, M 8, L 8; total **56**) |
| SCH-BA-02 | Full-batch shape | SCH-ORD-01 … 12 | For each full batch, read `(nfiles, nbytes)` | Every full batch: `nfiles == M(tier)` and `nbytes == M × file_size` (design doc §2.1) |
| SCH-BA-03 | finish()-flush handling | SCH-ORD-01 (Z,T,S = R2; M,L = R1) | Separate streamed vs finish() batches | Streamed rounds (R≥2 tiers) publish in chain order; each tier's final K batches form an **unordered** finish() set — no cross-tier publish-order assertion for R=1 tiers (design doc §3.2). Order for R=1 tiers proven only via `source.index` (SCH-EN-01) |
| SCH-BA-04 | DEEP batch-count oracle | SCH-DEEP-01 … 03 | Count `batches.created` per tier | Matches DEEP column (Z 400, T 400, S 200, M 16, L 16; total **1032**) — establishes the pending backlog for Group B |

---

## Group B — Scheduler Dispatch (P0, under test)

Precondition: Group A green for the dataset, and the full pending backlog materialised (run
`BATCH_BUILDER_ONLY` first, or let enumeration outrun dispatch). Sample per-tier in-flight batch
counts every N seconds. Assert against **known pending counts (§8.2)** — never against creation
order.

| ID | Case | Dataset | Pass when |
|---|---|---|---|
| SCH-SD-01 | Steady-state weight ratio | SCH-DEEP-01 | In-flight slots per tier match configured `NETWORK_PROFILE` weight ratio (±5%) while all tiers have pending work |
| SCH-SD-02 | Per-tier hard cap | SCH-DEEP-01 | `in-flight[tier] ≤ max_concurrent[tier]` at every sample, despite the deep backlog |
| SCH-SD-03 | Same-tier refill preference | SCH-DEEP-01 | ≥80% of post-completion dispatches pick the same tier while it still has pending work |
| SCH-SD-04 | Work-stealing on drain | SCH-DEEP-01 | LARGE (16) & MEDIUM (16) drain long before SMALL (200)/TINY (400)/ZERO (400); freed slots absorbed by remaining active tiers; `sum(in-flight) == max_workers` while any work remains; new ratio reached within 3 scheduling cycles |
| SCH-SD-05 | Large-first no-starvation | SCH-DEEP-02 | Bandwidth tiers created first, yet TINY/ZERO still receive their reserved slots (not starved) under the 100 GbE profile |
| SCH-SD-06 | Tiny-first request-race (WAN-like) | SCH-DEEP-03 | Huge ZERO/TINY request backlog drains at request-rate while MEDIUM/LARGE trickle; no idle slot while work exists anywhere |
| SCH-SD-07 | Profile switch = scheduling only | SCH-ORD-01 under 2 profiles | Slot distribution differs per profile (tiny-favoured on low-bandwidth, large-favoured on 100 GbE); **batch-file hashes byte-identical** across both runs |

**Convergence:** after any tier drains, the distribution reaches the new ratio within 3 scheduling
cycles.

---

## Group C — Configuration & Robustness (P0)

| ID | Setting | Value | Dataset | Pass when |
|---|---|---|---|---|
| SCH-CF-01 | `NETWORK_PROFILE` | `dt2_100gbe` | SCH-DEEP-01 | Scheduler uses profile weights; confirmed by slot sampling (SCH-SD-01) |
| SCH-CF-02 | `PARALLEL_WORKERS` | `1` | SCH-DEEP-01 | Never more than 1 concurrent cloudcp |
| SCH-CF-03 | `PARALLEL_WORKERS` | `32` | SCH-DEEP-01 | Up to 32 concurrent cloudcp; caps still respected |
| SCH-CF-04 | `PARALLEL_WORKERS` | `0` | any | Refuses to start; clear error |
| SCH-CF-05 | `BATCH_BUILDER_ONLY` | `true` | SCH-ORD-01 | Batches built to `pending/`; no cloudcp spawned; pending set matches the Group A oracle |

---

## Group E — Pause / Resume (Kill & Restart) (P0/P1)

The scheduler is a long-lived **broker** that spawns one **enumerator** plus up to
`max_workers` `aws_transfer.py` workers, each wrapping a single `cloudcp` invocation for one
batch. A **pause** is a hard stop of those processes; a **resume** is a re-run with the **same
`transfer-id` and `transfer-dir`**. Resume must never re-upload work that already finished.

**Batch-state model (the resume source of truth).** Each batch moves atomically
(`os.rename`, tier preserved) through
`transfer_<id>/batches/{pending → inprogress → completed}/<tier>/`. On resume `run()` calls
`_reset_stale_inprogress()` **before** dispatching anything:

- a batch in `inprogress/` **with** a cloudcp retry `.lst` on disk → cloudcp finished the bulk
  and the fallback owns the failed subset → **left inprogress** (log: `left N for the fallback`);
- a batch in `inprogress/` **without** a `.lst` (worker killed before flush) → **requeued** to
  `pending/` for re-dispatch (log: `reset N stale inprogress`);
- a batch already in `completed/` → `claim()` returns `None` → **never re-dispatched** (dedup).

A re-dispatched `cloudcp` uses its `SKIP_EXISTING` probe to skip objects already in the bucket,
so no object is uploaded twice. Separately, `cloudcp` appends one
`[Batch][pid] done records=N ok=N failed=N csv=…/cloud_transfer_<id>/…` line per completed batch
to `cloudcp.log`; the harness sums `ok=N` for `cloud_transfer_<id>/` to measure *how much was
already done* — `pr_baseline_ok` (before the final resume) and `pr_final_ok` (after).

> **Resume oracle (all E.1 cases).** PASS requires: final results CSV `total == expected`
> backlog (§8.2); **every object uploaded exactly once** (checked from the per-file CSV / bucket
> listing); `completed/` batches from the killed run are **not** re-dispatched; and the resumed
> broker exits `0`.

### E.1 Positive — resume correctness (P0)

Run on real `SCH-*` datasets via `schedular_test.py --pause-resume`. Kill timing is
`--pr-kill-after SEC` (per-case defaults below); cycles = number of kill→resume rounds.

| ID | Case | Dataset | Kill after / cycles | Pass when |
|---|---|---|---|---|
| SCH-PR-01 | Broker SIGKILL, single resume | SCH-DEEP-01 | 5 s / 1 | Kill the broker mid-run, re-run same id+dir → resume oracle holds; `pr_final_ok == expected` |
| SCH-PR-02 | Kill mid-multipart (LARGE/MEDIUM) | SCH-DEEP-02 | 8 s / 1 | Kill while a bandwidth-tier multipart batch is inflight; resume re-runs it, `SKIP_EXISTING` skips finished objects, no duplicate keys, report complete |
| SCH-PR-03 | Kill very early (baseline ≈ 0) | SCH-DEEP-01 | 2 s / 1 | Kill before any batch completes (`pr_baseline_ok ≈ 0`); resume uploads the full backlog; no crash from an empty/short log |
| SCH-PR-04 | Kill late (tail only) | SCH-ORD-01 | ~80 % done / 1 | Resume dispatches only remaining `pending`/`inprogress`; `completed/` batches untouched (`claim()`→None); final complete |
| SCH-PR-05 | Double kill / two resume cycles | SCH-DEEP-01 | 5 s ×2 / 2 | Kill, resume, kill again, resume to completion; state accumulates additively across cycles; no duplicates; final complete |
| SCH-PR-06 | Worker crash, broker survives | SCH-DEEP-01 | kill 1 worker / 1 | SIGKILL a single `aws_transfer.py` worker (not the broker); `_reap` re-dispatches that batch (≤ `MAX_CRASH_RETRIES`=3); batch completes; no data loss |
| SCH-PR-07 | `.lst`-backed inprogress kept for fallback | SCH-ORD-01 | after `.lst` flush / 1 | Resume reconcile **leaves** the inprogress batch (`left N for the fallback`); the fallback completes its failed subset; the batch is **not** re-sent to cloudcp |
| SCH-PR-08 | Stale inprogress (no `.lst`) requeued | SCH-ORD-01 | before `.lst` flush / 1 | Resume reconcile **requeues** inprogress→pending (`reset N stale inprogress`); re-dispatched; `SKIP_EXISTING` avoids duplicates; final complete |
| SCH-PR-09 | Graceful SIGINT pause + resume | SCH-DEEP-01 | 5 s / 1 | SIGINT the broker (clean shutdown, partial CSV parses), then re-run same id+dir → resume completes. Superset of NEG-LIFE-01 |

### E.2 Negative — resume fault handling (P1)

Scheduler-level fault injection in a **private throwaway sandbox** (tiny synthesised data; no host
config/creds/real batchmeta touched), driven by `schedular_negative_test.py`. A controlled,
correctly-handled failure is a PASS.

| ID | Injected fault | Pass when |
|---|---|---|
| SCH-PR-NEG-01 | Resume with a **different** transfer-id/dir | No resume occurs (state not shared); harness observes a full re-upload — proves resume is keyed on `id`+`transfer-dir`. Documented behaviour, not data loss |
| SCH-PR-NEG-02 | Resume after `transfer_<id>` deleted | No state to resume; a clean full run to completion, no crash |
| SCH-PR-NEG-03 | Corrupt batch-state `manifest.json` before resume | Resume reconciles/repairs from the state dirs (`pending`/`inprogress`/`completed`) — dedup does **not** depend on parsing a single artifact; run still completes with no duplicate keys. (The host `cloudcp.log` is shared and is intentionally never mutated by the sandbox.) |
| SCH-PR-NEG-04 | Read-only transfer-dir on resume (`chmod 0500`) | The reconcile requeue write fails fast with a clear non-zero error; no hang |
| SCH-PR-NEG-05 | Kill again **during** resume reconcile | SIGKILL the resume broker inside `_reset_stale_inprogress`; a third resume still reconciles correctly (idempotent rename); no batch lost or duplicated |
| SCH-PR-NEG-06 | Concurrent resume (two brokers, same id) | Atomic `claim()` (pending→inprogress rename) ensures no batch is dispatched twice; if unsupported the controlled failure is documented |
| SCH-PR-NEG-07 | Orphan inprogress, object already uploaded | Kill after objects uploaded but before batch→`completed` and before `.lst`; resume requeues to pending, `SKIP_EXISTING` skips the already-uploaded objects → no duplicates (the core "log says how much is done" path) |

**CLI**

```
python schedular_test.py --pause-resume                 # E.1 on the default PR datasets
python schedular_test.py --pause-resume-case SCH-PR-04  # one positive case (comma-separated ok)
python schedular_test.py --pause-resume --pr-kill-after 10   # override kill timing (all cases)
python schedular_test.py --pause-resume-list            # list the positive PR cases
python schedular_test.py --negative-case SCH-PR-NEG-07  # one negative case (via main harness)
python schedular_negative_test.py --case SCH-PR-NEG-01,SCH-PR-NEG-02   # standalone negatives
python schedular_test.py --negative                     # full negative suite incl. E.2
```

> POSIX-only faults (SIGINT/SIGKILL/chmod) auto-**SKIP** on non-POSIX hosts and under
> `--dry-run`. E.1 supersedes the minimal lifecycle checks NEG-LIFE-01/02, which remain as quick
> smoke coverage.

---

## Group D — Negative / Fault Injection (P1)

Scheduler-level fault injection, driven by
[schedular_negative_test.py](schedular_negative_test.py) (`schedular_test.py --negative`). Every
case runs in a **private throwaway sandbox** (`neg_<id>/{data,batchmeta,logs}`) with tiny
synthesised data — **no machine config, creds, or the real `/opt` batchmeta are touched**. A PASS
means the fault was correctly produced *and* the scheduler handled it as specified (a controlled
failure is a pass). Transport / auth / config-loading faults (bad endpoint, bad creds, network
drop, missing/malformed config, xattr) intentionally live in the CLI / binary suites, not here.

| ID | Group | Injected fault | Pass when |
|---|---|---|---|
| NEG-ENUM-01 | enumeration | Source root absent | Exits non-zero cleanly; no orphan `transfer_<id>` |
| NEG-ENUM-02 | enumeration | Empty root + empty level dir | 0 batches, clean completion (`csv_total == 0`) |
| NEG-ENUM-03 | enumeration | One file `chmod 000` | That file `FAILED`; the tier's other files `SUCCESS` |
| NEG-ENUM-04 | enumeration | A level dir `chmod 000` | Level skipped; no crash/hang |
| NEG-ENUM-05 | enumeration | Files unlinked mid-walk (timing) | Vanished entries tolerated; no crash |
| NEG-ENUM-06 | enumeration | Cyclic symlinks | BFS terminates (no infinite walk) |
| NEG-ENUM-07 | enumeration | Dangling symlink | Skipped; enumeration order preserved |
| NEG-BATCH-01 | batch | One file > tier `TARGET_SIZE_MB` (sparse) | Closes a single-file size-capped batch; that file `SUCCESS` |
| NEG-BATCH-03 | batch | Fewer files than one block | Partial batch flushed at `finish()`; all `SUCCESS` |
| NEG-META-01 | batchmeta | Transfer-dir `chmod 0500` | Metadata write fails fast with a clear non-zero error |
| NEG-META-02 | batchmeta | Malformed batchmeta pre-seeded | Reject or repair; no crash |
| NEG-META-03 | batchmeta | Stale `transfer_<id>` present | Safe bump or clean failure |
| NEG-SCHED-01 | scheduling | `--poll-interval 0` | No tight-loop / div-by-zero; run completes within bound |
| NEG-SCHED-02 | scheduling | One huge stalled batch (best-effort) | Free-worker accounting holds; other tiers progress |
| NEG-LIFE-01 | lifecycle | SIGINT mid-run | Clean shutdown; capture drained; partial CSV parses |
| NEG-LIFE-02 | lifecycle | Kill then re-run same id + transfer-dir | Idempotent resume, no duplicate uploads (needs scheduler resume support) |

> POSIX-only faults (chmod / symlink / signals) auto-**SKIP** on non-POSIX hosts and under
> `--dry-run`. `NEG-META-02` and `NEG-LIFE-02` depend on scheduler-side reject/repair and resume
> support respectively; their expectations are encoded but only pass if the scheduler implements
> them.

---

## Traceability

| Requirement (design doc §9) | Test cases |
|---|---|
| §9.1 Enumeration order deterministic | SCH-EN-01, SCH-EN-02 |
| §9.2 Batch shapes/counts deterministic | SCH-BA-01, SCH-BA-02, SCH-BA-04 |
| §9.3 finish()-flush caveat | SCH-BA-03 |
| §9.4 Scheduler dispatch under test | SCH-SD-01 … SCH-SD-07 |
| R1 homogeneous-dir invariance | SCH-EN-03 |
| Pause / resume (kill & restart) | SCH-PR-01 … SCH-PR-09, SCH-PR-NEG-01 … SCH-PR-NEG-07, NEG-LIFE-01, NEG-LIFE-02 |
| Robustness / fault handling | SCH-CF-04, NEG-ENUM-*, NEG-BATCH-*, NEG-META-*, NEG-SCHED-*, NEG-LIFE-* |

## Notes

- All `SCH-BA-*` byte assertions assume the tier's file size lands **inside** its bucket and the
  **file-count** limit closes every full batch (design doc §2.1). If a build reports byte-capped
  closure instead, the config under test differs from the oracle constants above — reconcile before
  running Group B.
- Group A is throughput-agnostic: run MEDIUM/LARGE as `sparse` (manifest default) to keep disk
  footprint near zero. For real throughput measurement (P2), regenerate with
  `generate_dataset.py … --content random`.
- Group D (negative) never mutates the machine: it builds and tears down its own sandbox per case,
  restores permissions before cleanup, and injects faults only via the sandbox filesystem, CLI
  overrides, child-process env, and signals — **no config, creds, or services on the host are
  changed**. Run it with `python schedular_test.py --negative` (`--negative-list` to enumerate,
  `--negative-case <ID>` for one).
