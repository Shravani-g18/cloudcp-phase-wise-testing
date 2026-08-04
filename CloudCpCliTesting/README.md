# CloudCP CLI Testing Package

**Phase:** CLI (Command-Line Interface) functional and configuration testing
**Priority:** P0 (correctness) / P2 (performance)
**Status:** Active — grounded in `tsecb/bryck` config, `tsecb/cloud` scheduler, and this
repo's datasets/specs.

---

## Overview

The CloudCP transfer system is controlled through a combination of:

1. **`/etc/bryck/bryckcloud/config.json`** — the primary runtime configuration file that
   governs all transfer behaviour (parallelism, batch sizing, multipart thresholds, etc.).
2. **`batch_scheduler.py`** — the broker/scheduler that dispatches batches to `cloudcp`
   workers according to the configured network profile and tier weights.
3. **`/opt/bryck/aws/bin/cloudcp`** — the C++ uploader binary; invoked by the broker
   per batch, never directly by an end-user CLI in production.

**CLI Testing** validates that configuration changes applied via the config file and
broker CLI flags produce the expected runtime behaviour: correct batch dispatch, correct
`cloudcp` invocation arguments, correct multipart thresholds, correct skip/resume
behaviour, and correct transfer report output.

This package does **not** duplicate the binary-level tests in
[`CloudCpBinaryTesting/`](../CloudCpBinaryTesting/) — it focuses on the
*operator-visible layer*: starting a transfer, observing its progress, reading its report,
and verifying that config knobs have the stated effect.

---

## Scope

| In scope | Out of scope |
|---|---|
| Broker/scheduler invocation and flag handling | C++ cloudcp internals (covered by binary testing) |
| Config-file knob verification (CHUNK_SIZE_MB, SKIP_EXISTING, PARALLEL_WORKERS, …) | AWS SDK performance profiling |
| Transfer start/stop/resume (SKIP_EXISTING, batch rerun) | UI and API layers (see phases 07 & 08) |
| Transfer report validation (CSV/JSON structure and field correctness) | PostgreSQL schema migrations |
| Multipart threshold boundary (64 MB) at the CLI/config level | Negative/hostile batch framing (covered by binary testing) |
| Encoding round-trip via CLI (unicode, special chars) | |
| Network profile selection | |
| Smoke selection (fast confidence gate) | |

---

## Repository Layout

```
CloudCpCliTesting/
├── README.md              ← this file
├── cli_test_plan.md       ← full test-case catalogue with dataset mapping
├── references.md          ← source provenance (this repo / tsecb/bryck / tsecb/cloud)
├── cli_config.py          ← shared paths, defaults, environment helpers
├── cli_cases.py           ← case catalogue, tags, dataset mapping, skip logic
├── run_cli_tests.py       ← main orchestrator (runner)
└── scripts/
    ├── report_validator.py  ← transfer-report CSV/JSON validation helper
    └── dataset_prep.py      ← dataset selection and mixed-workload preparation guide
```

---

## Quick Start

> All commands assume the **bryck host** with the full stack deployed.
> Use `--dry-run` anywhere to print commands without executing them.

### 1. List available CLI test cases

```bash
python3 run_cli_tests.py --list
```

### 2. Run the smoke suite (fastest confidence gate, ~300 GB mixed dataset)

```bash
python3 run_cli_tests.py --tag smoke
```

### 3. Dry-run a single case

```bash
python3 run_cli_tests.py --case CLI-SMOKE-01 --dry-run
```

### 4. Run all cases in a group

```bash
python3 run_cli_tests.py --tag config
python3 run_cli_tests.py --tag encoding
python3 run_cli_tests.py --tag boundary
```

### 5. Run the full CLI suite

```bash
python3 run_cli_tests.py --all
```

### 6. Validate a transfer report from a previous run

```bash
python3 scripts/report_validator.py \
    --csv /opt/bryck/bryckapi/downloads/cloud_transfer_logs/cloud_transfer_42/transfer_report_42.csv \
    --expected-count 91320
```

### 7. See dataset options for a mixed workload

```bash
python3 scripts/dataset_prep.py --suggest mixed --total-gb 4
```

---

## Execution Model

```
run_cli_tests.py
   │
   ├─ load cli_config.py         → environment paths, config.json location
   ├─ load cli_cases.py          → case catalogue and dataset mapping
   │
   ├─ [per case]
   │     ├─ dataset_prep.py      → choose/verify dataset on disk
   │     ├─ patch config.json    → apply per-case config overrides
   │     ├─ invoke broker        → python3 batch_scheduler.py ...
   │     ├─ wait for completion
   │     ├─ report_validator.py  → assert CSV structure and field values
   │     └─ restore config.json  → revert overrides
   │
   └─ emit run_report.md / run_report.json
```

The runner is intentionally **side-effect-free** until a case executes — `--list` and
`--dry-run` never touch config.json or invoke the broker.

---

## Relationship to Other Phases

| Phase | Folder | Relationship |
|---|---|---|
| Binary testing | `CloudCpBinaryTesting/` | CLI tests call the broker which calls `cloudcp`; binary tests call `cloudcp` directly. CLI tests confirm the broker plumbs the right flags. |
| Scheduler testing | `CloudCpSchedulerTesting/` | Scheduler tests focus on batch-dispatch ordering; CLI tests focus on the operator-visible surface and config knobs. |
| Report testing | `CloudCpReportTesting/` | `scripts/report_validator.py` is a shared utility; report tests go deeper on reconciliation edge cases. |
| Complete functional | `CloudCpCompleteFunctional/` | Full end-to-end; CLI tests are a subset. |

---

## Related Repositories

| Repo | Role |
|---|---|
| `tsecb/bryck` | Contains `config.json`, `batch_scheduler.py`, and the `cloudcp` binary. All CLI invocations tested here target those paths. |
| `tsecb/cloud` | Contains `bryckcloud` Python package (`batch_scheduler.py`, broker, verification engine). |
| This repo | Test datasets, spec files, dataset map, design docs, test plans. |

Full provenance documented in [`references.md`](references.md).

---

## Config File Location

```
/etc/bryck/bryckcloud/config.json
```

Baseline documented in [`../docs/config_reference.md`](../docs/config_reference.md).
The config used for all CLI tests is reproduced in [`cli_config.py`](cli_config.py).
