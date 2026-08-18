# Bryck Remote BatchBuilder Validation - Technical Guide

Full reference for the validation framework. For the quick-start commands see README.md.

---

## Table of Contents

1. [How It Works](#1-how-it-works)
2. [Configuration](#2-configuration)
3. [Running the Suite](#3-running-the-suite)
4. [Batch Parameter Override Testing](#4-batch-parameter-override-testing)
5. [Monitoring Unattended Runs](#5-monitoring-unattended-runs)
6. [Generating and Viewing Reports](#6-generating-and-viewing-reports)
7. [Report Structure](#7-report-structure)
8. [Cleanup](#8-cleanup)
9. [Troubleshooting](#9-troubleshooting)
10. [Framework Internals](#10-framework-internals)

---

## 1. How It Works

```
This machine (Windows)
        |
        | SSH (paramiko)
        v
Source host 192.168.0.71
  -- list dataset .yaml files
  -- download selected dataset YAML
        |
        | SSH + SFTP
        v
Dest host 192.168.6.35
  -- clean output/ directory (before run)
  -- upload dataset YAML
  -- convert YAML to CSV (grep strips comments)
  [if --override: update /etc/bryck/bryckcloud/config.json]
  -- run BatchBuilder.py (with CPU/temp telemetry)
  [if --override: restore /etc/bryck/bryckcloud/config.json]
  -- clean dataset + CSV from remote (after run)
        |
        | SFTP download
        v
This machine
  -- validate batch_summary.csv against expected values
  [if --override: validate per-batch constraints against configured limits]
  -- write JSON, HTML, Markdown reports
  -- clean local temp YAML copy
```

For each dataset run the framework collects:
- Pass/fail status and validation issues
- Elapsed time, CPU%, peak memory, throughput metrics
- CPU and NVMe drive temperatures
- Disk usage on destination host

---

## 2. Configuration

**`config/framework_config.json`** — all defaults. CLI flags override config values.

```jsonc
{
  "hosts": {
    "source_host": "192.168.0.71",
    "dest_host":   "192.168.6.35",
    "username":    "bryck",
    "password":    ""   // use BRYCK_SSH_PASSWORD env var instead
  },
  "paths": {
    "source_dir":          "~/dataset-spec-files",
    "dest_dir":            "/opt/bryck/.venv/bryck/lib/python3.10/site-packages/bryckcloud/lib/cloud",
    "dest_csv":            "bryck_file_list.csv",
    "output_dir":          "output",
    "batchbuilder_python": "/opt/bryck/.venv/bryck/bin/python3"
  },
  "runtime": {
    "cleanup_output":             true,   // delete output/ on dest before each run
    "cleanup_remote":             true,   // delete dataset+CSV from dest after each run
    "temp_sample_interval":       0.5,
    "remote_command_timeout_sec": 1800,
    "per_dataset_timeout_sec":    7200,
    "dataset_pattern":            "*.yaml"
  },
  "validation": {
    "overflow_tier": "large",
    "tiers": [
      { "name": "zero",   "max_bytes": 1          },
      { "name": "tiny",   "max_bytes": 1048576     },
      { "name": "small",  "max_bytes": 67108864    },
      { "name": "medium", "max_bytes": 1073741824  }
    ]
  }
}
```

Pass the SSH password via environment variable - do not store it in the config file:
```powershell
$env:BRYCK_SSH_PASSWORD = 'while(1);'
```

---

## 3. Running the Suite

### VS Code terminal (monitored)

```powershell
$env:BRYCK_SSH_PASSWORD = 'while(1);'

# Standard run
python remote_batchbuilder_validation/run_dataset_suite.py

# With batch parameter overrides
python remote_batchbuilder_validation/run_dataset_suite.py --override
```

The terminal shows live progress:
```
[1/12] Running dataset: spec_10gb.yaml
[1/12] Completed dataset: spec_10gb.yaml  status=PASSED
[2/12] Running dataset: spec_200gb.yaml
...
Suite directory: ...\artifacts\suite_20260818_120003
HTML report:     ...\artifacts\suite_20260818_120003\shareable_report.html
```

### Unattended Scheduled Task

```powershell
cd remote_batchbuilder_validation

# Standard run (close terminal / disconnect after triggering)
.\start_unattended_suite.ps1 -Password 'while(1);'

# With batch parameter overrides
.\start_unattended_suite.ps1 -Password 'while(1);' -Override

# Preview without running
.\start_unattended_suite.ps1 -Password 'while(1);' -DryRun
```

What happens after you trigger:
1. A Windows Scheduled Task named `BryckBatchBuilder_<timestamp>` is registered
2. It starts within 5 seconds
3. All output is written to `artifacts/unattended/run_<timestamp>/stdout.log`
4. When finished, `done.marker` is written and the task self-deletes
5. Reports are in `artifacts/suite_<timestamp>/`

---

## 4. Batch Parameter Override Testing

### The override file

Edit `config/batch_param_overrides.json`:

```json
{
  "remote_config_path": "/etc/bryck/bryckcloud/config.json",
  "use_sudo": false,
  "tiers": {
    "ZERO":   { "BATCH_SIZE": 8000, "TARGET_SIZE_MB": 0,     "OPEN_BATCHES": 3 },
    "TINY":   { "BATCH_SIZE": 1000, "TARGET_SIZE_MB": 256,   "OPEN_BATCHES": 3 },
    "SMALL":  { "BATCH_SIZE": 8192, "TARGET_SIZE_MB": 16384, "OPEN_BATCHES": 2 },
    "MEDIUM": { "BATCH_SIZE": 1,    "TARGET_SIZE_MB": 4096,  "OPEN_BATCHES": 2 },
    "LARGE":  { "BATCH_SIZE": 1,    "TARGET_SIZE_MB": 16384, "OPEN_BATCHES": 1 }
  }
}
```

You only need to include the tiers and parameters you want to change.

| Parameter | Meaning |
|---|---|
| `BATCH_SIZE` | Max files per batch (0 = unlimited) |
| `TARGET_SIZE_MB` | Max MB per batch (0 = no size limit) |
| `OPEN_BATCHES` | Max concurrent open batches |

> `use_sudo: true` if the `bryck` SSH user needs elevated permissions to write to the config path.

### Execution flow with `--override`

1. Read `config/batch_param_overrides.json`
2. SSH to dest host — read original `/etc/bryck/bryckcloud/config.json`
3. Apply your overrides (only the keys you specified)
4. Run BatchBuilder
5. Download `batch_summary.csv`
6. **Restore original config** (guaranteed, even on failure)
7. Validate each batch row against `BATCH_SIZE` and `TARGET_SIZE_MB`
8. Report violations under Issues in HTML/JSON

**The original config is always restored.** Even if BatchBuilder crashes, the SSH drops, or the run times out, the `finally` block restores the config before closing the SSH connection.

### Validation logic

- `BATCH_SIZE > 0`: each batch's `file_count` must be <= limit
- `TARGET_SIZE_MB > 0`: each batch's `total_size_MB` must be <= limit * 1.10 (10% tolerance)
- `OPEN_BATCHES`: concurrency setting, not validatable from the static CSV

If the CSV is in legacy aggregated format (no per-batch rows), per-batch validation is skipped and noted in the report.

### Using a custom override file

```powershell
python remote_batchbuilder_validation/run_dataset_suite.py `
    --batch-override-file remote_batchbuilder_validation\config\my_custom_overrides.json
```

---

## 5. Monitoring Unattended Runs

### Check status and progress

```powershell
cd remote_batchbuilder_validation

# Check most recent run (shows last 25 log lines)
.\check_unattended_suite.ps1

# Show 50 log lines
.\check_unattended_suite.ps1 -TailLines 50

# Open the HTML report when done
.\check_unattended_suite.ps1 -OpenReport
```

Sample output (in progress):
```
======  BatchBuilder Suite Status  ======
  Started  : 2026-08-18T12:00:00
  Elapsed  : 01:23:45
  Running  : True
  Finished : False

STATUS:  IN PROGRESS
  7 datasets completed   (~380s each so far)
  Suite dir: ...\artifacts\suite_20260818_120003
```

Sample output (finished):
```
======  BatchBuilder Suite Status  ======
  Running  : False
  Finished : True
  Exit code: 0

RESULT:  PASSED=11  FAILED=1  TOTAL=12

  HTML report: ...\artifacts\suite_20260818_120003\shareable_report.html
```

### Live log tail

```powershell
# Watch output in real time (Ctrl+C to stop)
Get-Content "remote_batchbuilder_validation\artifacts\unattended\run_<timestamp>\stdout.log" -Wait -Tail 30

# Auto-find the most recent log
$log = (Get-ChildItem remote_batchbuilder_validation\artifacts\unattended -Recurse -Filter stdout.log |
        Sort-Object LastWriteTime -Descending | Select-Object -First 1).FullName
Get-Content $log -Wait -Tail 30
```

### How to know it has finished

Any of these indicate the run is complete:
1. `done.marker` file appears in `artifacts/unattended/run_<timestamp>/`
2. `suite_summary.json` appears in `artifacts/suite_<timestamp>/`
3. `check_unattended_suite.ps1` prints `Finished : True`

---

## 6. Generating and Viewing Reports

Reports are written automatically at the end of every run. No extra steps needed.

To regenerate or open reports from existing artifacts (no SSH required):

```powershell
# Open most recent completed suite in browser
python remote_batchbuilder_validation/fetch_results.py --open

# List all past suite runs
python remote_batchbuilder_validation/fetch_results.py --list

# Regenerate from a specific suite directory
python remote_batchbuilder_validation/fetch_results.py `
    --suite-dir remote_batchbuilder_validation\artifacts\suite_20260818_120003 `
    --open

# Write HTML to a custom path
python remote_batchbuilder_validation/fetch_results.py `
    --out-html C:\Reports\batchbuilder.html --open
```

`fetch_results.py` reads the saved JSON files and re-renders using the same
rendering pipeline as the live run. Useful if you want to re-render after
copying artifacts from another machine, or after a partial run.

---

## 7. Report Structure

```
artifacts/
  suite_<timestamp>/
    shareable_report.html     # main dashboard — pass rate, metrics, charts
    shareable_bundle.zip      # full suite zipped for sharing
    suite_summary.json        # machine-readable summary
    structured_reports/
      dataset_metrics.json    # full metrics per dataset
      variation_analysis.json # min/avg/max across all datasets
      issues.json             # datasets with failures
      performance_flags.json  # datasets that exceeded baseline thresholds
    runs/
      run_<timestamp>/        # one directory per dataset
        validation_report.html
        validation_report.json
        validation_report.md
        batch_summary.csv
        execution.log         # raw SSH command log

artifacts/unattended/
  run_<timestamp>/
    run_meta.json     # task name, log paths, start time
    stdout.log        # all suite output
    done.marker       # written when run finishes (contains exit code)
    task_wrapper.ps1  # the PS1 the Scheduled Task executed

artifacts/performance_baselines.json  # rolling per-dataset perf history
```

---

## 8. Cleanup

### Automatic (always on)

Before each dataset run:
- Remote `output/` directory is deleted (`cleanup_output: true` in config)
- Old dataset YAML and CSV are removed from dest (`preclean` step)

After each dataset run:
- Dataset YAML and CSV are removed from dest (`cleanup_remote: true`)
- Local copy of the downloaded dataset YAML is deleted

### Manual cleanup of old local artifacts

The `artifacts/` directory grows over time. To remove old suite runs:

```powershell
# Remove suite runs older than 30 days
Get-ChildItem "remote_batchbuilder_validation\artifacts" -Directory -Filter "suite_*" |
    Where-Object { $_.LastWriteTime -lt (Get-Date).AddDays(-30) } |
    Remove-Item -Recurse -Force

# Remove old unattended run metadata
Get-ChildItem "remote_batchbuilder_validation\artifacts\unattended" -Directory |
    Where-Object { $_.LastWriteTime -lt (Get-Date).AddDays(-30) } |
    Remove-Item -Recurse -Force
```

---

## 9. Troubleshooting

| Problem | Fix |
|---|---|
| `Python not found` | Create venv: `python -m venv .venv && .\.venv\Scripts\pip install -r requirements.txt` |
| `Password required` | Set `$env:BRYCK_SSH_PASSWORD = '...'` or pass `-Password` |
| `No datasets found on source host` | Check `hosts.source_host` and `paths.source_dir` in config |
| `Scheduled task does not start` | Run `Get-ScheduledTask` to check state; check `Get-ScheduledTaskInfo -TaskName BryckBatchBuilder_...` |
| `Could not find batch summary` | BatchBuilder failed; check `execution.log` in the run artifacts |
| `Override restore failed` | SSH connection dropped; manually restore `/etc/bryck/bryckcloud/config.json` on dest host |
| `use_sudo: true needed` | bryck user lacks write permission to `/etc/bryck/`; set `"use_sudo": true` in override file |

---

## 10. Framework Internals

### Directory layout

```
remote_batchbuilder_validation/
  run_dataset_suite.py              # suite orchestrator
  run_remote_batchbuilder_validation.py  # single-dataset runner
  fetch_results.py                  # re-render HTML from saved JSON
  start_unattended_suite.ps1        # launch as Scheduled Task
  check_unattended_suite.ps1        # monitor + show progress
  functions/
    config_loader.py                # JSON config + deep merge
    dataset_discovery.py            # SSH dataset listing
    remote_ops.py                   # SSH/SFTP + remote config management
    telemetry_hooks.py              # GNU time + temperature parsing
  assertions/
    batchbuilder_assertions.py      # tier validation + batch param checks
  reports/
    report_lib.py                   # re-exports from reporting.py
  reporting.py                      # HTML/JSON rendering
  config/
    framework_config.json           # all defaults
    batch_param_overrides.json      # batch parameter override template
```

### Validation logic

For each dataset the expected output is computed from the YAML file itself:
every `size,path` record is classified into a tier bucket, file counts and
byte totals are accumulated. The actual `batch_summary.csv` from BatchBuilder
is compared against those expected values. No values are hardcoded.

### Remote config management (override mode)

`functions/remote_ops.py` provides four functions:
- `read_remote_json_config` — SSH `cat` + JSON parse
- `write_remote_json_config` — SFTP to temp file, then `cp` (atomic)
- `apply_remote_batch_overrides` — deep-merge tiers, write, return original
- `restore_remote_config` — write original back (called in `finally`)
