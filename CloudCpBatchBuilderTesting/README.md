# CloudCp BatchBuilder Validation

Automated end-to-end validation of Bryck's `BatchBuilder.py` across two remote hosts.
Part of the **cloudcp-phase-wise-testing** suite.

---

## What it does

For every dataset the framework:

1. Discovers `.yaml` dataset spec files on the **source host** (`192.168.0.71`)
2. Downloads the spec to this machine
3. Uploads it to the **destination host** (`192.168.6.35`)
4. Runs `BatchBuilder.py` on the destination host with telemetry (CPU, memory, temperature)
5. Downloads the resulting `batch_summary.csv`
6. Validates output against expected tier-bucketed totals
7. Cleans up remote temp files before and after each run (automatic)
8. Generates HTML + JSON reports locally in `artifacts/`

When `--override` is used, the BATCH parameters in `/etc/bryck/bryckcloud/config.json`
on the destination host are updated before each run and **automatically restored** after,
even if BatchBuilder crashes.

---

## Setup (one time)

```powershell
cd CloudCpBatchBuilderTesting

python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

---

## Two ways to run

### Option 1 — VS Code terminal (live output)

```powershell
cd CloudCpBatchBuilderTesting

$env:BRYCK_SSH_PASSWORD = 'while(1);'

# Standard run (all datasets, existing config)
python run_dataset_suite.py

# With batch parameter overrides
python run_dataset_suite.py --override
```

### Option 2 — Unattended Scheduled Task

Registers as a Windows Scheduled Task and keeps running even after you close VS Code,
disconnect VPN, or log off Remote Desktop.

```powershell
cd CloudCpBatchBuilderTesting

# Standard run
.\start_unattended_suite.ps1 -Password 'while(1);'

# With batch parameter overrides
.\start_unattended_suite.ps1 -Password 'while(1);' -Override

# Preview without starting
.\start_unattended_suite.ps1 -Password 'while(1);' -DryRun
```

After triggering you can close everything. The run finishes on this machine and
writes reports to `artifacts/suite_<timestamp>/`.

---

## Monitor an unattended run

```powershell
cd CloudCpBatchBuilderTesting

# Check status + see last 25 lines of log
.\check_unattended_suite.ps1

# Watch logs live  (Ctrl+C to stop)
$log = (Get-ChildItem artifacts\unattended -Recurse -Filter stdout.log |
        Sort-Object LastWriteTime -Descending | Select-Object -First 1).FullName
Get-Content $log -Wait -Tail 30

# Open the HTML report in the browser when done
.\check_unattended_suite.ps1 -OpenReport
```

The run is finished when `check_unattended_suite.ps1` prints `Finished : True`.

---

## Generate / view HTML reports

Reports are written automatically at the end of every run.
To regenerate or open a past report:

```powershell
cd CloudCpBatchBuilderTesting

# Open the most recent suite report in your browser
python fetch_results.py --open

# List all past suite runs
python fetch_results.py --list

# Regenerate from a specific suite
python fetch_results.py --suite-dir artifacts\suite_20260818_120003 --open
```

---

## Batch parameter override testing

Edit `config/batch_param_overrides.json` with the BATCH parameters you want to test,
then run with `--override`.

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

You only need to include the tiers / parameters you want to change.
Everything else on the remote host stays untouched.

The original config is **always restored** after each dataset run — even on failure.

---

## Configuration

`config/framework_config.json` — hosts, paths, timeouts.
Set the SSH password via environment variable — **never store it in the config file**:

```powershell
$env:BRYCK_SSH_PASSWORD = 'while(1);'
```

---

## Project layout

```
CloudCpBatchBuilderTesting/
  run_dataset_suite.py              # suite orchestrator (runs N datasets sequentially)
  run_remote_batchbuilder_validation.py  # single-dataset runner
  fetch_results.py                  # re-render HTML reports from saved JSON
  start_unattended_suite.ps1        # launch as Windows Scheduled Task
  check_unattended_suite.ps1        # monitor progress and show report path
  reporting.py                      # HTML / JSON report rendering
  requirements.txt                  # pip dependencies
  config/
    framework_config.json           # all runtime defaults
    batch_param_overrides.json      # batch parameter override template
  functions/
    config_loader.py                # JSON config + deep merge
    dataset_discovery.py            # SSH-based dataset listing
    remote_ops.py                   # SSH / SFTP + remote config management
    telemetry_hooks.py              # GNU time + temperature metric parsing
  assertions/
    batchbuilder_assertions.py      # tier validation + batch parameter checks
  reports/
    report_lib.py                   # re-exports reporting functions
  examples/
    suite_config.json               # example config for reference
    single_run_config.json          # example single-run config
  tests/
    test_remote_batchbuilder_validation.py   # unit tests
  artifacts/
    performance_baselines.json      # rolling per-dataset performance history (committed)
    suite_<timestamp>/              # generated — one per run  (gitignored)
    unattended/                     # generated — one per trigger (gitignored)
```

See `GUIDE.md` for the full technical reference.
