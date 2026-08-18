<#
.SYNOPSIS
    Trigger a BatchBuilder validation suite as a Windows Scheduled Task.

.DESCRIPTION
    Registers a one-shot Windows Scheduled Task that runs run_dataset_suite.py on
    this machine. The task is owned by the Windows Task Scheduler service, so it
    keeps running even after you close VS Code, disconnect from VPN, or log off
    Remote Desktop.

    What happens automatically on every run:
      1. Datasets are fetched from the source host (192.168.0.71)
      2. BatchBuilder.py runs on the destination host (192.168.6.35)
      3. Remote output directory is cleaned before each dataset run
      4. Remote temp files (dataset + CSV) are cleaned after each dataset run
      5. HTML and JSON reports are written to artifacts/suite_<timestamp>/
      6. The Scheduled Task self-deletes after the run finishes

    When -Override is used:
      - Batch parameters in /etc/bryck/bryckcloud/config.json on the destination
        host are updated from config/batch_param_overrides.json before each run
      - BatchBuilder output is validated against those parameters
      - The original config is ALWAYS restored afterwards, even on failure

.PARAMETER Password
    SSH password for the remote hosts.
    If omitted, the BRYCK_SSH_PASSWORD environment variable is used.

.PARAMETER Override
    Apply batch parameter overrides from config/batch_param_overrides.json.
    Original config is restored automatically after each dataset run.

.PARAMETER DryRun
    Print the command that would run without actually starting anything.

.EXAMPLE
    # Standard run - all datasets, default config
    .\start_unattended_suite.ps1 -Password 'while(1);'

.EXAMPLE
    # Run with batch parameter overrides
    .\start_unattended_suite.ps1 -Password 'while(1);' -Override

.EXAMPLE
    # Preview what would run
    .\start_unattended_suite.ps1 -Password 'while(1);' -DryRun
#>
param(
    [string]$Password = '',
    [switch]$Override,
    [switch]$DryRun
)

$ErrorActionPreference = 'Stop'

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$runner = Join-Path $scriptDir 'run_dataset_suite.py'
$cfgPath = Join-Path $scriptDir 'config\framework_config.json'

# Find python: prefer a venv inside this directory, fall back to system python.
$venvPython = Join-Path $scriptDir '.venv\Scripts\python.exe'
$pythonExe = if (Test-Path $venvPython) { $venvPython } `
    else { (Get-Command python -ErrorAction SilentlyContinue)?.Source }
if (-not $pythonExe) {
    throw 'Python not found. Run setup:  python -m venv .venv  &&  .venv\Scripts\pip install -r requirements.txt'
}

if (-not (Test-Path $runner)) { throw "Runner not found: $runner" }
if (-not (Test-Path $cfgPath)) { throw "Config not found: $cfgPath" }

$effectivePassword = if ($Password) { $Password } else { $env:BRYCK_SSH_PASSWORD }
if (-not $effectivePassword) {
    throw 'Password required. Pass -Password or set $env:BRYCK_SSH_PASSWORD.'
}

# Build Python argument list.
$pyArgs = @($runner, '--config', $cfgPath)
if ($Override) { $pyArgs += '--override' }

if ($DryRun) {
    Write-Host ''
    Write-Host '[DRY RUN]  Would register Scheduled Task to run:'
    Write-Host "  $pythonExe $($pyArgs -join ' ')"
    Write-Host ''
    Write-Host 'No task was registered.'
    return
}

# Create the artifact directory for this unattended run.
$ts = Get-Date -Format 'yyyyMMdd_HHmmss'
$runDir = Join-Path $scriptDir "artifacts\unattended\run_$ts"
New-Item -ItemType Directory -Path $runDir -Force | Out-Null

$stdoutLog = Join-Path $runDir 'stdout.log'
$metaPath = Join-Path $runDir 'run_meta.json'
$donePath = Join-Path $runDir 'done.marker'
$taskName = "BryckBatchBuilder_$ts"
$wrapperPath = Join-Path $runDir 'task_wrapper.ps1'

# Write the wrapper script the Scheduled Task will execute.
$escapedPwd = $effectivePassword -replace "'", "''"
$argFragment = ($pyArgs | ForEach-Object { "'$($_ -replace "'", "''")'" }) -join ' '
Set-Content -Path $wrapperPath -Encoding UTF8 -Value @"
`$env:BRYCK_SSH_PASSWORD = '$escapedPwd'
& '$pythonExe' $argFragment *>> '$stdoutLog'
`$exitCode = `$LASTEXITCODE
"`$exitCode" | Set-Content -Path '$donePath' -Encoding UTF8
Unregister-ScheduledTask -TaskName '$taskName' -Confirm:`$false -ErrorAction SilentlyContinue
"@

# Register the Scheduled Task.
$action = New-ScheduledTaskAction -Execute 'powershell.exe' `
    -Argument "-NonInteractive -WindowStyle Hidden -ExecutionPolicy Bypass -File `"$wrapperPath`""
$trigger = New-ScheduledTaskTrigger -Once -At (Get-Date).AddSeconds(5)
$settings = New-ScheduledTaskSettingsSet `
    -ExecutionTimeLimit (New-TimeSpan -Hours 48) `
    -MultipleInstances IgnoreNew `
    -StartWhenAvailable

Register-ScheduledTask `
    -TaskName $taskName `
    -Action   $action `
    -Trigger  $trigger `
    -Settings $settings `
    -RunLevel Highest `
    -Force | Out-Null

# Write metadata so check_unattended_suite.ps1 can find this run.
[ordered]@{
    mode        = 'scheduled_task'
    task_name   = $taskName
    started_at  = (Get-Date).ToString('o')
    override    = $Override.IsPresent
    stdout_log  = $stdoutLog
    done_marker = $donePath
    run_dir     = $runDir
} | ConvertTo-Json -Depth 3 | Set-Content -Path $metaPath -Encoding UTF8

Write-Host ''
Write-Host "Suite started as Scheduled Task:  $taskName"
Write-Host "  Log file : $stdoutLog"
Write-Host "  Done flag: $donePath  (written when run finishes)"
Write-Host ''
Write-Host 'You can close this terminal or disconnect.  The run continues on this machine.'
Write-Host ''
Write-Host 'Monitor status:        .\check_unattended_suite.ps1'
Write-Host "Tail logs live:        Get-Content '$stdoutLog' -Wait -Tail 30"
Write-Host 'Generate HTML report:  python fetch_results.py --open'
