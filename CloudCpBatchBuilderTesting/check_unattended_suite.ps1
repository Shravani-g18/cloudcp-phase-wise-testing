<#
.SYNOPSIS
    Check whether an unattended BatchBuilder suite run has finished.

.DESCRIPTION
    Reads the most recent run_meta.json from artifacts/unattended/ and tells you:
      - Whether the Scheduled Task is still running
      - How many datasets have completed and the estimated time remaining
      - Where the final HTML report is when the run finishes

.PARAMETER RunMetaPath
    Path to a specific run_meta.json. If omitted, uses the most recent run.

.PARAMETER TailLines
    Show the last N lines of the log file. Default 25. Set 0 to skip.

.PARAMETER OpenReport
    Open the HTML report in the browser when the run is complete.

.EXAMPLE
    # Check the most recent run
    .\check_unattended_suite.ps1

.EXAMPLE
    # Show more log lines
    .\check_unattended_suite.ps1 -TailLines 50

.EXAMPLE
    # Open the report in the browser when done
    .\check_unattended_suite.ps1 -OpenReport
#>
param(
    [string]$RunMetaPath = '',
    [int]$TailLines = 25,
    [switch]$OpenReport
)

$ErrorActionPreference = 'Stop'

$scriptDir      = Split-Path -Parent $MyInvocation.MyCommand.Path
$artifactsRoot  = Join-Path $scriptDir 'artifacts'
$unattendedRoot = Join-Path $artifactsRoot 'unattended'

# Find run_meta.json -------------------------------------------------------
if (-not $RunMetaPath) {
    if (-not (Test-Path $unattendedRoot)) {
        Write-Host 'No unattended runs found.  Start one with: .\start_unattended_suite.ps1 -Password <pwd>'
        return
    }
    $latestDir = Get-ChildItem -Path $unattendedRoot -Directory |
        Sort-Object LastWriteTime -Descending | Select-Object -First 1
    if (-not $latestDir) {
        Write-Host 'No run directories found in artifacts\unattended\'
        return
    }
    $RunMetaPath = Join-Path $latestDir.FullName 'run_meta.json'
}

if (-not (Test-Path $RunMetaPath)) {
    Write-Host "run_meta.json not found: $RunMetaPath"
    return
}

$meta      = Get-Content -Path $RunMetaPath -Raw | ConvertFrom-Json
$startedAt = [datetime]$meta.started_at
$donePath  = $meta.done_marker
$stdoutLog = $meta.stdout_log

# Is it still running? -----------------------------------------------------
$taskName = $meta.task_name
$task = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
$running = $null -ne $task -and $task.State -in @('Running', 'Ready')
$isDone  = Test-Path $donePath

$elapsed = (Get-Date) - $startedAt
$elapsedStr = '{0:hh\:mm\:ss}' -f $elapsed

# Find the matching suite directory ----------------------------------------
$suiteDir = Get-ChildItem -Path $artifactsRoot -Directory |
    Where-Object { $_.Name -like 'suite_*' -and $_.LastWriteTime -ge $startedAt.AddMinutes(-2) } |
    Sort-Object LastWriteTime -Descending | Select-Object -First 1

# Print status header -------------------------------------------------------
Write-Host ''
Write-Host '======  BatchBuilder Suite Status  ======'
Write-Host "  Started  : $($meta.started_at)"
Write-Host "  Elapsed  : $elapsedStr"
Write-Host "  Override : $($meta.override)"
Write-Host "  Running  : $running"
Write-Host "  Finished : $isDone"

if ($isDone) {
    $exitCode = Get-Content -Path $donePath -Raw -ErrorAction SilentlyContinue
    Write-Host "  Exit code: $($exitCode.Trim())"
}

Write-Host ''

# Suite progress or results ------------------------------------------------
if (-not $suiteDir) {
    Write-Host 'Suite directory not yet created (run may be starting up).'
} else {
    $summaryPath = Join-Path $suiteDir.FullName 'suite_summary.json'

    if (Test-Path $summaryPath) {
        # Run is complete.
        $summary = Get-Content -Path $summaryPath -Raw | ConvertFrom-Json
        $passed  = @($summary.results | Where-Object { $_.status -eq 'PASSED' }).Count
        $failed  = @($summary.results | Where-Object { $_.status -ne 'PASSED' }).Count
        $total   = [int]$summary.selected_count

        $htmlReport = Join-Path $suiteDir.FullName 'shareable_report.html'
        $structured = Join-Path $suiteDir.FullName 'structured_reports'

        Write-Host "RESULT:  PASSED=$passed  FAILED=$failed  TOTAL=$total"
        Write-Host ''
        Write-Host "  Suite dir  : $($suiteDir.FullName)"
        Write-Host "  HTML report: $htmlReport"
        Write-Host "  JSON data  : $structured"
        Write-Host ''
        Write-Host 'To open the report:  python fetch_results.py --open'
        Write-Host "Or directly:         Start-Process '$htmlReport'"

        if ($OpenReport -and (Test-Path $htmlReport)) {
            Start-Process $htmlReport
        }
    } else {
        # Still in progress — compute progress.
        $runsRoot       = Join-Path $suiteDir.FullName 'runs'
        $completedRuns  = 0
        if (Test-Path $runsRoot) {
            $completedRuns = (Get-ChildItem $runsRoot -Directory -Filter 'run_*' |
                Where-Object { Test-Path (Join-Path $_.FullName 'validation_report.json') }).Count
        }

        $progressStr = "$completedRuns datasets completed"
        if ($elapsed.TotalSeconds -gt 0 -and $completedRuns -gt 0) {
            $secsPerRun = $elapsed.TotalSeconds / $completedRuns
            $progressStr += "   (~$([int]$secsPerRun)s each so far)"
        }

        Write-Host "STATUS:  IN PROGRESS"
        Write-Host "  $progressStr"
        Write-Host "  Suite dir: $($suiteDir.FullName)"
    }
}

# Log tail ------------------------------------------------------------------
if ($TailLines -gt 0 -and (Test-Path $stdoutLog)) {
    Write-Host ''
    Write-Host "--- Last $TailLines lines of log ---"
    Get-Content -Path $stdoutLog -Tail $TailLines | ForEach-Object { Write-Host "  $_" }
    Write-Host ''
    Write-Host "Full log: $stdoutLog"
    Write-Host "Live tail: Get-Content '$stdoutLog' -Wait -Tail 30"
}
