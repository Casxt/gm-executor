# Run ONCE at first boot from elevated PowerShell.  Edit the three values
# below, paste the whole file into the VM's startup-flow box, and run.
#
# Self-contained — does NOT require the repo to be cloned yet.  Sets AV
# exclusions, machine-scope env vars, writes C:\gm\startup.ps1 inline, and
# registers a logon task that runs it.  The executor itself still expects
# the repo at C:\gm\gm-executor — clone it separately before first logon.

$ErrorActionPreference = "Stop"

# ── Fill these in before pasting ─────────────────────────────────────
$BaseDir     = 'C:\gm'
$GmToken     = '<PUT-GM-TOKEN-HERE>'
$GitRepoUrl  = '<PUT-GMX-GIT-REPO-URL-HERE>'
$StrategyId  = '<PUT-GM-STRATEGY-ID-HERE>'

# Defender scans every file open/close — kills the append-heavy
# order_record.jsonl workload on throttled cloud disks.
Add-MpPreference -ExclusionPath      $BaseDir
Add-MpPreference -ExclusionProcess   "python.exe"
Add-MpPreference -ExclusionExtension "jsonl"

[Environment]::SetEnvironmentVariable("GM_TOKEN",         $GmToken,    "Machine")
[Environment]::SetEnvironmentVariable("GMX_GIT_REPO_URL", $GitRepoUrl, "Machine")
[Environment]::SetEnvironmentVariable("GM_STRATEGY_ID",   $StrategyId, "Machine")

New-Item -ItemType Directory -Path $BaseDir -Force | Out-Null

# Inline so cloud-init has no repo dependency.  Single-quoted here-string —
# nothing inside expands at cloud-init time; everything runs at logon.
# `__BASE_DIR__` is the only sentinel cloud-init substitutes before writing.
$startupScript  = Join-Path $BaseDir "startup.ps1"
$startupContent = @'
# Runs at Administrator logon via the GmExecutorStartup task.  Brings up
# the GM trading client, waits for it to log in, then pops a visible
# terminal running the executor.  The executor handles its own file logging.

$ErrorActionPreference = "Continue"

$base    = "__BASE_DIR__"
$repo    = "$base\gm-executor"
$essence = "C:\Users\Administrator\AppData\Roaming\Essence Goldminer3\essence.exe"

# Transcript captures startup.ps1's own stdout/stderr so we can debug task
# failures — the python executor logs separately into its own files.
Start-Transcript -Path "$base\startup.log" -Append | Out-Null

Start-Process -FilePath $essence -WorkingDirectory (Split-Path $essence)

# essence needs ~a minute to log in and connect to the broker before the
# executor's get_position / get_unfinished_orders return data.
Start-Sleep -Seconds 60

# Guard: -WorkingDirectory throws if $repo doesn't exist.  Hits on first
# boot before the operator clones the repo — exit cleanly so the task
# doesn't show a cryptic DirectoryNotFoundException.
if (-not (Test-Path $repo)) {
    Write-Warning "$repo not found; clone the repo and re-logon."
    Stop-Transcript | Out-Null
    return
}

# New console window so the operator can watch / Ctrl-C it.  Start-Process
# spawns a fresh console even though the parent powershell is hidden.
Start-Process -FilePath "python" -ArgumentList "-m", "file_executor.main" -WorkingDirectory $repo

Stop-Transcript | Out-Null
'@ -replace '__BASE_DIR__', $BaseDir

# UTF-8 no BOM — PowerShell's default Out-File would write UTF-16 LE.
[System.IO.File]::WriteAllText(
    $startupScript,
    $startupContent,
    (New-Object System.Text.UTF8Encoding $false))

# essence.exe is a tray GUI — must run in an interactive session, so trigger
# at logon (not -AtStartup, which runs in session 0 with no UI).
$action    = New-ScheduledTaskAction `
                -Execute "powershell.exe" `
                -Argument "-NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File `"$startupScript`""
$trigger   = New-ScheduledTaskTrigger -AtLogOn -User "Administrator"
$principal = New-ScheduledTaskPrincipal -UserId "Administrator" -LogonType Interactive -RunLevel Highest
$settings  = New-ScheduledTaskSettingsSet `
                -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
                -StartWhenAvailable `
                -RestartCount 5 -RestartInterval (New-TimeSpan -Minutes 1)

Register-ScheduledTask -TaskName "GmExecutorStartup" `
    -Action $action -Trigger $trigger -Principal $principal -Settings $settings -Force | Out-Null

Write-Host "cloud-init complete. Clone the repo to $BaseDir\gm-executor, enable Administrator auto-logon, then reboot."
