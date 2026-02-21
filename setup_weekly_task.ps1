# WSB Woechentlicher E-Mail-Report - Task Scheduler Einrichtung
# Dieses Skript als Administrator ausfuehren:
#   Rechtsklick > "Mit PowerShell ausfuehren"
#
# Der Task laeuft jeden Sonntag um 18:00 Uhr.

$taskName = "WSB_Weekly_Email_Report"
$batPath = Join-Path $PSScriptRoot "weekly_email.bat"
$workDir = $PSScriptRoot

# Pruefen ob Task bereits existiert
$existing = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
if ($existing) {
    Write-Host "Task '$taskName' existiert bereits. Wird aktualisiert..." -ForegroundColor Yellow
    Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
}

# Aktion: weekly_email.bat ausfuehren
$action = New-ScheduledTaskAction -Execute $batPath -WorkingDirectory $workDir

# Trigger: Jeden Sonntag um 18:00
$trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Sunday -At "18:00"

# Einstellungen
$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -RunOnlyIfNetworkAvailable

# Task registrieren (laeuft als aktueller Benutzer)
Register-ScheduledTask `
    -TaskName $taskName `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -Description "GoldGräber: Woechentlicher E-Mail-Report mit Kaufempfehlungen" `
    -RunLevel Limited

Write-Host ""
Write-Host "Task '$taskName' erfolgreich eingerichtet!" -ForegroundColor Green
Write-Host "  Zeitplan: Jeden Sonntag um 18:00 Uhr" -ForegroundColor Cyan
Write-Host "  Aktion:   $batPath" -ForegroundColor Cyan
Write-Host ""
Write-Host "Verwalten unter: Taskplaner (taskschd.msc)" -ForegroundColor Gray
Write-Host ""
Read-Host "Druecke Enter zum Schliessen"
