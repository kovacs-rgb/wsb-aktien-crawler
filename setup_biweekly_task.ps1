# Reddit Aktien-Crawler - Alle 14 Tage Task Scheduler Einrichtung
# Dieses Skript als Administrator ausfuehren:
#   Rechtsklick > "Mit PowerShell ausfuehren"
#
# Der Task laeuft alle 14 Tage (sonntags um 18:00 Uhr).
# Crawlt alle 3 Kategorien (WSB, Meme, Multi-Bagger),
# aktualisiert Dashboard-Daten und sendet E-Mail-Report.

$taskName = "Reddit_Crawler_Biweekly"
$batPath = Join-Path $PSScriptRoot "weekly_email.bat"
$workDir = $PSScriptRoot

# Alten woechentlichen Task entfernen falls vorhanden
$oldTask = Get-ScheduledTask -TaskName "WSB_Weekly_Email_Report" -ErrorAction SilentlyContinue
if ($oldTask) {
    Write-Host "Alter woechentlicher Task gefunden. Wird entfernt..." -ForegroundColor Yellow
    Unregister-ScheduledTask -TaskName "WSB_Weekly_Email_Report" -Confirm:$false
}

# Pruefen ob Task bereits existiert
$existing = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
if ($existing) {
    Write-Host "Task '$taskName' existiert bereits. Wird aktualisiert..." -ForegroundColor Yellow
    Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
}

# Aktion: weekly_email.bat ausfuehren
$action = New-ScheduledTaskAction -Execute $batPath -WorkingDirectory $workDir

# Trigger: Alle 14 Tage (2 Wochen), sonntags um 18:00
$trigger = New-ScheduledTaskTrigger -Weekly -WeeksInterval 2 -DaysOfWeek Sunday -At "18:00"

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
    -Description "Reddit Aktien-Crawler: Alle 14 Tage Crawling + Dashboard + E-Mail (WSB, Meme, Multi-Bagger)" `
    -RunLevel Limited

Write-Host ""
Write-Host "Task '$taskName' erfolgreich eingerichtet!" -ForegroundColor Green
Write-Host "  Zeitplan: Alle 14 Tage, Sonntag um 18:00 Uhr" -ForegroundColor Cyan
Write-Host "  Aktion:   $batPath" -ForegroundColor Cyan
Write-Host "  Kategorien: WSB, Meme-Aktien, Multi-Bagger" -ForegroundColor Cyan
Write-Host ""
Write-Host "Verwalten unter: Taskplaner (taskschd.msc)" -ForegroundColor Gray
Write-Host ""
Read-Host "Druecke Enter zum Schliessen"
