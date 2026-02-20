@echo off
REM Woechentlicher E-Mail-Report - wird vom Task Scheduler aufgerufen
REM Fuehrt Crawler + Dashboard-Update + E-Mail-Versand aus

cd /d "%~dp0"
python 04_crawler.py
python 08_dashboard_data.py
python 09_email_report.py
