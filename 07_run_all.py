"""
07 - Mutter-Skript
Fuehrt den Crawler, Excel-Export, Dashboard-Update und E-Mail-Report hintereinander aus.
Ein Doppelklick auf diese Datei startet den gesamten Durchlauf.
"""
import subprocess
import sys
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_FILE = os.path.join(BASE_DIR, "secret.env")

STEPS = [
    ("04_crawler.py", "Crawler"),
    ("06_excel_export.py", "Excel-Export"),
    ("08_dashboard_data.py", "Dashboard-Update"),
]


def email_configured():
    """Prueft ob E-Mail-Credentials in secret.env vorhanden sind."""
    if not os.path.exists(ENV_FILE):
        return False
    with open(ENV_FILE, "r", encoding="utf-8") as f:
        content = f.read()
    has_sender = "EMAIL_SENDER=" in content and "ihre-email" not in content
    has_password = "EMAIL_PASSWORD=" in content and "xxxx-xxxx" not in content
    return has_sender and has_password


def main():
    print("=" * 50)
    print("Reddit-Crawler + Excel + Dashboard + E-Mail")
    print("=" * 50)

    for script_name, label in STEPS:
        script_path = os.path.join(BASE_DIR, script_name)
        print(f"\n>>> Starte {label}...\n")
        result = subprocess.run([sys.executable, script_path])
        if result.returncode != 0:
            print(f"\n{label} mit Fehler beendet.")
            input("Druecke Enter zum Schliessen...")
            return

    # E-Mail-Report (nur wenn konfiguriert)
    if email_configured():
        print(f"\n>>> Starte E-Mail-Report...\n")
        email_path = os.path.join(BASE_DIR, "09_email_report.py")
        subprocess.run([sys.executable, email_path])
    else:
        print("\n  E-Mail-Report uebersprungen (nicht konfiguriert).")
        print("  Zum Einrichten: secret.env.example lesen.")

    print("\n" + "=" * 50)
    print("Fertig!")
    print("  Excel:     output/crawler_results.xlsx")
    print("  Dashboard: dashboard/index.html")
    print("=" * 50)
    input("Druecke Enter zum Schliessen...")


if __name__ == "__main__":
    main()
