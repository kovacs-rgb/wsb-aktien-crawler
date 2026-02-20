"""
09 - Woechentlicher E-Mail-Report
Sendet die aktuellen Kaufempfehlungen per E-Mail.
Nutzt Gmail SMTP mit App-Passwort.

Einrichtung:
1. Google-Konto: https://myaccount.google.com/apppasswords
2. App-Passwort generieren (Mail, Windows)
3. In secret.env eintragen:
   EMAIL_SENDER=infokovacs@googlemail.com
   EMAIL_PASSWORD=xxxx-xxxx-xxxx-xxxx
   EMAIL_RECIPIENT=infokovacs@googlemail.com
"""
import json
import os
import smtplib
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(BASE_DIR, "dashboard", "data.json")
ENV_FILE = os.path.join(BASE_DIR, "secret.env")

# Standard-Empfaenger
DEFAULT_RECIPIENT = "infokovacs@googlemail.com"


def load_env():
    """Laedt E-Mail-Credentials aus secret.env."""
    env = {}
    if not os.path.exists(ENV_FILE):
        return env
    with open(ENV_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, val = line.split("=", 1)
                env[key.strip()] = val.strip()
    return env


def build_email_html(data):
    """Erstellt den HTML-Inhalt der Kaufempfehlungs-E-Mail."""
    top5 = data.get("top5", [])
    budget = data.get("budget", {})
    alloc = budget.get("allocation", [])
    monthly = budget.get("monthly_eur", 200)
    generated = data.get("generated", "")
    alloc_map = {a["symbol"]: a for a in alloc}

    html = f"""
    <html>
    <head>
    <style>
        body {{ font-family: 'Segoe UI', Arial, sans-serif; background: #0f0f14; color: #e0e0e0; padding: 20px; }}
        .container {{ max-width: 600px; margin: 0 auto; background: #1a1a24; border-radius: 16px; padding: 30px; border: 1px solid #2a2a3a; }}
        h1 {{ color: #fff; font-size: 22px; margin-bottom: 5px; }}
        .subtitle {{ color: #888; font-size: 13px; margin-bottom: 25px; }}
        .stock-card {{ background: linear-gradient(135deg, #1e1e2e, #252540); border-radius: 12px; padding: 16px; margin-bottom: 12px; border-left: 4px solid; }}
        .stock-card:nth-child(1) {{ border-color: #4d65ff; }}
        .stock-card:nth-child(2) {{ border-color: #a855f7; }}
        .stock-card:nth-child(3) {{ border-color: #06b6d4; }}
        .stock-card:nth-child(4) {{ border-color: #ec4899; }}
        .stock-card:nth-child(5) {{ border-color: #22c55e; }}
        .symbol {{ font-size: 20px; font-weight: 800; color: #fff; }}
        .company {{ font-size: 12px; color: #888; margin-top: 2px; }}
        .details {{ display: flex; gap: 15px; margin-top: 10px; flex-wrap: wrap; }}
        .detail {{ font-size: 12px; }}
        .detail-label {{ color: #666; }}
        .detail-value {{ color: #fff; font-weight: 700; }}
        .bullish {{ color: #22c55e; }}
        .bearish {{ color: #ef4444; }}
        .neutral {{ color: #888; }}
        .budget-row {{ display: flex; justify-content: space-between; align-items: center; padding: 8px 0; border-bottom: 1px solid #2a2a3a; }}
        .budget-amount {{ font-weight: 800; color: #4d65ff; }}
        .hold-tag {{ font-size: 11px; padding: 2px 8px; border-radius: 4px; font-weight: 700; }}
        .hold-long {{ color: #22c55e; background: rgba(34,197,94,.15); }}
        .hold-medium {{ color: #06b6d4; background: rgba(6,182,212,.15); }}
        .hold-short {{ color: #f59e0b; background: rgba(245,158,11,.15); }}
        .flatex-btn {{ display: inline-block; background: linear-gradient(135deg, #0ea5e9, #2563eb); color: #fff; text-decoration: none; padding: 6px 14px; border-radius: 6px; font-size: 12px; font-weight: 700; margin-top: 8px; }}
        .footer {{ margin-top: 25px; padding-top: 15px; border-top: 1px solid #2a2a3a; font-size: 11px; color: #555; }}
        .total {{ font-size: 24px; font-weight: 900; color: #4d65ff; margin: 10px 0; }}
    </style>
    </head>
    <body>
    <div class="container">
        <h1>WSB Kaufempfehlungen</h1>
        <div class="subtitle">Woche vom {generated} &mdash; r/wallstreetbets Analyse</div>

        <div class="total">{monthly:.0f} EUR Budget</div>
    """

    for i, item in enumerate(top5):
        al = alloc_map.get(item["symbol"], {})
        amount = al.get("amount_eur", 0)
        isin = al.get("isin", "")
        sentiment = item.get("sentiment", {})
        sent_label = sentiment.get("label", "neutral") if isinstance(sentiment, dict) else "neutral"
        sent_score = sentiment.get("score", 0) if isinstance(sentiment, dict) else 0
        sent_cls = "bullish" if sent_label == "bullish" else "bearish" if sent_label == "bearish" else "neutral"
        momentum = item.get("momentum_score", 0)

        if momentum >= 70:
            hold_tag = '<span class="hold-tag hold-long">3-6 Monate</span>'
        elif momentum >= 50:
            hold_tag = '<span class="hold-tag hold-medium">1-3 Monate</span>'
        else:
            hold_tag = '<span class="hold-tag hold-short">1-4 Wochen</span>'

        flatex_link = ""
        if isin:
            flatex_link = f'<a href="https://www.flatex.de/suche?q={isin}" class="flatex-btn">Bei Flatex kaufen &rarr;</a>'

        # Kursdaten
        prices = item.get("prices", [])
        price_info = ""
        if prices:
            last_price = prices[-1]["close"]
            first_price = prices[0]["close"]
            pct = ((last_price - first_price) / first_price * 100)
            pct_color = "#22c55e" if pct >= 0 else "#ef4444"
            price_info = f'<span class="detail"><span class="detail-label">Kurs:</span> <span class="detail-value">${last_price}</span></span> <span class="detail"><span class="detail-label">30T:</span> <span style="color:{pct_color};font-weight:700">{pct:+.1f}%</span></span>'

        html += f"""
        <div class="stock-card">
            <div style="display:flex;justify-content:space-between;align-items:center">
                <div>
                    <div class="symbol">#{i+1} {item['symbol']}</div>
                    <div class="company">{item.get('company_name', '')}</div>
                </div>
                <div style="text-align:right">
                    <div class="budget-amount">{amount:.0f} EUR</div>
                    <div style="font-size:11px;color:#888">{al.get('weight_pct', 0)}%</div>
                </div>
            </div>
            <div class="details">
                <span class="detail"><span class="detail-label">Score:</span> <span class="detail-value">{momentum}</span></span>
                <span class="detail"><span class="detail-label">Sentiment:</span> <span class="{sent_cls}">{sent_label} ({sent_score:+.1f})</span></span>
                <span class="detail"><span class="detail-label">Erwaechnungen:</span> <span class="detail-value">{item['count']}</span></span>
                {price_info}
                <span class="detail">{hold_tag}</span>
            </div>
            {flatex_link}
        </div>
        """

    html += """
        <div class="footer">
            <strong style="color:#ef4444">Hinweis:</strong> Dies ist keine Anlageberatung.
            Alle Daten basieren auf Reddit-Erwaechnungen und automatischer Sentiment-Analyse.
            Eigene Recherche ist unbedingt erforderlich. Investitionen bergen Risiken bis hin zum Totalverlust.
            <br><br>
            Generiert von WSB Aktien-Crawler Dashboard
        </div>
    </div>
    </body>
    </html>
    """
    return html


def send_email(recipient, subject, html_body, env):
    """Sendet eine E-Mail via Gmail SMTP."""
    sender = env.get("EMAIL_SENDER", "")
    password = env.get("EMAIL_PASSWORD", "")

    if not sender or not password:
        print("FEHLER: EMAIL_SENDER und EMAIL_PASSWORD muessen in secret.env stehen.")
        print("\nSo richtest du es ein:")
        print("1. Gehe zu: https://myaccount.google.com/apppasswords")
        print("2. Erstelle ein App-Passwort fuer 'Mail'")
        print("3. Trage in secret.env ein:")
        print(f"   EMAIL_SENDER={recipient}")
        print("   EMAIL_PASSWORD=dein-app-passwort")
        return False

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = recipient
    msg.attach(MIMEText(html_body, "html"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(sender, password)
            server.sendmail(sender, recipient, msg.as_string())
        return True
    except smtplib.SMTPAuthenticationError:
        print("FEHLER: Authentifizierung fehlgeschlagen.")
        print("Stelle sicher, dass du ein App-Passwort verwendest:")
        print("  https://myaccount.google.com/apppasswords")
        return False
    except Exception as e:
        print(f"FEHLER beim Senden: {e}")
        return False


def main():
    print("=== E-Mail-Report ===")

    if not os.path.exists(DATA_FILE):
        print(f"FEHLER: {DATA_FILE} nicht gefunden.")
        print("Bitte zuerst 08_dashboard_data.py ausfuehren.")
        return

    with open(DATA_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not data.get("top5"):
        print("Keine Top-5-Daten vorhanden.")
        return

    env = load_env()
    recipient = env.get("EMAIL_RECIPIENT", DEFAULT_RECIPIENT)
    today = datetime.now().strftime("%d.%m.%Y")
    subject = f"WSB Kaufempfehlungen - {today}"

    print(f"  Empfaenger: {recipient}")
    print(f"  Top 5: {[t['symbol'] for t in data['top5']]}")

    html = build_email_html(data)

    # E-Mail senden
    if send_email(recipient, subject, html, env):
        print(f"\n  E-Mail erfolgreich gesendet an {recipient}!")
    else:
        # Fallback: HTML als Datei speichern
        fallback_path = os.path.join(BASE_DIR, "dashboard", "email_preview.html")
        with open(fallback_path, "w", encoding="utf-8") as f:
            f.write(html)
        print(f"\n  E-Mail konnte nicht gesendet werden.")
        print(f"  Vorschau gespeichert: {fallback_path}")


if __name__ == "__main__":
    main()
