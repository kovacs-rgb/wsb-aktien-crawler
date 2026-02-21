"""
09 - Woechentlicher E-Mail-Report (3 Kategorien)
Sendet die aktuellen Kaufempfehlungen per E-Mail.
Nutzt Gmail SMTP mit App-Passwort.

Drei Sektionen: WSB Top-5, Meme Top-5, Multi-Bagger Top-5

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

# Kategorie-Konfiguration
CATEGORY_STYLES = {
    "wsb": {
        "label": "WSB Top-5",
        "color": "#4d65ff",
        "gradient": "linear-gradient(135deg, #4d65ff, #818cf8)",
        "border_colors": ["#4d65ff", "#a855f7", "#06b6d4", "#ec4899", "#22c55e"],
        "score_key": "momentum_score",
        "score_label": "Momentum",
    },
    "meme": {
        "label": "Meme-Aktien",
        "color": "#f97316",
        "gradient": "linear-gradient(135deg, #ef4444, #f97316)",
        "border_colors": ["#ef4444", "#f97316", "#f59e0b", "#f43f5e", "#e11d48"],
        "score_key": "meme_score",
        "score_label": "Meme-Score",
    },
    "multibagger": {
        "label": "Multi-Bagger",
        "color": "#22c55e",
        "gradient": "linear-gradient(135deg, #22c55e, #eab308)",
        "border_colors": ["#22c55e", "#84cc16", "#eab308", "#14b8a6", "#06b6d4"],
        "score_key": "multibagger_score",
        "score_label": "Multi-Score",
    },
}


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


def build_category_html(cat_key, cat_data, budget_split_pct, total_budget):
    """Erstellt HTML fuer eine Kategorie-Sektion."""
    style = CATEGORY_STYLES.get(cat_key, CATEGORY_STYLES["wsb"])
    top5 = cat_data.get("top5", [])
    alloc = cat_data.get("budget", {}).get("allocation", [])
    alloc_map = {a["symbol"]: a for a in alloc}
    cat_budget = total_budget * budget_split_pct / 100

    if not top5:
        return ""

    html = f"""
    <div style="margin-top:25px;margin-bottom:15px;padding:12px 16px;background:{style['gradient']};border-radius:10px">
        <div style="font-size:16px;font-weight:800;color:#fff">{style['label']}</div>
        <div style="font-size:11px;color:rgba(255,255,255,.7)">Budget: {cat_budget:.0f} EUR ({budget_split_pct}%)</div>
    </div>
    """

    for i, item in enumerate(top5):
        al = alloc_map.get(item["symbol"], {})
        amount = al.get("weight_pct", 0) / 100 * cat_budget if al else 0
        isin = al.get("isin", "")
        sentiment = item.get("sentiment", {})
        sent_label = sentiment.get("label", "neutral") if isinstance(sentiment, dict) else str(sentiment) if sentiment else "neutral"
        sent_score = sentiment.get("score", 0) if isinstance(sentiment, dict) else 0
        sent_cls = "bullish" if sent_label == "bullish" else "bearish" if sent_label == "bearish" else "neutral"

        score_key = style["score_key"]
        score = item.get(score_key, item.get("momentum_score", 0))

        if score >= 70:
            hold_tag = '<span class="hold-tag hold-long">3-6 Monate</span>'
        elif score >= 50:
            hold_tag = '<span class="hold-tag hold-medium">1-3 Monate</span>'
        else:
            hold_tag = '<span class="hold-tag hold-short">1-4 Wochen</span>'

        flatex_link = ""
        if isin:
            flatex_link = f'<a href="https://www.flatex.de/suche?q={isin}" class="flatex-btn">Bei Flatex kaufen &rarr;</a>'

        # Potenzial
        potential_html = ""
        potential_pct = item.get("potential_pct")
        if potential_pct and potential_pct > 0:
            potential_html = f'<span class="detail"><span class="detail-label">Potenzial:</span> <span style="color:#fbbf24;font-weight:800">+{potential_pct:,}%</span></span>'

        # Kursdaten
        prices = item.get("prices", {})
        month_prices = prices.get("month", []) if isinstance(prices, dict) else prices
        price_info = ""
        if month_prices and len(month_prices) >= 2:
            last_price = month_prices[-1]["close"]
            first_price = month_prices[0]["close"]
            pct = ((last_price - first_price) / first_price * 100)
            pct_color = "#22c55e" if pct >= 0 else "#ef4444"
            price_info = f'<span class="detail"><span class="detail-label">Kurs:</span> <span class="detail-value">${last_price}</span></span> <span class="detail"><span class="detail-label">30T:</span> <span style="color:{pct_color};font-weight:700">{pct:+.1f}%</span></span>'

        border_color = style["border_colors"][i % len(style["border_colors"])]

        html += f"""
        <div class="stock-card" style="border-left-color:{border_color}">
            <div style="display:flex;justify-content:space-between;align-items:center">
                <div>
                    <div class="symbol">#{i+1} {item['symbol']}</div>
                    <div class="company">{item.get('company_name', '')}</div>
                </div>
                <div style="text-align:right">
                    <div class="budget-amount" style="color:{style['color']}">{amount:.0f} EUR</div>
                    <div style="font-size:11px;color:#888">{al.get('weight_pct', 0) if al else 0}%</div>
                </div>
            </div>
            <div class="details">
                <span class="detail"><span class="detail-label">{style['score_label']}:</span> <span class="detail-value">{score}</span></span>
                <span class="detail"><span class="detail-label">Sentiment:</span> <span class="{sent_cls}">{sent_label} ({sent_score:+.1f})</span></span>
                <span class="detail"><span class="detail-label">Erwaechnungen:</span> <span class="detail-value">{item['count']}</span></span>
                {price_info}
                {potential_html}
                <span class="detail">{hold_tag}</span>
            </div>
            {flatex_link}
        </div>
        """

    return html


def build_email_html(data):
    """Erstellt den HTML-Inhalt der Kaufempfehlungs-E-Mail mit 3 Kategorien."""
    categories = data.get("categories", {})
    budget = data.get("budget", {})
    total_budget = budget.get("monthly_eur", 200)
    split = budget.get("split", {"wsb": 50, "meme": 25, "multibagger": 25})
    generated = data.get("generated", "")

    html = f"""
    <html>
    <head>
    <style>
        body {{ font-family: 'Segoe UI', Arial, sans-serif; background: #0f0f14; color: #e0e0e0; padding: 20px; }}
        .container {{ max-width: 600px; margin: 0 auto; background: #1a1a24; border-radius: 16px; padding: 30px; border: 1px solid #2a2a3a; }}
        h1 {{ color: #fff; font-size: 22px; margin-bottom: 5px; }}
        .subtitle {{ color: #888; font-size: 13px; margin-bottom: 20px; }}
        .stock-card {{ background: linear-gradient(135deg, #1e1e2e, #252540); border-radius: 12px; padding: 16px; margin-bottom: 10px; border-left: 4px solid; }}
        .symbol {{ font-size: 18px; font-weight: 800; color: #fff; }}
        .company {{ font-size: 12px; color: #888; margin-top: 2px; }}
        .details {{ display: flex; gap: 12px; margin-top: 10px; flex-wrap: wrap; }}
        .detail {{ font-size: 12px; }}
        .detail-label {{ color: #666; }}
        .detail-value {{ color: #fff; font-weight: 700; }}
        .bullish {{ color: #22c55e; }}
        .bearish {{ color: #ef4444; }}
        .neutral {{ color: #888; }}
        .budget-amount {{ font-weight: 800; font-size: 16px; }}
        .hold-tag {{ font-size: 11px; padding: 2px 8px; border-radius: 4px; font-weight: 700; }}
        .hold-long {{ color: #22c55e; background: rgba(34,197,94,.15); }}
        .hold-medium {{ color: #06b6d4; background: rgba(6,182,212,.15); }}
        .hold-short {{ color: #f59e0b; background: rgba(245,158,11,.15); }}
        .flatex-btn {{ display: inline-block; background: linear-gradient(135deg, #0ea5e9, #2563eb); color: #fff; text-decoration: none; padding: 6px 14px; border-radius: 6px; font-size: 12px; font-weight: 700; margin-top: 8px; }}
        .footer {{ margin-top: 25px; padding-top: 15px; border-top: 1px solid #2a2a3a; font-size: 11px; color: #555; }}
        .total {{ font-size: 24px; font-weight: 900; margin: 10px 0; }}
        .split-info {{ display: flex; gap: 15px; margin-bottom: 5px; }}
        .split-item {{ font-size: 12px; font-weight: 700; }}
    </style>
    </head>
    <body>
    <div class="container">
        <h1>Reddit Kaufempfehlungen</h1>
        <div class="subtitle">{generated} &mdash; WSB + Meme + Multi-Bagger Analyse</div>

        <div class="total" style="color:#4d65ff">{total_budget:.0f} EUR Budget</div>
        <div class="split-info">
            <span class="split-item" style="color:#818cf8">WSB: {split.get('wsb', 50)}%</span>
            <span class="split-item" style="color:#f97316">Meme: {split.get('meme', 25)}%</span>
            <span class="split-item" style="color:#22c55e">Multi: {split.get('multibagger', 25)}%</span>
        </div>
    """

    # Drei Kategorie-Sektionen
    for cat_key in ["wsb", "meme", "multibagger"]:
        cat_data = categories.get(cat_key, {})
        cat_split = split.get(cat_key, 0)
        if cat_data:
            html += build_category_html(cat_key, cat_data, cat_split, total_budget)

    html += """
        <div class="footer">
            <strong style="color:#ef4444">Hinweis:</strong> Dies ist keine Anlageberatung.
            Alle Daten basieren auf Reddit-Erwaechnungen und automatischer Sentiment-Analyse.
            Eigene Recherche ist unbedingt erforderlich. Investitionen bergen Risiken bis hin zum Totalverlust.
            <br><br>
            Generiert von Reddit Aktien-Crawler Dashboard &mdash; WSB &bull; Meme &bull; Multi-Bagger
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
    print("=== E-Mail-Report (3 Kategorien) ===")

    if not os.path.exists(DATA_FILE):
        print(f"FEHLER: {DATA_FILE} nicht gefunden.")
        print("Bitte zuerst 08_dashboard_data.py ausfuehren.")
        return

    with open(DATA_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    categories = data.get("categories", {})
    if not categories:
        print("Keine Kategorie-Daten vorhanden.")
        return

    # Zusammenfassung anzeigen
    for cat_key in ["wsb", "meme", "multibagger"]:
        cat = categories.get(cat_key, {})
        top5 = cat.get("top5", [])
        if top5:
            symbols = [t["symbol"] for t in top5]
            print(f"  {CATEGORY_STYLES[cat_key]['label']}: {symbols}")
        else:
            print(f"  {CATEGORY_STYLES[cat_key]['label']}: Keine Daten")

    env = load_env()
    recipient = env.get("EMAIL_RECIPIENT", DEFAULT_RECIPIENT)
    today = datetime.now().strftime("%d.%m.%Y")
    subject = f"Reddit Kaufempfehlungen (WSB + Meme + Multi-Bagger) - {today}"

    print(f"\n  Empfaenger: {recipient}")

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
