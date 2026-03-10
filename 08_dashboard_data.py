"""
08 - GoldGraeber Dashboard-Daten generieren (Multi-Kategorie)
Liest Pickle-Ergebnisse und erstellt JSON fuer das Web-Dashboard.
Drei Kategorien: WSB Top-5, Meme-Aktien, Multi-Bagger.
Berechnet: Momentum-Score, Meme-Score, Multi-Bagger-Score,
           Sentiment-Analyse, Vervielfachungspotenzial, Budget-Verteilung,
           Risiko-Score, ISIN/WKN, KI-Zusammenfassung, JustTrade-Links,
           Performance-Tracking.
Laedt Kursdaten (1W, 1M, 1J) und ISIN/WKN-Daten fuer Broker-Kauflinks.
"""
import csv
import json
import math
import os
import pickle
import re
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PICKLE_DIR = os.path.join(BASE_DIR, "pickle")
DASHBOARD_DIR = os.path.join(BASE_DIR, "dashboard")
OUTPUT_FILE = os.path.join(DASHBOARD_DIR, "data.json")
DATA_DIR = os.path.join(BASE_DIR, "data")
PERFORMANCE_FILE = os.path.join(DATA_DIR, "performance_history.json")

MONTHLY_BUDGET = 200.0

# ============================================================
# ISIN + WKN LOOKUP (fuer Flatex & JustTrade Kauflinks)
# ============================================================

def load_isin_wkn_mapping():
    """ISIN und WKN Mapping fuer alle bekannten Symbole."""
    known = {
        # Symbol: (ISIN, WKN)
        "AAPL": ("US0378331005", "865985"), "AMZN": ("US0231351067", "906866"),
        "AMD": ("US0079031078", "863186"), "AVGO": ("US11135F1012", "A2JG9Z"),
        "COIN": ("US19260Q1076", "A2QP7J"), "CVNA": ("US12686C1099", "A2DQB2"),
        "DASH": ("US86311W1062", "A2QHJ0"), "DELL": ("US24703L2025", "A2N6WP"),
        "DOW": ("US2605571031", "A2PFAX"), "DPZ": ("US25754A2015", "A0B6VQ"),
        "GOOG": ("US02079K3059", "A14Y6H"), "GOOGL": ("US02079K1079", "A14Y6F"),
        "GME": ("US36467W1099", "A0HGDX"), "HIMS": ("US43365H1023", "A2QMYY"),
        "HOOD": ("US4385161066", "A3CVQC"), "IREN": ("US46272L1089", "A3C7M0"),
        "META": ("US30303M1027", "A1JWVX"), "MSFT": ("US5949181045", "870747"),
        "MSTR": ("US5949724083", "722713"), "NFLX": ("US64110L1061", "552484"),
        "NVDA": ("US67066G1040", "918422"), "OKLO": ("US6790921038", "A40G23"),
        "OPEN": ("US6837121036", "A3CTJV"), "PLTR": ("US69608A1088", "A2QA4J"),
        "QQQ": ("US46090E1038", "A0F5UF"), "RDDT": ("US75734B1008", "A3EBLR"),
        "RKLB": ("US7575441023", "A3C975"), "SMH": ("US92189F6065", "A0MMR0"),
        "SNOW": ("US8334451098", "A2QB38"), "SOFI": ("US83406F1021", "A3CVMH"),
        "SOUN": ("US83607X1028", "A3C7SE"), "TSLA": ("US88160R1014", "A1CX3T"),
        "XOM": ("US30231G1022", "852549"), "RTX": ("US75513E1010", "A2PK2R"),
        "LMT": ("US5398301094", "894648"), "KTOS": ("US5028371042", "A2DVT5"),
        "AMC": ("US00165C3025", "A1W90H"), "CRWV": ("US22679L1008", "A40X9M"),
        "TNDM": ("US87588F1075", "A2H5TQ"), "PLUG": ("US72919P2020", "A1JA81"),
        "UNH": ("US91324P1021", "869561"),
        # Meme-Aktien
        "FFIE": ("US30249U1016", "A3DD6W"), "MULN": ("US62526P2074", "A3C7QP"),
        "TTOO": ("US87285P1003", "A1W2FN"), "FNGR": ("US31810F1012", "A2P74K"),
        "GNS": ("US36241M1027", "A3D1JH"),
        # Multi-Bagger
        "SEZL": ("US81733D1063", "A3DXUH"), "APP": ("US03835T1034", "A2QJFK"),
        "DAVE": ("US23830C1062", "A3C5QP"), "CRDO": ("US22612U1088", "A3CRZX"),
        "MRVL": ("US5738741041", "930131"), "HUT": ("CA44812W1068", "A3C4UV"),
        "SPOT": ("LU1778762911", "A2JEGN"), "SE": ("US81141R1005", "A2H5LX"),
        "AS": ("US02351X1028", "A3EHLT"), "LLY": ("US5324571083", "858560"),
        "NEE": ("US65339F1012", "A1CZ4H"),
    }
    return known

ISIN_WKN_MAPPING = load_isin_wkn_mapping()

def get_isin(symbol):
    entry = ISIN_WKN_MAPPING.get(symbol, None)
    return entry[0] if entry else ""

def get_wkn(symbol):
    entry = ISIN_WKN_MAPPING.get(symbol, None)
    return entry[1] if entry else ""

# ============================================================
# UNTERNEHMENS-BESCHREIBUNGEN
# ============================================================

COMPANY_INFO = {
    "AAPL": {"name": "Apple Inc.", "desc": "Technologiekonzern: iPhone, Mac, iPad, Services.", "sector": "Tech", "mcap": "large"},
    "AMD": {"name": "Advanced Micro Devices", "desc": "Chiphersteller: CPUs und GPUs fuer PCs, Server und Rechenzentren.", "sector": "Halbleiter", "mcap": "large"},
    "AMC": {"name": "AMC Entertainment", "desc": "Groesste Kinokette der Welt. Meme-Stock seit 2021.", "sector": "Unterhaltung", "mcap": "small"},
    "AMZN": {"name": "Amazon.com", "desc": "E-Commerce und Cloud-Computing (AWS).", "sector": "Tech/Handel", "mcap": "mega"},
    "APP": {"name": "AppLovin Corp.", "desc": "Mobile Advertising und App-Monetarisierung. Starkes Umsatzwachstum.", "sector": "Tech/Werbung", "mcap": "large"},
    "AS": {"name": "Amer Sports", "desc": "Sportartikelkonzern: Salomon, Arc'teryx, Wilson.", "sector": "Sport/Konsum", "mcap": "mid"},
    "AVGO": {"name": "Broadcom Inc.", "desc": "Halbleiter und Infrastruktur-Software fuer Rechenzentren.", "sector": "Halbleiter", "mcap": "mega"},
    "COIN": {"name": "Coinbase Global", "desc": "Groesste US-Kryptoboerse.", "sector": "Krypto/Finanzen", "mcap": "large"},
    "CRDO": {"name": "Credo Technology", "desc": "Halbleiter fuer Hochgeschwindigkeits-Datenverbindungen in KI-Rechenzentren.", "sector": "Halbleiter/KI", "mcap": "mid"},
    "CRWV": {"name": "Coreweave", "desc": "Cloud-Infrastruktur spezialisiert auf GPU-Computing.", "sector": "Cloud/KI", "mcap": "mid"},
    "CVNA": {"name": "Carvana Co.", "desc": "Online-Plattform fuer Gebrauchtwagen-Kauf und -Verkauf.", "sector": "E-Commerce/Auto", "mcap": "mid"},
    "DASH": {"name": "DoorDash Inc.", "desc": "Lieferdienst-Plattform fuer Restaurants.", "sector": "Lieferdienst", "mcap": "large"},
    "DAVE": {"name": "Dave Inc.", "desc": "Fintech-App: Banking, Vorschuesse, Budgetplanung.", "sector": "Fintech", "mcap": "micro"},
    "DELL": {"name": "Dell Technologies", "desc": "PCs, Server und IT-Infrastruktur.", "sector": "Tech/Hardware", "mcap": "large"},
    "DOW": {"name": "Dow Inc.", "desc": "Chemiekonzern: Kunststoffe, Beschichtungen.", "sector": "Chemie", "mcap": "large"},
    "DPZ": {"name": "Domino's Pizza", "desc": "Groesste Pizza-Lieferkette mit 20.000+ Filialen.", "sector": "Gastronomie", "mcap": "large"},
    "FFIE": {"name": "Faraday Future", "desc": "Elektroauto-Startup. Extrem spekulativ, Meme-Legende.", "sector": "E-Auto/Meme", "mcap": "nano"},
    "FNGR": {"name": "FingerMotion Inc.", "desc": "Mobile Daten-Services in China. Hochvolatil.", "sector": "Mobile/Daten", "mcap": "nano"},
    "GME": {"name": "GameStop Corp.", "desc": "Videospiel-Einzelhaendler. Bekanntester Meme-Stock.", "sector": "Einzelhandel/Meme", "mcap": "mid"},
    "GNS": {"name": "Genius Group", "desc": "Bildungsplattform mit Blockchain-Fokus.", "sector": "Bildung/Blockchain", "mcap": "nano"},
    "GOOG": {"name": "Alphabet (Google)", "desc": "Google, YouTube, Cloud, KI.", "sector": "Tech", "mcap": "mega"},
    "GOOGL": {"name": "Alphabet Class A", "desc": "Google, YouTube, Cloud, KI.", "sector": "Tech", "mcap": "mega"},
    "HIMS": {"name": "Hims & Hers Health", "desc": "Telemedizin-Plattform.", "sector": "Gesundheit", "mcap": "mid"},
    "HOOD": {"name": "Robinhood Markets", "desc": "Provisionsfreie Trading-App.", "sector": "Finanzen", "mcap": "mid"},
    "HUT": {"name": "Hut 8 Corp.", "desc": "Bitcoin-Mining und KI-Rechenzentren.", "sector": "Krypto/Mining", "mcap": "small"},
    "IREN": {"name": "Iris Energy", "desc": "Bitcoin-Mining mit erneuerbarer Energie.", "sector": "Krypto/Energie", "mcap": "mid"},
    "KTOS": {"name": "Kratos Defense", "desc": "Drohnen, Satelliten, Cybersecurity.", "sector": "Verteidigung", "mcap": "mid"},
    "LLY": {"name": "Eli Lilly", "desc": "Pharmakonzern: Diabetes, Adipositas (Mounjaro).", "sector": "Pharma", "mcap": "mega"},
    "LMT": {"name": "Lockheed Martin", "desc": "Groesster Ruestungskonzern.", "sector": "Verteidigung", "mcap": "mega"},
    "META": {"name": "Meta Platforms", "desc": "Facebook, Instagram, WhatsApp.", "sector": "Tech/Social", "mcap": "mega"},
    "MRVL": {"name": "Marvell Technology", "desc": "Custom AI-Chips und Datenzentrums-Infrastruktur.", "sector": "Halbleiter/KI", "mcap": "large"},
    "MSFT": {"name": "Microsoft Corp.", "desc": "Windows, Office, Azure Cloud, KI.", "sector": "Tech", "mcap": "mega"},
    "MSTR": {"name": "MicroStrategy", "desc": "Groesster Bitcoin-Halter unter Unternehmen.", "sector": "Tech/Krypto", "mcap": "large"},
    "MULN": {"name": "Mullen Automotive", "desc": "E-Auto-Startup. Starke Meme-Dynamik, Short-Squeeze Kandidat.", "sector": "E-Auto/Meme", "mcap": "nano"},
    "NEE": {"name": "NextEra Energy", "desc": "Groesster Erzeuger von Wind- und Solarenergie.", "sector": "Energie/Gruen", "mcap": "mega"},
    "NFLX": {"name": "Netflix Inc.", "desc": "Streaming-Marktfuehrer, 300+ Mio. Abonnenten.", "sector": "Unterhaltung", "mcap": "mega"},
    "NVDA": {"name": "NVIDIA Corp.", "desc": "Marktfuehrer fuer KI-Chips (GPUs).", "sector": "Halbleiter/KI", "mcap": "mega"},
    "OKLO": {"name": "Oklo Inc.", "desc": "Kleine modulare Kernreaktoren (SMR).", "sector": "Energie/Nuklear", "mcap": "small"},
    "OPEN": {"name": "Opendoor Technologies", "desc": "iBuyer fuer Wohnimmobilien.", "sector": "Immobilien/Tech", "mcap": "small"},
    "PLTR": {"name": "Palantir Technologies", "desc": "Big-Data und KI-Software fuer Regierungen/Unternehmen.", "sector": "Tech/KI", "mcap": "large"},
    "PLUG": {"name": "Plug Power", "desc": "Wasserstoff-Brennstoffzellen.", "sector": "Energie/Wasserstoff", "mcap": "small"},
    "QQQ": {"name": "Invesco QQQ Trust", "desc": "ETF auf Nasdaq-100.", "sector": "ETF/Tech", "mcap": "etf"},
    "RDDT": {"name": "Reddit Inc.", "desc": "Social-Media-Plattform mit Forum-Communities.", "sector": "Tech/Social", "mcap": "mid"},
    "RKLB": {"name": "Rocket Lab USA", "desc": "Raketenstartunternehmen fuer Kleinsatelliten.", "sector": "Raumfahrt", "mcap": "mid"},
    "RTX": {"name": "RTX Corp.", "desc": "Ruestungs- und Luftfahrtkonzern.", "sector": "Verteidigung", "mcap": "mega"},
    "SE": {"name": "Sea Ltd", "desc": "E-Commerce (Shopee), Gaming (Garena), Fintech.", "sector": "Tech/E-Commerce", "mcap": "large"},
    "SEZL": {"name": "Sezzle Inc.", "desc": "Buy Now Pay Later Fintech. 800%+ Wachstum.", "sector": "Fintech/BNPL", "mcap": "small"},
    "SMH": {"name": "VanEck Semiconductor ETF", "desc": "ETF auf Halbleiter-Unternehmen.", "sector": "ETF/Halbleiter", "mcap": "etf"},
    "SNOW": {"name": "Snowflake Inc.", "desc": "Cloud-Datenplattform.", "sector": "Cloud/Daten", "mcap": "large"},
    "SOFI": {"name": "SoFi Technologies", "desc": "Digitale Finanzplattform.", "sector": "Finanzen", "mcap": "mid"},
    "SOUN": {"name": "SoundHound AI", "desc": "KI-Spracherkennung fuer Autos und IoT.", "sector": "KI", "mcap": "mid"},
    "SPOT": {"name": "Spotify Technology", "desc": "Musik-Streaming-Marktfuehrer.", "sector": "Unterhaltung/Tech", "mcap": "large"},
    "TNDM": {"name": "Tandem Diabetes Care", "desc": "Insulinpumpen.", "sector": "Medizintechnik", "mcap": "small"},
    "TSLA": {"name": "Tesla Inc.", "desc": "Elektroautos, Energiespeicher, Solar.", "sector": "Auto/Energie", "mcap": "mega"},
    "TTOO": {"name": "T2 Biosystems", "desc": "Biotech: Schnelldiagnose fuer Infektionen. Pennystock.", "sector": "Biotech/Meme", "mcap": "nano"},
    "UNH": {"name": "UnitedHealth Group", "desc": "Groesster US-Krankenversicherer.", "sector": "Gesundheit", "mcap": "mega"},
    "XOM": {"name": "Exxon Mobil", "desc": "Groesster Oelkonzern der Welt.", "sector": "Energie/Oel", "mcap": "mega"},
}

# ============================================================
# RISIKO-SCORE (1-10): Totalverlust-Wahrscheinlichkeit
# ============================================================

def calculate_risk_score(symbol, cat_key, snippets, prices_data=None):
    """
    Risiko-Score 1-10:
    1-3 = Niedrig (gruene Mega/Large-Caps, ETFs)
    4-6 = Mittel (Mid-Caps, volatile Branchen)
    7-10 = Hoch (Nano/Micro-Caps, Meme-Stocks, Pennystocks)

    Faktoren:
    - Marktkapitalisierung (30%)
    - Kategorie Meme/WSB/Multi (20%)
    - Volatilitaet aus Kursdaten (20%)
    - Bearish Sentiment (15%)
    - Pennystock/Spekulations-Keywords (15%)
    """
    risk = 0.0
    company = COMPANY_INFO.get(symbol, {})
    mcap = company.get("mcap", "unknown")

    # 1. MARKTKAPITALISIERUNG (0-3 Punkte)
    mcap_risk = {
        "mega": 0.5, "large": 1.0, "mid": 1.5,
        "small": 2.2, "micro": 2.7, "nano": 3.0, "etf": 0.3, "unknown": 2.0
    }
    risk += mcap_risk.get(mcap, 2.0)

    # 2. KATEGORIE (0-2 Punkte)
    cat_risk = {"wsb": 1.0, "meme": 2.0, "multibagger": 1.5}
    risk += cat_risk.get(cat_key, 1.0)

    # 3. VOLATILITAET aus Kursdaten (0-2 Punkte)
    if prices_data and prices_data.get("month"):
        month_prices = [p["close"] for p in prices_data["month"]]
        if len(month_prices) >= 5:
            avg = sum(month_prices) / len(month_prices)
            if avg > 0:
                daily_changes = [abs(month_prices[i] - month_prices[i-1]) / month_prices[i-1] * 100
                                 for i in range(1, len(month_prices)) if month_prices[i-1] > 0]
                avg_vol = sum(daily_changes) / len(daily_changes) if daily_changes else 0
                # >5% taegl. Schwankung = max Risiko
                vol_risk = min(2.0, avg_vol / 2.5)
                risk += vol_risk
    else:
        risk += 1.0  # Keine Daten = mittleres Risiko

    # 4. BEARISH SENTIMENT (0-1.5 Punkte)
    if snippets:
        all_text = " ".join(snippets).lower()
        bearish_hits = sum(1 for w in re.findall(r"[a-zA-Z]+", all_text)
                          if w in {"scam","fraud","bankruptcy","bankrupt","worthless","garbage","trash","dead","collapse"})
        risk += min(1.5, bearish_hits * 0.3)

    # 5. SPEKULATIONS-KEYWORDS (0-1.5 Punkte)
    if snippets:
        all_text = " ".join(snippets).lower()
        spec_keywords = {"penny", "lottery", "lotto", "gamble", "degen", "yolo", "all in", "moonshot"}
        spec_hits = sum(all_text.count(kw) for kw in spec_keywords)
        risk += min(1.5, spec_hits * 0.2)

    # Clamp 1-10
    return max(1, min(10, round(risk)))


def risk_label(score):
    """Gibt Label und Farbe fuer Risiko-Score zurueck."""
    if score <= 3:
        return {"label": "Niedrig", "color": "green"}
    elif score <= 6:
        return {"label": "Mittel", "color": "yellow"}
    else:
        return {"label": "Hoch", "color": "red"}


# ============================================================
# KI-ZUSAMMENFASSUNG (regelbasiert, kein API-Call noetig)
# ============================================================

def generate_ai_summary(symbol, cat_key, snippets, sentiment, score, potential_pct):
    """
    Generiert eine kurze Zusammenfassung warum Reddit ueber diese Aktie spricht.
    Regelbasiert — keine externe KI-API noetig.
    """
    company = COMPANY_INFO.get(symbol, {})
    name = company.get("name", symbol)
    sector = company.get("sector", "")

    reasons = []

    # Sentiment-basierte Aussage
    if sentiment and sentiment.get("label") == "bullish":
        reasons.append(f"Die Reddit-Community ist ueberwiegend bullish zu {symbol}")
    elif sentiment and sentiment.get("label") == "bearish":
        reasons.append(f"Gemischte bis negative Stimmung zu {symbol} auf Reddit")

    # Kategorie-spezifisch
    if cat_key == "meme":
        if snippets:
            all_text = " ".join(snippets).lower()
            if "squeeze" in all_text:
                reasons.append("Short-Squeeze Potenzial wird diskutiert")
            if "diamond hands" in all_text or "ape" in all_text:
                reasons.append("Starke Community-Ueberzeugung (Diamond Hands)")
    elif cat_key == "multibagger":
        if snippets:
            all_text = " ".join(snippets).lower()
            if "growth" in all_text or "revenue" in all_text:
                reasons.append("Starkes Umsatzwachstum als Treiber")
            if "10x" in all_text or "100x" in all_text or "tenbagger" in all_text:
                reasons.append("Hohes Vervielfachungspotenzial erwartet")
    else:  # wsb
        if score >= 70:
            reasons.append("Hohes Momentum — einer der meistdiskutierten Werte")
        elif score >= 50:
            reasons.append("Solides Interesse auf WallStreetBets")

    # Potenzial
    if potential_pct and potential_pct > 500:
        reasons.append(f"Community sieht +{potential_pct}% Kurspotenzial")

    # Sektor-Kontext
    sector_context = {
        "Halbleiter/KI": "Profitiert vom KI-Boom",
        "Krypto/Finanzen": "Korreliert mit Krypto-Markt",
        "Krypto/Mining": "Bitcoin-Mining Zyklus als Katalysator",
        "Energie/Nuklear": "Nuklear-Renaissance als Megatrend",
        "Raumfahrt": "Wachsender Raumfahrt-Sektor",
        "Verteidigung": "Steigende Verteidigungsausgaben weltweit",
        "Fintech/BNPL": "Buy-Now-Pay-Later Wachstumsmarkt",
    }
    if sector in sector_context:
        reasons.append(sector_context[sector])

    if not reasons:
        reasons.append(f"{name} wird in Reddit-Foren aktiv diskutiert")

    return ". ".join(reasons[:3]) + "."


# ============================================================
# PERFORMANCE-TRACKING
# ============================================================

def load_performance_history():
    """Laedt gespeicherte Performance-Historie."""
    if os.path.exists(PERFORMANCE_FILE):
        try:
            with open(PERFORMANCE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"records": [], "stats": {}}


def save_performance_history(history):
    """Speichert Performance-Historie."""
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(PERFORMANCE_FILE, "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)


def update_performance_tracking(categories_data, stock_prices):
    """
    Trackt welche Aktien empfohlen wurden und zu welchem Kurs.
    Bei jedem Run wird geprueft wie sich vorherige Empfehlungen entwickelt haben.
    """
    history = load_performance_history()
    today = datetime.now().strftime("%Y-%m-%d")

    # 1. Aktualisiere vergangene Empfehlungen mit aktuellem Kurs
    for record in history.get("records", []):
        sym = record["symbol"]
        if sym in stock_prices and stock_prices[sym].get("week"):
            week_data = stock_prices[sym]["week"]
            if week_data:
                current_price = week_data[-1]["close"]
                record["current_price"] = current_price
                if record.get("rec_price") and record["rec_price"] > 0:
                    record["pct_change"] = round((current_price - record["rec_price"]) / record["rec_price"] * 100, 1)

    # 2. Neue Empfehlungen hinzufuegen (Top-5 jeder Kategorie)
    existing_keys = {(r["symbol"], r["rec_date"]) for r in history.get("records", [])}

    for cat_key, data_list in categories_data.items():
        if not data_list:
            continue
        latest = sorted(data_list, key=lambda x: x["run_id"])[-1]
        sorted_syms = sorted(latest["results"].items(), key=lambda x: x[1], reverse=True)[:5]

        for symbol, count in sorted_syms:
            key = (symbol, today)
            if key in existing_keys:
                continue

            rec_price = None
            if symbol in stock_prices and stock_prices[symbol].get("week"):
                week_data = stock_prices[symbol]["week"]
                if week_data:
                    rec_price = week_data[-1]["close"]

            history.setdefault("records", []).append({
                "symbol": symbol,
                "category": cat_key,
                "rec_date": today,
                "rec_price": rec_price,
                "current_price": rec_price,
                "pct_change": 0.0,
            })

    # 3. Statistiken berechnen
    records = history.get("records", [])
    if records:
        with_change = [r for r in records if r.get("pct_change") is not None and r.get("rec_price")]
        if with_change:
            winners = sum(1 for r in with_change if r["pct_change"] > 0)
            losers = sum(1 for r in with_change if r["pct_change"] < 0)
            avg_return = round(sum(r["pct_change"] for r in with_change) / len(with_change), 1)
            best = max(with_change, key=lambda r: r["pct_change"])
            worst = min(with_change, key=lambda r: r["pct_change"])

            history["stats"] = {
                "total_recommendations": len(with_change),
                "winners": winners,
                "losers": losers,
                "win_rate_pct": round(winners / len(with_change) * 100, 1) if with_change else 0,
                "avg_return_pct": avg_return,
                "best": {"symbol": best["symbol"], "pct": best["pct_change"], "date": best["rec_date"]},
                "worst": {"symbol": worst["symbol"], "pct": worst["pct_change"], "date": worst["rec_date"]},
            }

    # Nur die letzten 200 Records behalten
    history["records"] = history.get("records", [])[-200:]

    save_performance_history(history)
    return history


# ============================================================
# SENTIMENT-ANALYSE (Keyword-basiert)
# ============================================================

BULLISH_WORDS = {
    "buy", "calls", "call", "long", "moon", "rocket", "bullish", "bull",
    "yolo", "diamond", "hands", "hold", "hodl", "squeeze", "pump",
    "undervalued", "cheap", "dip", "loading", "accumulate", "green",
    "breakout", "rally", "gains", "tendies", "print", "money",
    "up", "high", "growth", "profit", "winner", "strong", "surge",
    "rip", "fire", "lambo", "millionaire", "rich", "golden",
    "oversold", "recovery", "bounce", "flying", "skyrocket",
}

BEARISH_WORDS = {
    "sell", "puts", "put", "short", "crash", "bearish", "bear",
    "dump", "overvalued", "expensive", "bag", "bagholder", "loss",
    "red", "drop", "tank", "sink", "fall", "drilling", "drill",
    "dead", "rip", "worthless", "garbage", "trash", "scam",
    "fraud", "down", "low", "weak", "bankruptcy", "bankrupt",
    "overbought", "bubble", "collapse", "plunge", "bleeding",
}

# ============================================================
# MEME-AKTIEN KEYWORDS
# ============================================================

SQUEEZE_WORDS = {
    "squeeze", "short squeeze", "gamma squeeze", "short interest",
    "float", "low float", "shares outstanding", "days to cover",
    "dtc", "ctb", "cost to borrow", "utilization",
    "ftd", "failure to deliver", "threshold", "reg sho",
    "naked short", "dark pool", "ortex", "fintel",
}

MEME_HYPE_WORDS = {
    "yolo", "diamond hands", "ape", "apes", "degen", "degenerate",
    "gamma", "options chain", "call sweep", "unusual activity",
    "penny", "penny stock", "lottery", "lotto", "bet",
    "moonshot", "mooning", "squeeze play",
    "bagholding", "avg down", "all in",
}

# ============================================================
# MULTI-BAGGER KEYWORDS
# ============================================================

MULTIBAGGER_WORDS = {
    "10x", "100x", "1000x", "multi bagger", "multibagger", "tenbagger",
    "ten bagger", "100 bagger", "compounder", "exponential",
    "parabolic", "generational", "generational wealth",
    "life changing", "retire", "retirement",
}

GROWTH_WORDS = {
    "revenue growth", "tam", "total addressable market",
    "market cap", "mcap", "undervalued", "fair value",
    "intrinsic value", "dcf", "moat", "competitive advantage",
    "margin expansion", "operating leverage", "scale",
    "recurring revenue", "arr", "saas",
    "eps growth", "guidance", "beat estimates", "upgrade",
    "analyst upgrade", "price target",
    "disruptor", "category killer", "first mover",
}

CONVICTION_WORDS = {
    "long term hold", "never selling", "conviction",
    "thesis", "due diligence", "deep dive",
    "fundamentals", "catalyst", "upcoming catalyst",
    "earnings play", "insider buying", "institutional",
}


def analyze_sentiment(snippets):
    if not snippets:
        return {"score": 0, "label": "neutral", "bullish": 0, "bearish": 0, "total_words": 0}
    bullish_count = 0
    bearish_count = 0
    total_words = 0
    for snippet in snippets:
        words = re.findall(r"[a-zA-Z]+", snippet.lower())
        total_words += len(words)
        for w in words:
            if w in BULLISH_WORDS:
                bullish_count += 1
            elif w in BEARISH_WORDS:
                bearish_count += 1
    total_sentiment = bullish_count + bearish_count
    score = round((bullish_count - bearish_count) / total_sentiment, 2) if total_sentiment else 0
    if score > 0.2:
        label = "bullish"
    elif score < -0.2:
        label = "bearish"
    else:
        label = "neutral"
    return {"score": score, "label": label, "bullish": bullish_count, "bearish": bearish_count, "total_words": total_words}


# ============================================================
# VERVIELFACHUNGSPOTENZIAL
# ============================================================

def extract_potential(snippets):
    if not snippets:
        return None
    potentials = []
    all_text = " ".join(snippets).lower()
    for m in re.finditer(r"\b(\d+)\s*x\b", all_text):
        try:
            mult = int(m.group(1))
            if 2 <= mult <= 10000:
                potentials.append(mult * 100)
        except ValueError:
            pass
    for m in re.finditer(r"[+]?\s*(\d{2,5})\s*%", all_text):
        try:
            pct = int(m.group(1))
            if 50 <= pct <= 100000:
                potentials.append(pct)
        except ValueError:
            pass
    word_to_num = {"two": 2, "three": 3, "five": 5, "ten": 10, "twenty": 20, "hundred": 100}
    for m in re.finditer(r"(\w+)[\s-]*bagger", all_text):
        word = m.group(1)
        if word in word_to_num:
            potentials.append(word_to_num[word] * 100)
        else:
            try:
                potentials.append(int(word) * 100)
            except ValueError:
                pass
    if not potentials:
        return None
    avg = sum(potentials) / len(potentials)
    return round(avg)


# ============================================================
# SCORING-ALGORITHMEN
# ============================================================

def calculate_momentum(symbol, history, latest_count, all_runs_count):
    counts = [h["count"] for h in history]
    max_count_ever = max(counts) if counts else 1
    pop_score = min(40, (latest_count / max(max_count_ever, 1)) * 40)
    if len(counts) >= 2:
        prev = counts[-2]
        growth_ratio = latest_count / prev if prev > 0 else 2.0
        growth_score = min(30, max(0, (math.log2(max(growth_ratio, 0.1)) + 1) * 15))
    else:
        growth_score = 15
    if all_runs_count > 1:
        consistency_score = (len(counts) / all_runs_count) * 15
    else:
        consistency_score = 7.5
    return round(min(85, pop_score + growth_score + consistency_score), 1)


def add_sentiment_to_momentum(base_momentum, sentiment_score):
    sentiment_bonus = (sentiment_score + 1) / 2 * 15
    return round(min(100, max(0, base_momentum + sentiment_bonus)), 1)


def calculate_meme_score(symbol, history, latest_count, all_runs_count, snippets):
    counts = [h["count"] for h in history]
    total_words = 0
    all_text_lower = ""
    for snippet in snippets:
        words = snippet.lower().split()
        total_words += len(words)
        all_text_lower += " " + snippet.lower()
    total_words = max(total_words, 1)
    squeeze_hits = sum(all_text_lower.count(kw) for kw in SQUEEZE_WORDS)
    squeeze_density = (squeeze_hits / total_words) * 100
    squeeze_score = min(30, squeeze_density * 15)
    hype_hits = sum(all_text_lower.count(kw) for kw in MEME_HYPE_WORDS)
    hype_density = (hype_hits / total_words) * 100
    hype_score = min(25, hype_density * 12)
    if len(counts) >= 2:
        prev = counts[-2]
        velocity_ratio = latest_count / prev if prev > 0 else 5.0
        velocity_score = min(25, max(0, math.log2(max(velocity_ratio, 0.1)) * 12.5))
    else:
        velocity_score = 20
    bullish_count = 0
    total_sentiment = 0
    for snippet in snippets:
        for w in re.findall(r"[a-zA-Z]+", snippet.lower()):
            if w in BULLISH_WORDS or w in MEME_HYPE_WORDS:
                bullish_count += 1
                total_sentiment += 1
            elif w in BEARISH_WORDS:
                total_sentiment += 1
    conviction_score = (bullish_count / max(total_sentiment, 1)) * 20
    total = squeeze_score + hype_score + velocity_score + conviction_score
    return round(min(100, max(0, total)), 1)


def calculate_multibagger_score(symbol, history, latest_count, all_runs_count, snippets):
    counts = [h["count"] for h in history]
    total_words = 0
    all_text_lower = ""
    for snippet in snippets:
        total_words += len(snippet.lower().split())
        all_text_lower += " " + snippet.lower()
    total_words = max(total_words, 1)
    growth_hits = sum(all_text_lower.count(kw) for kw in GROWTH_WORDS)
    growth_density = (growth_hits / total_words) * 100
    growth_score = min(30, growth_density * 12)
    multi_hits = sum(all_text_lower.count(kw) for kw in MULTIBAGGER_WORDS)
    multi_density = (multi_hits / total_words) * 100
    multi_score = min(25, multi_density * 15)
    if all_runs_count > 1:
        consistency_base = (len(counts) / all_runs_count) * 10
    else:
        consistency_base = 5
    conviction_hits = sum(all_text_lower.count(kw) for kw in CONVICTION_WORDS)
    conviction_density = (conviction_hits / total_words) * 100
    conviction_language = min(15, conviction_density * 10)
    conviction_score = min(25, consistency_base + conviction_language)
    catalyst_words = {"catalyst", "earnings", "fda", "approval", "contract",
                      "partnership", "acquisition", "merger", "buyout",
                      "insider buying", "institutional"}
    cat_hits = sum(all_text_lower.count(kw) for kw in SQUEEZE_WORDS | catalyst_words)
    cat_density = (cat_hits / total_words) * 100
    catalyst_score = min(20, cat_density * 10)
    total = growth_score + multi_score + conviction_score + catalyst_score
    return round(min(100, max(0, total)), 1)


# ============================================================
# BUDGET-VERTEILUNG
# ============================================================

def calculate_budget(top_symbols, budget=MONTHLY_BUDGET, score_key="momentum_score"):
    eligible = [(s, s.get(score_key, 0)) for s in top_symbols if s.get(score_key, 0) > 30]
    if not eligible:
        eligible = [(s, 50) for s in top_symbols]
    if not eligible:
        return []
    total_score = sum(score for _, score in eligible)
    result = []
    for sym_data, score in eligible:
        weight = score / total_score
        amount = round(budget * weight, 2)
        symbol = sym_data["symbol"]
        isin = get_isin(symbol)
        wkn = get_wkn(symbol)
        entry = {
            "symbol": symbol,
            "amount_eur": amount,
            "weight_pct": round(weight * 100, 1),
            "isin": isin,
            "wkn": wkn,
        }
        if isin:
            entry["flatex_url"] = f"https://www.flatex.de/suche?q={isin}"
            entry["justtrade_url"] = f"https://www.justtrade.com/suche?q={isin}"
            entry["traderepublic_url"] = f"https://app.traderepublic.com/search/{isin}"
            entry["scalable_url"] = f"https://de.scalable.capital/broker/security?isin={isin}"
        result.append(entry)
    return result


# ============================================================
# KURSDATEN (Yahoo Finance) - 3 Zeitraeume
# ============================================================

def fetch_stock_prices_multi(symbols):
    try:
        import yfinance as yf
    except ImportError:
        print("  yfinance nicht installiert - ueberspringe Kursdaten")
        return {}
    if not symbols:
        return {}
    print(f"\n  Lade Kursdaten fuer {len(symbols)} Symbole...")
    prices = {}
    periods = [("5d", "week", "1d"), ("1mo", "month", "1d"), ("1y", "year", "1wk")]
    for period, key, interval in periods:
        print(f"    Zeitraum: {key}...")
        try:
            data = yf.download(symbols, period=period, interval=interval, progress=False)
            if data.empty:
                continue
            for symbol in symbols:
                if symbol not in prices:
                    prices[symbol] = {}
                try:
                    if len(symbols) == 1:
                        close_data = data["Close"]
                    else:
                        close_data = data["Close"][symbol]
                    price_list = []
                    for date, price in close_data.dropna().items():
                        price_list.append({
                            "date": date.strftime("%Y-%m-%d"),
                            "close": round(float(price), 2),
                        })
                    if price_list:
                        prices[symbol][key] = price_list
                except Exception:
                    pass
        except Exception as e:
            print(f"    Fehler bei {key}: {e}")
    loaded = sum(1 for s in prices if prices[s])
    print(f"    {loaded} Symbole mit Kursdaten geladen.")
    return prices


# ============================================================
# PICKLE-DATEIEN LESEN (nach Kategorie)
# ============================================================

def read_pickle_files_by_category(directory):
    by_category = {"wsb": [], "meme": [], "multibagger": []}
    if not os.path.exists(directory):
        return by_category
    for filename in sorted(os.listdir(directory)):
        if not (filename.endswith(".pkl") or filename.endswith(".pickle")):
            continue
        filepath = os.path.join(directory, filename)
        try:
            with open(filepath, "rb") as f:
                data = pickle.load(f)
            cat = data.get("category", None)
            if cat is None:
                if "_meme_" in filename:
                    cat = "meme"
                elif "_multibagger_" in filename:
                    cat = "multibagger"
                else:
                    cat = "wsb"
            if cat in by_category:
                by_category[cat].append(data)
        except Exception as e:
            print(f"  FEHLER bei {filename}: {e}")
    return by_category


# ============================================================
# KATEGORIE-DATEN AUFBEREITEN
# ============================================================

def build_category_data(data_list, cat_key, score_fn=None, budget=MONTHLY_BUDGET, stock_prices=None):
    if not data_list:
        return {"runs": [], "top5": [], "all_ranked": [], "all_symbols": {},
                "budget": {"category_eur": budget, "allocation": []},
                "latest_run": None, "total_runs": 0}

    runs = []
    all_symbol_history = {}
    latest_snippets = {}

    for entry in sorted(data_list, key=lambda x: x["run_id"]):
        runs.append({
            "id": entry["run_id"],
            "total_posts": entry.get("total_posts", 0),
            "results": entry["results"],
        })
        for symbol, count in entry["results"].items():
            if symbol not in all_symbol_history:
                all_symbol_history[symbol] = []
            all_symbol_history[symbol].append({"run": entry["run_id"], "count": count})
        if "snippets" in entry:
            latest_snippets = entry["snippets"]

    latest_run = runs[-1] if runs else None
    total_runs = len(runs)

    top_symbols = []
    if latest_run:
        sorted_symbols = sorted(latest_run["results"].items(), key=lambda x: x[1], reverse=True)

        for symbol, count in sorted_symbols:
            history = all_symbol_history.get(symbol, [])
            counts = [h["count"] for h in history]
            if len(counts) >= 2:
                prev = counts[-2]
                trend_pct = round(((count - prev) / prev) * 100, 1) if prev > 0 else 100.0
            else:
                trend_pct = 0.0

            snippets = latest_snippets.get(symbol, [])
            sentiment = analyze_sentiment(snippets)
            potential = extract_potential(snippets)

            if score_fn:
                score = score_fn(symbol, history, count, total_runs, snippets)
                score_key = "meme_score" if cat_key == "meme" else "multibagger_score"
            else:
                base = calculate_momentum(symbol, history, count, total_runs)
                score = add_sentiment_to_momentum(base, sentiment["score"])
                score_key = "momentum_score"

            company = COMPANY_INFO.get(symbol, {})
            isin = get_isin(symbol)
            wkn = get_wkn(symbol)

            # Risiko-Score berechnen
            prices_data = stock_prices.get(symbol, {}) if stock_prices else {}
            risk = calculate_risk_score(symbol, cat_key, snippets, prices_data)
            risk_info = risk_label(risk)

            # KI-Zusammenfassung
            ai_summary = generate_ai_summary(symbol, cat_key, snippets, sentiment, score, potential)

            entry = {
                "symbol": symbol,
                "count": count,
                "trend_pct": trend_pct,
                "history": history,
                "sentiment": sentiment,
                score_key: score,
                "momentum_score": score,
                "potential_pct": potential,
                "company_name": company.get("name", symbol),
                "company_desc": company.get("desc", ""),
                "sector": company.get("sector", ""),
                "isin": isin,
                "wkn": wkn,
                "risk_score": risk,
                "risk_label": risk_info["label"],
                "risk_color": risk_info["color"],
                "ai_summary": ai_summary,
            }
            top_symbols.append(entry)

    top_symbols.sort(key=lambda x: x["momentum_score"], reverse=True)

    sk = "momentum_score"
    budget_alloc = calculate_budget(top_symbols[:5], budget=budget, score_key=sk)

    top5 = top_symbols[:5]

    return {
        "runs": runs,
        "top5": top5,
        "all_symbols": all_symbol_history,
        "latest_run": latest_run["id"] if latest_run else None,
        "total_runs": total_runs,
        "all_ranked": [
            {
                "symbol": s["symbol"],
                "count": s["count"],
                "momentum_score": s["momentum_score"],
                "sentiment": s["sentiment"]["label"],
                "sentiment_score": s["sentiment"]["score"],
                "potential_pct": s.get("potential_pct"),
                "isin": get_isin(s["symbol"]),
                "wkn": get_wkn(s["symbol"]),
                "company_name": s.get("company_name", s["symbol"]),
                "company_desc": s.get("company_desc", ""),
                "sector": s.get("sector", ""),
                "risk_score": s.get("risk_score", 5),
                "risk_label": s.get("risk_label", "Mittel"),
                "risk_color": s.get("risk_color", "yellow"),
                "ai_summary": s.get("ai_summary", ""),
            }
            for s in top_symbols
        ],
        "budget": {
            "category_eur": budget,
            "allocation": budget_alloc,
        },
    }


# ============================================================
# HAUPT-LOGIK
# ============================================================

def build_dashboard_data_multi(categories_data):
    default_split = {"wsb": 50, "meme": 25, "multibagger": 25}

    result = {
        "generated": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "categories": {},
        "budget": {
            "monthly_eur": MONTHLY_BUDGET,
            "split": default_split,
        },
    }

    score_fns = {
        "wsb": None,
        "meme": calculate_meme_score,
        "multibagger": calculate_multibagger_score,
    }

    cat_labels = {
        "wsb": {"label": "WSB Top-5", "label_short": "WSB", "color_scheme": "blue-purple", "score_type": "momentum_score"},
        "meme": {"label": "Meme-Aktien", "label_short": "Meme", "color_scheme": "red-orange", "score_type": "meme_score"},
        "multibagger": {"label": "Multi-Bagger", "label_short": "Multi", "color_scheme": "green-gold", "score_type": "multibagger_score"},
    }

    # Zuerst alle Symbole sammeln
    all_stock_symbols = set()
    for cat_key, data_list in categories_data.items():
        for entry in data_list:
            all_stock_symbols.update(entry.get("results", {}).keys())

    # Kursdaten fuer ALLE Symbole laden (ein Aufruf)
    stock_prices = fetch_stock_prices_multi(list(all_stock_symbols))
    result["stock_prices"] = stock_prices

    for cat_key, data_list in categories_data.items():
        cat_budget = MONTHLY_BUDGET * default_split.get(cat_key, 0) / 100
        cat_result = build_category_data(
            data_list, cat_key,
            score_fn=score_fns.get(cat_key),
            budget=cat_budget,
            stock_prices=stock_prices,
        )
        cat_result.update(cat_labels.get(cat_key, {}))
        cat_result["budget"]["split_pct"] = default_split.get(cat_key, 0)
        result["categories"][cat_key] = cat_result

        for s in cat_result.get("top5", []):
            all_stock_symbols.add(s["symbol"])

    # Kursdaten an top5 anhaengen
    for cat_key in result["categories"]:
        for item in result["categories"][cat_key].get("top5", []):
            item["prices"] = stock_prices.get(item["symbol"], {})

    # Performance-Tracking aktualisieren
    perf_history = update_performance_tracking(categories_data, stock_prices)
    result["performance"] = perf_history.get("stats", {})
    # Letzte 20 Records fuer Dashboard
    recent_records = perf_history.get("records", [])[-20:]
    result["performance_records"] = recent_records

    # Abwaertskompatibilitaet: WSB-Daten auf Root-Level
    wsb = result["categories"].get("wsb", {})
    result["runs"] = wsb.get("runs", [])
    result["top5"] = wsb.get("top5", [])
    result["all_symbols"] = wsb.get("all_symbols", {})
    result["latest_run"] = wsb.get("latest_run")
    result["total_runs"] = wsb.get("total_runs", 0)
    result["all_ranked"] = wsb.get("all_ranked", [])
    result["budget"]["allocation"] = wsb.get("budget", {}).get("allocation", [])

    return result


def main():
    print("=== GoldGraeber Dashboard-Daten generieren ===")

    os.makedirs(DASHBOARD_DIR, exist_ok=True)

    if not os.path.exists(PICKLE_DIR):
        print("Keine Pickle-Daten vorhanden. Erstelle Demo-Daten...")
        create_demo_data()

    categories_data = read_pickle_files_by_category(PICKLE_DIR)

    total_files = sum(len(v) for v in categories_data.values())
    if total_files == 0:
        print("Keine Pickle-Dateien gefunden. Erstelle Demo-Daten...")
        create_demo_data()
        categories_data = read_pickle_files_by_category(PICKLE_DIR)

    print(f"\nGefundene Pickle-Dateien:")
    for cat, files in categories_data.items():
        print(f"  {cat}: {len(files)} Dateien")

    dashboard_data = build_dashboard_data_multi(categories_data)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(dashboard_data, f, ensure_ascii=False, indent=2)

    print(f"\nDashboard-Daten gespeichert: {OUTPUT_FILE}")

    for cat_key, cat_data in dashboard_data.get("categories", {}).items():
        top5 = cat_data.get("top5", [])
        label = cat_data.get("label", cat_key)
        if top5:
            print(f"\n  {label}:")
            for s in top5:
                pot = f" | Potenzial: +{s['potential_pct']}%" if s.get("potential_pct") else ""
                risk = f" | Risiko: {s['risk_score']}/10 ({s['risk_label']})"
                isin = f" | ISIN: {s.get('isin', 'n/a')}"
                print(f"    {s['symbol']}: Score {s['momentum_score']} | {s['sentiment']['label']}{pot}{risk}{isin}")

    # Performance-Statistiken ausgeben
    perf = dashboard_data.get("performance", {})
    if perf:
        print(f"\n  Performance-Tracking:")
        print(f"    Empfehlungen: {perf.get('total_recommendations', 0)}")
        print(f"    Trefferquote: {perf.get('win_rate_pct', 0)}%")
        print(f"    Durchschnittsrendite: {perf.get('avg_return_pct', 0)}%")


def create_demo_data():
    import random
    os.makedirs(PICKLE_DIR, exist_ok=True)
    wsb_symbols = {
        "TSLA": (15, 35), "NVDA": (10, 30), "PLTR": (8, 25),
        "AAPL": (5, 15), "GOOG": (5, 12), "AMD": (5, 18),
        "GME": (4, 12), "AMZN": (3, 10), "META": (3, 8),
    }
    wsb_snippets = {
        "TSLA": ["TSLA to the moon 10x potential, buying more calls", "TSLA looks bullish after earnings squeeze incoming"],
        "NVDA": ["NVDA is undervalued, loading up on calls", "NVDA 100x potential AI revolution"],
        "PLTR": ["PLTR long term hold diamond hands conviction thesis", "PLTR contract news bullish catalyst"],
    }
    meme_symbols = {
        "GME": (8, 25), "AMC": (6, 20), "FFIE": (4, 18),
        "MULN": (3, 15), "TTOO": (2, 12), "FNGR": (2, 10),
    }
    meme_snippets = {
        "GME": ["GME short squeeze gamma squeeze incoming diamond hands apes", "GME low float high short interest YOLO all in"],
        "FFIE": ["FFIE short squeeze potential 100x lottery ticket moonshot", "FFIE cost to borrow through the roof ortex data"],
        "MULN": ["MULN squeeze play diamond hands apes strong", "MULN low float naked short failure to deliver"],
    }
    multi_symbols = {
        "PLTR": (10, 30), "SEZL": (6, 20), "CRDO": (5, 18),
        "MRVL": (4, 15), "APP": (3, 12), "HUT": (3, 10),
    }
    multi_snippets = {
        "PLTR": ["PLTR 10x potential AI disruptor total addressable market huge", "PLTR revenue growth moat conviction long term hold"],
        "SEZL": ["SEZL tenbagger exponential growth BNPL", "SEZL 100x life changing generational wealth"],
        "CRDO": ["CRDO margin expansion analyst upgrade revenue growth", "CRDO 5x potential catalyst earnings beat estimates"],
    }
    categories = [
        ("wsb", wsb_symbols, wsb_snippets),
        ("meme", meme_symbols, meme_snippets),
        ("multibagger", multi_symbols, multi_snippets),
    ]
    for cat_key, symbols, snippets in categories:
        for day_offset in range(5):
            run_id = f"2502{17 + day_offset:02d}-0700"
            results = {}
            for symbol, (low, high) in symbols.items():
                val = random.randint(low, high)
                if day_offset > 2:
                    val = int(val * 1.3)
                if val > 3:
                    results[symbol] = val
            snips = {s: snippets.get(s, []) for s in results}
            data = {
                "run_id": run_id,
                "category": cat_key,
                "category_label": cat_key,
                "results": results,
                "snippets": snips,
                "total_posts": random.randint(40, 100),
            }
            filepath = os.path.join(PICKLE_DIR, f"{run_id}_{cat_key}_crawler-ergebnis.pkl")
            with open(filepath, "wb") as f:
                pickle.dump(data, f)
            print(f"  Demo: {run_id}_{cat_key}")


if __name__ == "__main__":
    main()
