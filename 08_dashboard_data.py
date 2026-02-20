"""
08 - Dashboard-Daten generieren
Liest die Pickle-Ergebnisse und erstellt eine JSON-Datei fuer das Web-Dashboard.
Berechnet: Momentum-Score, Sentiment-Analyse, Budget-Verteilung.
Laedt ISIN-Daten fuer Flatex-Kauflinks.
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

# Budget in EUR pro Monat
MONTHLY_BUDGET = 200.0

# ============================================================
# ISIN-LOOKUP (fuer Flatex-Kauflinks)
# ============================================================

DATA_DIR = os.path.join(BASE_DIR, "data")


def load_isin_mapping():
    """
    Laedt ISIN-Mapping aus den heruntergeladenen CSV-Dateien.
    NASDAQ-listed.csv hat Spalte: Symbol, ISIN (falls vorhanden)
    Fallback: Bekannte US-Aktien-ISINs als Backup.
    """
    mapping = {}

    # Bekannte ISINs als Fallback (haeufige WSB-Aktien)
    known_isins = {
        "AAPL": "US0378331005", "AMZN": "US0231351067", "AMD": "US0079031078",
        "AVGO": "US11135F1012", "COIN": "US19260Q1076", "CVNA": "US12686C1099",
        "DASH": "US86311W1062", "DELL": "US24703L2025", "DOW": "US2605571031",
        "DPZ": "US25754A2015", "GOOG": "US02079K3059", "GOOGL": "US02079K1079",
        "GME": "US36467W1099", "HIMS": "US43365H1023", "HOOD": "US4385161066",
        "IREN": "US46272L1089", "META": "US30303M1027", "MSFT": "US5949181045",
        "MSTR": "US5949724083", "NFLX": "US64110L1061", "NVDA": "US67066G1040",
        "OKLO": "US6790921038", "OPEN": "US6837121036", "PLTR": "US69608A1088",
        "QQQ": "US46090E1038", "RDDT": "US75734B1008", "RKLB": "US7575441023",
        "SMH": "US92189F6065", "SNOW": "US8334451098", "SOFI": "US83406F1021",
        "SOUN": "US83607X1028", "TSLA": "US88160R1014", "XOM": "US30231G1022",
        "RTX": "US75513E1010", "LMT": "US5398301094", "KTOS": "US5028371042",
        "AMC": "US00165C3025", "CRWV": "US22679L1008", "TNDM": "US87588F1075",
        "PLUG": "US72919P2020", "UNH": "US91324P1021",
    }
    mapping.update(known_isins)

    # Versuche aus NASDAQ CSV zu laden (hat manchmal ISIN-Spalte nicht)
    nasdaq_csv = os.path.join(DATA_DIR, "nasdaq-listed.csv")
    nyse_csv = os.path.join(DATA_DIR, "nyse-listed.csv")

    for csv_path in [nasdaq_csv, nyse_csv]:
        if not os.path.exists(csv_path):
            continue
        try:
            with open(csv_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    symbol = row.get("Symbol") or row.get("ACT Symbol") or ""
                    symbol = symbol.strip()
                    if symbol and symbol not in mapping:
                        # CSV hat keine ISIN-Spalte, aber wir haben Symbol -> Name Mapping
                        pass
        except Exception:
            pass

    return mapping


ISIN_MAPPING = load_isin_mapping()

# ============================================================
# UNTERNEHMENS-BESCHREIBUNGEN (fuer Hover-Tooltips)
# ============================================================

COMPANY_INFO = {
    "AAPL": {"name": "Apple Inc.", "desc": "Technologiekonzern: iPhone, Mac, iPad, Services. Groesster Boersenwert weltweit.", "sector": "Tech"},
    "AMD": {"name": "Advanced Micro Devices", "desc": "Chiphersteller: CPUs und GPUs fuer PCs, Server und Rechenzentren.", "sector": "Halbleiter"},
    "AMC": {"name": "AMC Entertainment", "desc": "Groesste Kinokette der Welt. Meme-Stock seit 2021.", "sector": "Unterhaltung"},
    "AMZN": {"name": "Amazon.com", "desc": "E-Commerce und Cloud-Computing (AWS). Weltweit groesster Online-Haendler.", "sector": "Tech/Handel"},
    "AVGO": {"name": "Broadcom Inc.", "desc": "Halbleiter und Infrastruktur-Software fuer Rechenzentren und Netzwerke.", "sector": "Halbleiter"},
    "COIN": {"name": "Coinbase Global", "desc": "Groesste US-Kryptoboerse. Handel mit Bitcoin, Ethereum und anderen Kryptos.", "sector": "Krypto/Finanzen"},
    "CRWV": {"name": "CrowdStrike (alt: Coreweave)", "desc": "Cloud-Infrastruktur spezialisiert auf GPU-Computing und KI-Workloads.", "sector": "Cloud/KI"},
    "CVNA": {"name": "Carvana Co.", "desc": "Online-Plattform fuer Gebrauchtwagen-Kauf und -Verkauf.", "sector": "E-Commerce/Auto"},
    "DASH": {"name": "DoorDash Inc.", "desc": "Lieferdienst-Plattform fuer Restaurants und Lebensmittel in den USA.", "sector": "Lieferdienst"},
    "DELL": {"name": "Dell Technologies", "desc": "PCs, Server und IT-Infrastruktur. Profitiert von KI-Server-Nachfrage.", "sector": "Tech/Hardware"},
    "DOW": {"name": "Dow Inc.", "desc": "Chemiekonzern: Kunststoffe, Beschichtungen, Industriechemikalien.", "sector": "Chemie"},
    "DPZ": {"name": "Domino's Pizza", "desc": "Weltweit groesste Pizza-Lieferkette mit ueber 20.000 Filialen.", "sector": "Gastronomie"},
    "GME": {"name": "GameStop Corp.", "desc": "Videospiel-Einzelhaendler. Bekanntester Meme-Stock (Short-Squeeze 2021).", "sector": "Einzelhandel"},
    "GOOG": {"name": "Alphabet (Google)", "desc": "Mutterkonzern von Google: Suchmaschine, YouTube, Cloud, KI.", "sector": "Tech"},
    "GOOGL": {"name": "Alphabet Class A", "desc": "Mutterkonzern von Google: Suchmaschine, YouTube, Cloud, KI.", "sector": "Tech"},
    "HIMS": {"name": "Hims & Hers Health", "desc": "Telemedizin-Plattform fuer Gesundheits- und Wellnessprodukte.", "sector": "Gesundheit"},
    "HOOD": {"name": "Robinhood Markets", "desc": "Provisionsfreie Trading-App fuer Aktien, Optionen und Krypto.", "sector": "Finanzen"},
    "IREN": {"name": "Iris Energy", "desc": "Bitcoin-Mining und Rechenzentren mit erneuerbarer Energie.", "sector": "Krypto/Energie"},
    "KTOS": {"name": "Kratos Defense", "desc": "Verteidigungs- und Sicherheitstechnologie: Drohnen, Satelliten, Cybersecurity.", "sector": "Verteidigung"},
    "LMT": {"name": "Lockheed Martin", "desc": "Groesster Ruestungskonzern: F-35, Raketen, Raumfahrt.", "sector": "Verteidigung"},
    "META": {"name": "Meta Platforms", "desc": "Facebook, Instagram, WhatsApp, Oculus VR. Investiert stark in KI.", "sector": "Tech/Social"},
    "MSFT": {"name": "Microsoft Corp.", "desc": "Software-Riese: Windows, Office, Azure Cloud, KI (OpenAI-Partner).", "sector": "Tech"},
    "MSTR": {"name": "MicroStrategy", "desc": "Software-Firma, groesster Bitcoin-Halter unter boersennotierten Unternehmen.", "sector": "Tech/Krypto"},
    "NFLX": {"name": "Netflix Inc.", "desc": "Streaming-Marktfuehrer mit ueber 300 Mio. Abonnenten weltweit.", "sector": "Unterhaltung"},
    "NVDA": {"name": "NVIDIA Corp.", "desc": "Marktfuehrer fuer KI-Chips (GPUs). Treiber der KI-Revolution.", "sector": "Halbleiter/KI"},
    "OKLO": {"name": "Oklo Inc.", "desc": "Entwickler von kleinen modularen Kernreaktoren (SMR) fuer saubere Energie.", "sector": "Energie/Nuklear"},
    "OPEN": {"name": "Opendoor Technologies", "desc": "iBuyer-Plattform: Kauft und verkauft Wohnimmobilien online.", "sector": "Immobilien/Tech"},
    "PLTR": {"name": "Palantir Technologies", "desc": "Big-Data-Analyse und KI-Software fuer Regierungen und Unternehmen.", "sector": "Tech/KI"},
    "PLUG": {"name": "Plug Power", "desc": "Wasserstoff-Brennstoffzellen fuer Logistik und Industrie.", "sector": "Energie/Wasserstoff"},
    "QQQ": {"name": "Invesco QQQ Trust", "desc": "ETF auf den Nasdaq-100 Index. Top-100 Tech-Aktien der USA.", "sector": "ETF/Tech"},
    "RDDT": {"name": "Reddit Inc.", "desc": "Social-Media-Plattform mit Forum-Communities (Subreddits). IPO 2024.", "sector": "Tech/Social"},
    "RKLB": {"name": "Rocket Lab USA", "desc": "Raketenstartunternehmen fuer Kleinsatelliten. Konkurrent von SpaceX.", "sector": "Raumfahrt"},
    "RTX": {"name": "RTX Corp. (Raytheon)", "desc": "Ruestungs- und Luftfahrtkonzern: Raketen, Radar, Triebwerke.", "sector": "Verteidigung"},
    "SMH": {"name": "VanEck Semiconductor ETF", "desc": "ETF auf die groessten Halbleiter-Unternehmen weltweit.", "sector": "ETF/Halbleiter"},
    "SNOW": {"name": "Snowflake Inc.", "desc": "Cloud-Datenplattform fuer Data-Warehousing und Analytics.", "sector": "Cloud/Daten"},
    "SOFI": {"name": "SoFi Technologies", "desc": "Digitale Finanzplattform: Kredite, Banking, Investieren.", "sector": "Finanzen"},
    "SOUN": {"name": "SoundHound AI", "desc": "KI-Spracherkennung fuer Autos, Restaurants und IoT-Geraete.", "sector": "KI"},
    "TNDM": {"name": "Tandem Diabetes Care", "desc": "Insulinpumpen und Diabetes-Management-Technologie.", "sector": "Medizintechnik"},
    "TSLA": {"name": "Tesla Inc.", "desc": "Elektroautos, Energiespeicher, Solar. CEO: Elon Musk.", "sector": "Auto/Energie"},
    "UNH": {"name": "UnitedHealth Group", "desc": "Groesster US-Krankenversicherer und Gesundheitsdienstleister.", "sector": "Gesundheit"},
    "XOM": {"name": "Exxon Mobil", "desc": "Groesster boersennotierter Oelkonzern der Welt.", "sector": "Energie/Oel"},
}

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


def analyze_sentiment(snippets):
    """
    Analysiert Sentiment aus Kontext-Snippets.
    Gibt zurueck: score (-1.0 bearish bis +1.0 bullish), label, details
    """
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
    if total_sentiment == 0:
        score = 0
    else:
        score = round((bullish_count - bearish_count) / total_sentiment, 2)

    if score > 0.2:
        label = "bullish"
    elif score < -0.2:
        label = "bearish"
    else:
        label = "neutral"

    return {
        "score": score,
        "label": label,
        "bullish": bullish_count,
        "bearish": bearish_count,
        "total_words": total_words,
    }


# ============================================================
# MOMENTUM-SCORE
# ============================================================

def calculate_momentum(symbol, history, latest_count, all_runs_count):
    """
    Berechnet einen Momentum-Score (0-100) basierend auf:
    - Aktuelle Erwaechnungen (Gewicht 40%)
    - Wachstumsrate vs. vorheriger Run (Gewicht 30%)
    - Konsistenz: in wie vielen Runs aufgetaucht (Gewicht 15%)
    - Sentiment-Bonus (Gewicht 15%)
    """
    counts = [h["count"] for h in history]
    max_count_ever = max(counts) if counts else 1

    # 1. Popularitaets-Score (0-40): Wie hoch sind aktuelle Erwaechnungen
    pop_score = min(40, (latest_count / max(max_count_ever, 1)) * 40)

    # 2. Wachstums-Score (0-30): Trend gegenueber vorherigem Run
    if len(counts) >= 2:
        prev = counts[-2]
        if prev > 0:
            growth_ratio = latest_count / prev
        else:
            growth_ratio = 2.0  # Neu aufgetaucht
        # Logarithmische Skalierung, 1.0 = neutral, >1 = wachsend
        growth_score = min(30, max(0, (math.log2(max(growth_ratio, 0.1)) + 1) * 15))
    else:
        growth_score = 15  # Erster Run, neutral

    # 3. Konsistenz-Score (0-15): In wie vielen Runs aufgetaucht
    if all_runs_count > 1:
        consistency = len(counts) / all_runs_count
        consistency_score = consistency * 15
    else:
        consistency_score = 7.5

    # 4. Placeholder fuer Sentiment (wird spaeter addiert)
    # Sentiment-Score (0-15) wird separat berechnet

    base_score = pop_score + growth_score + consistency_score
    return round(min(85, base_score), 1)  # Max 85 ohne Sentiment


def add_sentiment_to_momentum(base_momentum, sentiment_score):
    """Fuegt den Sentiment-Anteil (0-15) zum Momentum-Score hinzu."""
    # Sentiment von -1..+1 auf 0..15 mappen
    sentiment_bonus = (sentiment_score + 1) / 2 * 15
    total = base_momentum + sentiment_bonus
    return round(min(100, max(0, total)), 1)


# ============================================================
# BUDGET-VERTEILUNG
# ============================================================

def calculate_budget(top_symbols, budget=MONTHLY_BUDGET):
    """
    Verteilt Budget gewichtet nach Momentum-Score.
    Nur Symbole mit Score > 40 bekommen Anteil.
    """
    eligible = [(s, s["momentum_score"]) for s in top_symbols if s["momentum_score"] > 40]

    if not eligible:
        return []

    total_score = sum(score for _, score in eligible)

    result = []
    for sym_data, score in eligible:
        weight = score / total_score
        amount = round(budget * weight, 2)
        symbol = sym_data["symbol"]
        isin = ISIN_MAPPING.get(symbol, "")
        entry = {
            "symbol": symbol,
            "amount_eur": amount,
            "weight_pct": round(weight * 100, 1),
            "isin": isin,
        }
        if isin:
            entry["flatex_url"] = f"https://www.flatex.de/suche?q={isin}"
        result.append(entry)

    return result


# ============================================================
# KURSDATEN (Yahoo Finance)
# ============================================================

def fetch_stock_prices(symbols, period="1mo"):
    """
    Laedt historische Kursdaten fuer die angegebenen Symbole via yfinance.
    Gibt dict zurueck: {symbol: [{date: "2026-02-01", close: 123.45}, ...]}
    """
    try:
        import yfinance as yf
    except ImportError:
        print("  yfinance nicht installiert - ueberspringe Kursdaten")
        return {}

    if not symbols:
        return {}

    print(f"  Lade Kursdaten fuer {len(symbols)} Symbole...")
    prices = {}

    try:
        data = yf.download(symbols, period=period, interval="1d", progress=False)
        if data.empty:
            return {}

        for symbol in symbols:
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
                    prices[symbol] = price_list
                    print(f"    {symbol}: {len(price_list)} Datenpunkte, letzter Kurs ${price_list[-1]['close']}")
            except Exception as e:
                print(f"    {symbol}: Fehler - {e}")
    except Exception as e:
        print(f"  Fehler beim Laden der Kursdaten: {e}")

    return prices


# ============================================================
# HAUPT-LOGIK
# ============================================================

def read_pickle_files(directory):
    data_list = []
    for filename in sorted(os.listdir(directory)):
        if filename.endswith(".pkl") or filename.endswith(".pickle"):
            filepath = os.path.join(directory, filename)
            try:
                with open(filepath, "rb") as f:
                    data = pickle.load(f)
                data_list.append(data)
            except Exception as e:
                print(f"  FEHLER bei {filename}: {e}")
    return data_list


def build_dashboard_data(data_list):
    if not data_list:
        return {"runs": [], "top5": [], "all_symbols": {}, "generated": "", "budget": []}

    runs = []
    all_symbol_history = {}

    # Snippets aus dem letzten Run sammeln
    latest_snippets = {}

    for entry in sorted(data_list, key=lambda x: x["run_id"]):
        run = {
            "id": entry["run_id"],
            "total_posts": entry.get("total_posts", 0),
            "results": entry["results"],
        }
        runs.append(run)

        for symbol, count in entry["results"].items():
            if symbol not in all_symbol_history:
                all_symbol_history[symbol] = []
            all_symbol_history[symbol].append({"run": entry["run_id"], "count": count})

        # Snippets des letzten Runs uebernehmen
        if "snippets" in entry:
            latest_snippets = entry["snippets"]

    latest_run = runs[-1] if runs else None
    total_runs = len(runs)

    # Top-Symbole nach Treffern sortiert
    top_symbols = []
    if latest_run:
        sorted_symbols = sorted(
            latest_run["results"].items(), key=lambda x: x[1], reverse=True
        )

        for symbol, count in sorted_symbols:
            history = all_symbol_history.get(symbol, [])

            # Trend berechnen
            if len(history) >= 2:
                prev_count = history[-2]["count"]
                if prev_count > 0:
                    trend_pct = round(((count - prev_count) / prev_count) * 100, 1)
                else:
                    trend_pct = 100.0
            else:
                trend_pct = 0.0

            # Sentiment analysieren
            snippets = latest_snippets.get(symbol, [])
            sentiment = analyze_sentiment(snippets)

            # Momentum-Score berechnen
            base_momentum = calculate_momentum(symbol, history, count, total_runs)
            momentum = add_sentiment_to_momentum(base_momentum, sentiment["score"])

            company = COMPANY_INFO.get(symbol, {})
            top_symbols.append({
                "symbol": symbol,
                "count": count,
                "trend_pct": trend_pct,
                "history": history,
                "sentiment": sentiment,
                "momentum_score": momentum,
                "company_name": company.get("name", symbol),
                "company_desc": company.get("desc", ""),
                "sector": company.get("sector", ""),
            })

    # Nach Momentum-Score sortieren
    top_symbols.sort(key=lambda x: x["momentum_score"], reverse=True)

    # Budget-Verteilung berechnen (Top 5)
    budget_split = calculate_budget(top_symbols[:5])

    # Top 5 fuer Dashboard
    top5 = top_symbols[:5]

    # Kursdaten fuer alle gerankten Symbole laden
    all_ranked_symbols = [s["symbol"] for s in top_symbols]
    stock_prices = fetch_stock_prices(all_ranked_symbols, period="1mo")

    # Kursdaten zu top5 hinzufuegen
    for item in top5:
        item["prices"] = stock_prices.get(item["symbol"], [])

    return {
        "runs": runs,
        "top5": top5,
        "all_symbols": all_symbol_history,
        "latest_run": latest_run["id"] if latest_run else None,
        "total_runs": total_runs,
        "generated": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "budget": {
            "monthly_eur": MONTHLY_BUDGET,
            "allocation": budget_split,
        },
        "stock_prices": stock_prices,
        "all_ranked": [
            {
                "symbol": s["symbol"],
                "count": s["count"],
                "momentum_score": s["momentum_score"],
                "sentiment": s["sentiment"]["label"],
                "sentiment_score": s["sentiment"]["score"],
                "isin": ISIN_MAPPING.get(s["symbol"], ""),
                "company_name": s.get("company_name", s["symbol"]),
                "company_desc": s.get("company_desc", ""),
                "sector": s.get("sector", ""),
            }
            for s in top_symbols
        ],
    }


def main():
    print("=== Dashboard-Daten generieren ===")

    os.makedirs(DASHBOARD_DIR, exist_ok=True)

    if not os.path.exists(PICKLE_DIR):
        print("Keine Pickle-Daten vorhanden. Erstelle Demo-Daten...")
        create_demo_data()

    data_list = read_pickle_files(PICKLE_DIR)

    if not data_list:
        print("Keine Pickle-Dateien gefunden. Erstelle Demo-Daten...")
        create_demo_data()
        data_list = read_pickle_files(PICKLE_DIR)

    dashboard_data = build_dashboard_data(data_list)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(dashboard_data, f, ensure_ascii=False, indent=2)

    print(f"Dashboard-Daten gespeichert: {OUTPUT_FILE}")
    print(f"  Runs: {dashboard_data['total_runs']}")
    print(f"  Top 5: {[t['symbol'] for t in dashboard_data['top5']]}")

    if dashboard_data.get("budget", {}).get("allocation"):
        print(f"\n  Budget-Verteilung ({MONTHLY_BUDGET} EUR):")
        for a in dashboard_data["budget"]["allocation"]:
            print(f"    {a['symbol']}: {a['amount_eur']} EUR ({a['weight_pct']}%)")

    if dashboard_data.get("all_ranked"):
        print(f"\n  Momentum-Ranking:")
        for s in dashboard_data["all_ranked"][:5]:
            print(f"    {s['symbol']}: Score {s['momentum_score']} | Sentiment: {s['sentiment']} ({s['sentiment_score']})")


def create_demo_data():
    import random
    os.makedirs(PICKLE_DIR, exist_ok=True)

    demo_symbols = {
        "TSLA": (15, 35), "NVDA": (10, 30), "PLTR": (8, 25),
        "AAPL": (5, 15), "GOOG": (5, 12), "ASTS": (3, 20),
        "AMD": (5, 18), "GME": (4, 12), "AMZN": (3, 10),
        "META": (3, 8), "SOFI": (2, 15), "MSTR": (2, 12),
    }

    demo_snippets = {
        "TSLA": ["TSLA to the moon, buying more calls", "TSLA looks bullish after earnings"],
        "NVDA": ["NVDA is undervalued at this price, loading up", "NVDA calls printing money"],
        "PLTR": ["PLTR long term hold, diamond hands", "PLTR contract news is bullish"],
        "AAPL": ["AAPL might drop after keynote", "AAPL puts looking good"],
        "GOOG": ["GOOG is strong, buying the dip"],
    }

    for day_offset in range(7):
        run_id = f"2502{13 + day_offset:02d}-0700"
        results = {}
        for symbol, (low, high) in demo_symbols.items():
            val = random.randint(low, high)
            if symbol in ("PLTR", "ASTS", "NVDA") and day_offset > 3:
                val = int(val * 1.5)
            if val > 5:
                results[symbol] = val

        snippets = {s: demo_snippets.get(s, []) for s in results}
        data = {
            "run_id": run_id,
            "results": results,
            "snippets": snippets,
            "total_posts": random.randint(40, 70),
        }
        filepath = os.path.join(PICKLE_DIR, f"{run_id}_crawler-ergebnis.pkl")
        with open(filepath, "wb") as f:
            pickle.dump(data, f)
        print(f"  Demo-Datei erstellt: {run_id}")


if __name__ == "__main__":
    main()
