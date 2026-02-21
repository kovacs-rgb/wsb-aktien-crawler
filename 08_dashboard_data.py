"""
08 - Dashboard-Daten generieren (Multi-Kategorie)
Liest Pickle-Ergebnisse und erstellt JSON fuer das Web-Dashboard.
Drei Kategorien: WSB Top-5, Meme-Aktien, Multi-Bagger.
Berechnet: Momentum-Score, Meme-Score, Multi-Bagger-Score,
           Sentiment-Analyse, Vervielfachungspotenzial, Budget-Verteilung.
Laedt Kursdaten (1W, 1M, 1J) und ISIN-Daten fuer Flatex-Kauflinks.
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

MONTHLY_BUDGET = 200.0

# ============================================================
# ISIN-LOOKUP (fuer Flatex-Kauflinks)
# ============================================================

def load_isin_mapping():
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
        # Meme-Aktien
        "FFIE": "US30249U1016", "MULN": "US62526P2074", "TTOO": "US87285P1003",
        "FNGR": "US31810F1012", "GNS": "US36241M1027",
        # Multi-Bagger
        "SEZL": "US81733D1063", "APP": "US03835T1034", "DAVE": "US23830C1062",
        "CRDO": "US22612U1088", "MRVL": "US5738741041", "HUT": "CA44812W1068",
        "SPOT": "LU1778762911", "SE": "US81141R1005", "AS": "US02351X1028",
        "LLY": "US5324571083", "NEE": "US65339F1012",
    }
    return known_isins

ISIN_MAPPING = load_isin_mapping()

# ============================================================
# UNTERNEHMENS-BESCHREIBUNGEN
# ============================================================

COMPANY_INFO = {
    "AAPL": {"name": "Apple Inc.", "desc": "Technologiekonzern: iPhone, Mac, iPad, Services.", "sector": "Tech"},
    "AMD": {"name": "Advanced Micro Devices", "desc": "Chiphersteller: CPUs und GPUs fuer PCs, Server und Rechenzentren.", "sector": "Halbleiter"},
    "AMC": {"name": "AMC Entertainment", "desc": "Groesste Kinokette der Welt. Meme-Stock seit 2021.", "sector": "Unterhaltung"},
    "AMZN": {"name": "Amazon.com", "desc": "E-Commerce und Cloud-Computing (AWS).", "sector": "Tech/Handel"},
    "APP": {"name": "AppLovin Corp.", "desc": "Mobile Advertising und App-Monetarisierung. Starkes Umsatzwachstum.", "sector": "Tech/Werbung"},
    "AS": {"name": "Amer Sports", "desc": "Sportartikelkonzern: Salomon, Arc'teryx, Wilson.", "sector": "Sport/Konsum"},
    "AVGO": {"name": "Broadcom Inc.", "desc": "Halbleiter und Infrastruktur-Software fuer Rechenzentren.", "sector": "Halbleiter"},
    "COIN": {"name": "Coinbase Global", "desc": "Groesste US-Kryptoboerse.", "sector": "Krypto/Finanzen"},
    "CRDO": {"name": "Credo Technology", "desc": "Halbleiter fuer Hochgeschwindigkeits-Datenverbindungen in KI-Rechenzentren.", "sector": "Halbleiter/KI"},
    "CRWV": {"name": "Coreweave", "desc": "Cloud-Infrastruktur spezialisiert auf GPU-Computing.", "sector": "Cloud/KI"},
    "CVNA": {"name": "Carvana Co.", "desc": "Online-Plattform fuer Gebrauchtwagen-Kauf und -Verkauf.", "sector": "E-Commerce/Auto"},
    "DASH": {"name": "DoorDash Inc.", "desc": "Lieferdienst-Plattform fuer Restaurants.", "sector": "Lieferdienst"},
    "DAVE": {"name": "Dave Inc.", "desc": "Fintech-App: Banking, Vorschuesse, Budgetplanung.", "sector": "Fintech"},
    "DELL": {"name": "Dell Technologies", "desc": "PCs, Server und IT-Infrastruktur.", "sector": "Tech/Hardware"},
    "DOW": {"name": "Dow Inc.", "desc": "Chemiekonzern: Kunststoffe, Beschichtungen.", "sector": "Chemie"},
    "DPZ": {"name": "Domino's Pizza", "desc": "Groesste Pizza-Lieferkette mit 20.000+ Filialen.", "sector": "Gastronomie"},
    "FFIE": {"name": "Faraday Future", "desc": "Elektroauto-Startup. Extrem spekulativ, Meme-Legende.", "sector": "E-Auto/Meme"},
    "FNGR": {"name": "FingerMotion Inc.", "desc": "Mobile Daten-Services in China. Hochvolatil.", "sector": "Mobile/Daten"},
    "GME": {"name": "GameStop Corp.", "desc": "Videospiel-Einzelhaendler. Bekanntester Meme-Stock.", "sector": "Einzelhandel/Meme"},
    "GNS": {"name": "Genius Group", "desc": "Bildungsplattform mit Blockchain-Fokus.", "sector": "Bildung/Blockchain"},
    "GOOG": {"name": "Alphabet (Google)", "desc": "Google, YouTube, Cloud, KI.", "sector": "Tech"},
    "GOOGL": {"name": "Alphabet Class A", "desc": "Google, YouTube, Cloud, KI.", "sector": "Tech"},
    "HIMS": {"name": "Hims & Hers Health", "desc": "Telemedizin-Plattform.", "sector": "Gesundheit"},
    "HOOD": {"name": "Robinhood Markets", "desc": "Provisionsfreie Trading-App.", "sector": "Finanzen"},
    "HUT": {"name": "Hut 8 Corp.", "desc": "Bitcoin-Mining und KI-Rechenzentren.", "sector": "Krypto/Mining"},
    "IREN": {"name": "Iris Energy", "desc": "Bitcoin-Mining mit erneuerbarer Energie.", "sector": "Krypto/Energie"},
    "KTOS": {"name": "Kratos Defense", "desc": "Drohnen, Satelliten, Cybersecurity.", "sector": "Verteidigung"},
    "LLY": {"name": "Eli Lilly", "desc": "Pharmakonzern: Diabetes, Adipositas (Mounjaro).", "sector": "Pharma"},
    "LMT": {"name": "Lockheed Martin", "desc": "Groesster Ruestungskonzern.", "sector": "Verteidigung"},
    "META": {"name": "Meta Platforms", "desc": "Facebook, Instagram, WhatsApp.", "sector": "Tech/Social"},
    "MRVL": {"name": "Marvell Technology", "desc": "Custom AI-Chips und Datenzentrums-Infrastruktur.", "sector": "Halbleiter/KI"},
    "MSFT": {"name": "Microsoft Corp.", "desc": "Windows, Office, Azure Cloud, KI.", "sector": "Tech"},
    "MSTR": {"name": "MicroStrategy", "desc": "Groesster Bitcoin-Halter unter Unternehmen.", "sector": "Tech/Krypto"},
    "MULN": {"name": "Mullen Automotive", "desc": "E-Auto-Startup. Starke Meme-Dynamik, Short-Squeeze Kandidat.", "sector": "E-Auto/Meme"},
    "NEE": {"name": "NextEra Energy", "desc": "Groesster Erzeuger von Wind- und Solarenergie.", "sector": "Energie/Gruen"},
    "NFLX": {"name": "Netflix Inc.", "desc": "Streaming-Marktfuehrer, 300+ Mio. Abonnenten.", "sector": "Unterhaltung"},
    "NVDA": {"name": "NVIDIA Corp.", "desc": "Marktfuehrer fuer KI-Chips (GPUs).", "sector": "Halbleiter/KI"},
    "OKLO": {"name": "Oklo Inc.", "desc": "Kleine modulare Kernreaktoren (SMR).", "sector": "Energie/Nuklear"},
    "OPEN": {"name": "Opendoor Technologies", "desc": "iBuyer fuer Wohnimmobilien.", "sector": "Immobilien/Tech"},
    "PLTR": {"name": "Palantir Technologies", "desc": "Big-Data und KI-Software fuer Regierungen/Unternehmen.", "sector": "Tech/KI"},
    "PLUG": {"name": "Plug Power", "desc": "Wasserstoff-Brennstoffzellen.", "sector": "Energie/Wasserstoff"},
    "QQQ": {"name": "Invesco QQQ Trust", "desc": "ETF auf Nasdaq-100.", "sector": "ETF/Tech"},
    "RDDT": {"name": "Reddit Inc.", "desc": "Social-Media-Plattform mit Forum-Communities.", "sector": "Tech/Social"},
    "RKLB": {"name": "Rocket Lab USA", "desc": "Raketenstartunternehmen fuer Kleinsatelliten.", "sector": "Raumfahrt"},
    "RTX": {"name": "RTX Corp.", "desc": "Ruestungs- und Luftfahrtkonzern.", "sector": "Verteidigung"},
    "SE": {"name": "Sea Ltd", "desc": "E-Commerce (Shopee), Gaming (Garena), Fintech.", "sector": "Tech/E-Commerce"},
    "SEZL": {"name": "Sezzle Inc.", "desc": "Buy Now Pay Later Fintech. 800%+ Wachstum.", "sector": "Fintech/BNPL"},
    "SMH": {"name": "VanEck Semiconductor ETF", "desc": "ETF auf Halbleiter-Unternehmen.", "sector": "ETF/Halbleiter"},
    "SNOW": {"name": "Snowflake Inc.", "desc": "Cloud-Datenplattform.", "sector": "Cloud/Daten"},
    "SOFI": {"name": "SoFi Technologies", "desc": "Digitale Finanzplattform.", "sector": "Finanzen"},
    "SOUN": {"name": "SoundHound AI", "desc": "KI-Spracherkennung fuer Autos und IoT.", "sector": "KI"},
    "SPOT": {"name": "Spotify Technology", "desc": "Musik-Streaming-Marktfuehrer.", "sector": "Unterhaltung/Tech"},
    "TNDM": {"name": "Tandem Diabetes Care", "desc": "Insulinpumpen.", "sector": "Medizintechnik"},
    "TSLA": {"name": "Tesla Inc.", "desc": "Elektroautos, Energiespeicher, Solar.", "sector": "Auto/Energie"},
    "TTOO": {"name": "T2 Biosystems", "desc": "Biotech: Schnelldiagnose fuer Infektionen. Pennystock.", "sector": "Biotech/Meme"},
    "UNH": {"name": "UnitedHealth Group", "desc": "Groesster US-Krankenversicherer.", "sector": "Gesundheit"},
    "XOM": {"name": "Exxon Mobil", "desc": "Groesster Oelkonzern der Welt.", "sector": "Energie/Oel"},
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
    """
    Extrahiert Vervielfachungspotenzial aus Reddit-Snippets.
    Sucht nach: "10x", "100x", "+500%", "5-bagger", etc.
    Gibt Durchschnitts-Potenzial in % zurueck oder None.
    """
    if not snippets:
        return None

    potentials = []
    all_text = " ".join(snippets).lower()

    # Pattern: Nx (z.B. 10x, 100x, 5x)
    for m in re.finditer(r"\b(\d+)\s*x\b", all_text):
        try:
            mult = int(m.group(1))
            if 2 <= mult <= 10000:
                potentials.append(mult * 100)  # 10x = 1000%
        except ValueError:
            pass

    # Pattern: +NNN% (z.B. +500%, +1000%)
    for m in re.finditer(r"[+]?\s*(\d{2,5})\s*%", all_text):
        try:
            pct = int(m.group(1))
            if 50 <= pct <= 100000:
                potentials.append(pct)
        except ValueError:
            pass

    # Pattern: N-bagger (z.B. ten-bagger, 5-bagger)
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
    """Momentum-Score (0-100) fuer WSB Top-5."""
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
    """
    Meme-Score (0-100): belohnt EXPLOSIVE, ploetzliche Aufmerksamkeit.
    - Squeeze-Signal (30%): Short-Squeeze Keywords
    - Hype-Intensitaet (25%): YOLO, diamond hands, ape
    - Velocity (25%): Spike-Erkennung
    - Community-Ueberzeugung (20%): Bullish/Bearish Ratio
    """
    counts = [h["count"] for h in history]
    total_words = 0
    all_text_lower = ""
    for snippet in snippets:
        words = snippet.lower().split()
        total_words += len(words)
        all_text_lower += " " + snippet.lower()

    total_words = max(total_words, 1)

    # 1. SQUEEZE SIGNAL (0-30)
    squeeze_hits = sum(all_text_lower.count(kw) for kw in SQUEEZE_WORDS)
    squeeze_density = (squeeze_hits / total_words) * 100
    squeeze_score = min(30, squeeze_density * 15)

    # 2. HYPE INTENSITY (0-25)
    hype_hits = sum(all_text_lower.count(kw) for kw in MEME_HYPE_WORDS)
    hype_density = (hype_hits / total_words) * 100
    hype_score = min(25, hype_density * 12)

    # 3. VELOCITY (0-25) - Spike detection
    if len(counts) >= 2:
        prev = counts[-2]
        velocity_ratio = latest_count / prev if prev > 0 else 5.0
        velocity_score = min(25, max(0, math.log2(max(velocity_ratio, 0.1)) * 12.5))
    else:
        velocity_score = 20  # Erster Auftritt = interessant fuer Memes

    # 4. CONVICTION (0-20)
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
    """
    Multi-Bagger Score (0-100): belohnt Wachstum + Vervielfachungspotenzial.
    - Growth-These (30%): Revenue growth, TAM, moat
    - Multiplier-Sprache (25%): 10x, 100x, tenbagger
    - Community-Ueberzeugung (25%): Konsistenz + DD
    - Squeeze/Katalysator (20%): catalyst, FDA, partnership
    """
    counts = [h["count"] for h in history]
    total_words = 0
    all_text_lower = ""
    for snippet in snippets:
        total_words += len(snippet.lower().split())
        all_text_lower += " " + snippet.lower()

    total_words = max(total_words, 1)

    # 1. GROWTH THESIS (0-30)
    growth_hits = sum(all_text_lower.count(kw) for kw in GROWTH_WORDS)
    growth_density = (growth_hits / total_words) * 100
    growth_score = min(30, growth_density * 12)

    # 2. MULTIPLIER LANGUAGE (0-25)
    multi_hits = sum(all_text_lower.count(kw) for kw in MULTIBAGGER_WORDS)
    multi_density = (multi_hits / total_words) * 100
    multi_score = min(25, multi_density * 15)

    # 3. COMMUNITY CONVICTION (0-25)
    if all_runs_count > 1:
        consistency_base = (len(counts) / all_runs_count) * 10
    else:
        consistency_base = 5

    conviction_hits = sum(all_text_lower.count(kw) for kw in CONVICTION_WORDS)
    conviction_density = (conviction_hits / total_words) * 100
    conviction_language = min(15, conviction_density * 10)
    conviction_score = min(25, consistency_base + conviction_language)

    # 4. SQUEEZE/CATALYST (0-20)
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
    """Verteilt Budget gewichtet nach Score. Nur Symbole mit Score > 30 bekommen Anteil."""
    eligible = [(s, s.get(score_key, 0)) for s in top_symbols if s.get(score_key, 0) > 30]
    if not eligible:
        # Fallback: alle bekommen gleichen Anteil
        eligible = [(s, 50) for s in top_symbols]

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
# KURSDATEN (Yahoo Finance) - 3 Zeitraeume
# ============================================================

def fetch_stock_prices_multi(symbols):
    """
    Laedt Kursdaten fuer 3 Zeitraeume: 5d (1W), 1mo, 1y.
    Gibt dict zurueck: {symbol: {week: [...], month: [...], year: [...]}}
    """
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
    """Liest Pickle-Dateien und gruppiert nach Kategorie."""
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
                # Legacy: Dateiname pruefen
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

def build_category_data(data_list, cat_key, score_fn=None, budget=MONTHLY_BUDGET):
    """Baut Dashboard-Daten fuer eine einzelne Kategorie."""
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

            # Trend
            if len(counts) >= 2:
                prev = counts[-2]
                trend_pct = round(((count - prev) / prev) * 100, 1) if prev > 0 else 100.0
            else:
                trend_pct = 0.0

            snippets = latest_snippets.get(symbol, [])
            sentiment = analyze_sentiment(snippets)
            potential = extract_potential(snippets)

            # Score berechnen
            if score_fn:
                score = score_fn(symbol, history, count, total_runs, snippets)
                score_key = "meme_score" if cat_key == "meme" else "multibagger_score"
            else:
                base = calculate_momentum(symbol, history, count, total_runs)
                score = add_sentiment_to_momentum(base, sentiment["score"])
                score_key = "momentum_score"

            company = COMPANY_INFO.get(symbol, {})
            entry = {
                "symbol": symbol,
                "count": count,
                "trend_pct": trend_pct,
                "history": history,
                "sentiment": sentiment,
                score_key: score,
                "momentum_score": score,  # Alias fuer Budget-Berechnung
                "potential_pct": potential,
                "company_name": company.get("name", symbol),
                "company_desc": company.get("desc", ""),
                "sector": company.get("sector", ""),
            }
            top_symbols.append(entry)

    top_symbols.sort(key=lambda x: x["momentum_score"], reverse=True)

    # Score-Key fuer Budget
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
                "isin": ISIN_MAPPING.get(s["symbol"], ""),
                "company_name": s.get("company_name", s["symbol"]),
                "company_desc": s.get("company_desc", ""),
                "sector": s.get("sector", ""),
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
    """Baut Dashboard-Daten fuer alle drei Kategorien."""
    default_split = {"wsb": 50, "meme": 25, "multibagger": 25}

    result = {
        "generated": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "categories": {},
        "budget": {
            "monthly_eur": MONTHLY_BUDGET,
            "split": default_split,
        },
    }

    # Score-Funktionen pro Kategorie
    score_fns = {
        "wsb": None,  # Standard Momentum
        "meme": calculate_meme_score,
        "multibagger": calculate_multibagger_score,
    }

    cat_labels = {
        "wsb": {"label": "WSB Top-5", "label_short": "WSB", "color_scheme": "blue-purple", "score_type": "momentum_score"},
        "meme": {"label": "Meme-Aktien", "label_short": "Meme", "color_scheme": "red-orange", "score_type": "meme_score"},
        "multibagger": {"label": "Multi-Bagger", "label_short": "Multi", "color_scheme": "green-gold", "score_type": "multibagger_score"},
    }

    all_stock_symbols = set()

    for cat_key, data_list in categories_data.items():
        cat_budget = MONTHLY_BUDGET * default_split.get(cat_key, 0) / 100
        cat_result = build_category_data(
            data_list, cat_key,
            score_fn=score_fns.get(cat_key),
            budget=cat_budget,
        )
        cat_result.update(cat_labels.get(cat_key, {}))
        cat_result["budget"]["split_pct"] = default_split.get(cat_key, 0)
        result["categories"][cat_key] = cat_result

        for s in cat_result.get("top5", []):
            all_stock_symbols.add(s["symbol"])
        for s in cat_result.get("all_ranked", []):
            all_stock_symbols.add(s["symbol"])

    # Kursdaten fuer ALLE Symbole (ein Aufruf, 3 Zeitraeume)
    stock_prices = fetch_stock_prices_multi(list(all_stock_symbols))
    result["stock_prices"] = stock_prices

    # Kursdaten an top5 anhaengen
    for cat_key in result["categories"]:
        for item in result["categories"][cat_key].get("top5", []):
            item["prices"] = stock_prices.get(item["symbol"], {})

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
    print("=== Dashboard-Daten generieren (Multi-Kategorie) ===")

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
                print(f"    {s['symbol']}: Score {s['momentum_score']} | {s['sentiment']['label']}{pot}")


def create_demo_data():
    import random
    os.makedirs(PICKLE_DIR, exist_ok=True)

    # WSB Demo
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

    # Meme Demo
    meme_symbols = {
        "GME": (8, 25), "AMC": (6, 20), "FFIE": (4, 18),
        "MULN": (3, 15), "TTOO": (2, 12), "FNGR": (2, 10),
    }
    meme_snippets = {
        "GME": ["GME short squeeze gamma squeeze incoming diamond hands apes", "GME low float high short interest YOLO all in"],
        "FFIE": ["FFIE short squeeze potential 100x lottery ticket moonshot", "FFIE cost to borrow through the roof ortex data"],
        "MULN": ["MULN squeeze play diamond hands apes strong", "MULN low float naked short failure to deliver"],
    }

    # Multi-Bagger Demo
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
