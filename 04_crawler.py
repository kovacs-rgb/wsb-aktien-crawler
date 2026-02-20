"""
04 - Reddit-Crawler fuer r/wallstreetbets
Durchsucht neue Posts und Kommentare nach Tickersymbolen aus der Pickle-Liste.
Speichert Ergebnisse als Pickle-Datei mit eindeutiger Run-ID.
Speichert zusaetzlich Kontext-Snippets pro Symbol fuer Sentiment-Analyse.

Unterstuetzt zwei Modi:
  - Web-Scraping (Standard): Nutzt oeffentliche Reddit JSON-Endpunkte, kein API-Key noetig
  - API-Modus: Nutzt PRAW mit Reddit API-Credentials aus secret.env (tiefere Crawltiefe)
"""
import json
import os
import pickle
import re
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone

import requests

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
PICKLE_DIR = os.path.join(BASE_DIR, "pickle")
ENV_FILE = os.path.join(BASE_DIR, "secret.env")
SYMBOLS_FILE = os.path.join(DATA_DIR, "symbols_list.pkl")

BLACKLIST = {
    "BE", "GO", "IT", "OR", "SO", "NO", "UP", "FOR", "ON", "BY",
    "AS", "HE", "AM", "AN", "AI", "DD", "OP", "ALL", "YOU", "TV",
    "PM", "HAS", "ARM", "ARE", "PUMP", "EOD", "DAY", "WTF", "HIT",
    "NOW", "AT", "ANY", "CAN", "DO", "HAS", "LOW", "NEW", "OLD",
    "OUT", "RUN", "SEE", "TWO", "WAY", "BIG", "CAR", "FUN", "MAN",
    "NET", "ONE", "OUR", "PAY", "SAY", "TEN", "THE", "TOO", "TOP",
    "USE", "WIN", "OTC",
}

PATTERN_TEMPLATE = r"(?<!\w)(\${symbol}|{symbol})(?!\w)"

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"

# Max Snippet-Laenge pro Kontext-Eintrag
SNIPPET_CHARS = 300
# Max Snippets pro Symbol
MAX_SNIPPETS = 20


def has_api_credentials():
    if not os.path.exists(ENV_FILE):
        return False
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=ENV_FILE)
    client_id = os.getenv("REDDIT_CLIENT_ID")
    return client_id and client_id != "ihre-reddit-client-id"


def extract_snippet(text, symbol, chars=SNIPPET_CHARS):
    """Extrahiert einen Kontext-Snippet rund um das Symbol im Text."""
    pattern = PATTERN_TEMPLATE.format(symbol=re.escape(symbol))
    match = re.search(pattern, text)
    if not match:
        return None
    start = max(0, match.start() - chars // 2)
    end = min(len(text), match.end() + chars // 2)
    snippet = text[start:end].strip()
    # Zeilenumbrueche normalisieren
    snippet = re.sub(r"\s+", " ", snippet)
    return snippet


# ============================================================
# WEB-SCRAPING MODUS
# ============================================================

def fetch_json(url, max_retries=3):
    headers = {"User-Agent": USER_AGENT}
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, headers=headers, timeout=15)
            if resp.status_code == 429:
                wait = 5 * (attempt + 1)
                print(f"    Rate-Limit erreicht, warte {wait}s...")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            if attempt < max_retries - 1:
                time.sleep(2)
            else:
                print(f"    Fehler bei {url}: {e}")
                return None
    return None


def fetch_posts_web(subreddit="wallstreetbets", limit=100):
    posts = []
    after = None
    remaining = limit

    while remaining > 0:
        batch = min(remaining, 25)
        url = f"https://www.reddit.com/r/{subreddit}/new.json?limit={batch}&raw_json=1"
        if after:
            url += f"&after={after}"

        data = fetch_json(url)
        if not data or "data" not in data:
            break

        children = data["data"].get("children", [])
        if not children:
            break

        for child in children:
            post = child["data"]
            posts.append({
                "title": post.get("title", ""),
                "selftext": post.get("selftext", ""),
                "created_utc": post.get("created_utc", 0),
                "permalink": post.get("permalink", ""),
                "id": post.get("id", ""),
                "score": post.get("score", 0),
                "num_comments": post.get("num_comments", 0),
            })

        after = data["data"].get("after")
        remaining -= len(children)
        if not after:
            break
        time.sleep(1.5)

    return posts


def fetch_comments_web(permalink, limit=200):
    """Holt Kommentare als Liste von Einzeltexten."""
    url = f"https://www.reddit.com{permalink}.json?limit={limit}&raw_json=1"
    data = fetch_json(url)
    if not data or len(data) < 2:
        return []

    comments = []

    def extract_comments(listing):
        if not isinstance(listing, dict):
            return
        children = listing.get("data", {}).get("children", [])
        for child in children:
            cdata = child.get("data", {})
            body = cdata.get("body", "")
            if body:
                comments.append(body)
            replies = cdata.get("replies", "")
            if isinstance(replies, dict):
                extract_comments(replies)

    extract_comments(data[1])
    return comments


def crawl_web_scraping(symbols):
    print("Modus: Web-Scraping (kein API-Key noetig)\n")

    cutoff_time = (datetime.now(timezone.utc) - timedelta(days=1)).timestamp()
    symbol_counts = Counter()
    symbol_snippets = defaultdict(list)

    print("Lade Posts von r/wallstreetbets...")
    posts = fetch_posts_web(limit=100)
    print(f"{len(posts)} Posts geladen.\n")

    post_count = 0
    for post in posts:
        if post["created_utc"] < cutoff_time:
            continue

        post_count += 1
        print(f"Post {post_count}: {post['title'][:60]}...")

        post_text = f"{post['title']} {post['selftext']}"

        # Kommentare als einzelne Texte laden
        comment_texts = []
        if post["permalink"]:
            comment_texts = fetch_comments_web(post["permalink"])
            time.sleep(1)

        all_text = post_text + " " + " ".join(comment_texts)

        for symbol in symbols:
            pattern = PATTERN_TEMPLATE.format(symbol=re.escape(symbol))
            matches = len(re.findall(pattern, all_text))
            if matches > 0:
                symbol_counts[symbol] += matches
                print(f"  -> {symbol}: {matches} Treffer")

                # Snippets sammeln (aus Post und Kommentaren)
                if len(symbol_snippets[symbol]) < MAX_SNIPPETS:
                    snip = extract_snippet(post_text, symbol)
                    if snip:
                        symbol_snippets[symbol].append(snip)
                    for ct in comment_texts:
                        if len(symbol_snippets[symbol]) >= MAX_SNIPPETS:
                            break
                        snip = extract_snippet(ct, symbol)
                        if snip:
                            symbol_snippets[symbol].append(snip)

    return post_count, symbol_counts, dict(symbol_snippets)


# ============================================================
# API MODUS
# ============================================================

def crawl_api(symbols):
    import praw
    from dotenv import load_dotenv

    print("Modus: Reddit API (PRAW)\n")

    load_dotenv(dotenv_path=ENV_FILE)
    reddit = praw.Reddit(
        client_id=os.getenv("REDDIT_CLIENT_ID"),
        client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
        user_agent=os.getenv("REDDIT_USER_AGENT"),
    )

    symbol_counts = Counter()
    symbol_snippets = defaultdict(list)
    subreddit = reddit.subreddit("wallstreetbets")
    cutoff_time = datetime.now() - timedelta(days=1)

    post_count = 0
    for post in subreddit.new(limit=100):
        post_time = datetime.fromtimestamp(post.created_utc)
        if post_time < cutoff_time:
            continue

        post_count += 1
        print(f"Post {post_count}: {post.title[:60]}...")

        post_text = f"{post.title} {post.selftext}"
        comment_texts = []

        post.comments.replace_more(limit=30)
        for comment in post.comments.list():
            comment_texts.append(comment.body)

        all_text = post_text + " " + " ".join(comment_texts)

        for symbol in symbols:
            pattern = PATTERN_TEMPLATE.format(symbol=re.escape(symbol))
            matches = len(re.findall(pattern, all_text))
            if matches > 0:
                symbol_counts[symbol] += matches
                print(f"  -> {symbol}: {matches} Treffer")

                if len(symbol_snippets[symbol]) < MAX_SNIPPETS:
                    snip = extract_snippet(post_text, symbol)
                    if snip:
                        symbol_snippets[symbol].append(snip)
                    for ct in comment_texts:
                        if len(symbol_snippets[symbol]) >= MAX_SNIPPETS:
                            break
                        snip = extract_snippet(ct, symbol)
                        if snip:
                            symbol_snippets[symbol].append(snip)

    return post_count, symbol_counts, dict(symbol_snippets)


# ============================================================
# HAUPTFUNKTION
# ============================================================

def reddit_crawler():
    run_id = datetime.now().strftime("%y%m%d-%H%M")
    print("=== Reddit-Crawler gestartet ===")
    print(f"Run-ID: {run_id}")

    if not os.path.exists(SYMBOLS_FILE):
        print(f"\nFEHLER: {SYMBOLS_FILE} nicht gefunden.")
        print("Bitte zuerst 03_create_pickle.py ausfuehren.")
        return

    with open(SYMBOLS_FILE, "rb") as f:
        all_symbols = pickle.load(f)

    symbols = [s for s in all_symbols if s not in BLACKLIST]
    print(f"Suche nach {len(symbols)} Symbolen in r/wallstreetbets...")

    if has_api_credentials():
        post_count, symbol_counts, symbol_snippets = crawl_api(symbols)
    else:
        print("Keine API-Credentials gefunden - verwende Web-Scraping.\n")
        post_count, symbol_counts, symbol_snippets = crawl_web_scraping(symbols)

    print(f"\nSuche abgeschlossen. {post_count} Posts durchsucht.")

    filtered_results = {s: c for s, c in symbol_counts.items() if c > 5}

    if filtered_results:
        os.makedirs(PICKLE_DIR, exist_ok=True)

        # Snippets nur fuer gefilterte Symbole behalten
        filtered_snippets = {s: symbol_snippets.get(s, []) for s in filtered_results}

        result_data = {
            "run_id": run_id,
            "results": filtered_results,
            "snippets": filtered_snippets,
            "total_posts": post_count,
        }

        filename = f"{run_id}_crawler-ergebnis.pkl"
        filepath = os.path.join(PICKLE_DIR, filename)

        with open(filepath, "wb") as f:
            pickle.dump(result_data, f, protocol=pickle.HIGHEST_PROTOCOL)

        print(f"\nErgebnisse gespeichert: {filename}")
        print(f"{len(filtered_results)} Symbole mit >5 Treffern:")
        for symbol, count in sorted(filtered_results.items(), key=lambda x: x[1], reverse=True):
            print(f"  {symbol}: {count} Treffer")
    else:
        print("\nKeine Symbole mit >5 Treffern gefunden.")


if __name__ == "__main__":
    reddit_crawler()
