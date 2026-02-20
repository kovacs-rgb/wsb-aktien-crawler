"""
01 - Ticker-Daten herunterladen
Laedt die Tickersymbol-Listen von NASDAQ und NYSE als CSV-Dateien herunter.
Quelle: Datahub (GitHub)
"""
import os
import requests

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")

URLS = {
    "nasdaq-listed.csv": "https://raw.githubusercontent.com/datasets/nasdaq-listings/master/data/nasdaq-listed.csv",
    "nyse-listed.csv": "https://raw.githubusercontent.com/datasets/nyse-other-listings/master/data/nyse-listed.csv",
}


def download_file(url, filepath):
    print(f"Lade herunter: {url}")
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    with open(filepath, "wb") as f:
        f.write(resp.content)
    print(f"  Gespeichert: {filepath}")


def main():
    os.makedirs(DATA_DIR, exist_ok=True)

    for filename, url in URLS.items():
        filepath = os.path.join(DATA_DIR, filename)
        try:
            download_file(url, filepath)
        except Exception as e:
            print(f"  FEHLER beim Download von {filename}: {e}")

    print("\nDownload abgeschlossen.")
    print(f"Dateien liegen in: {DATA_DIR}")


if __name__ == "__main__":
    main()
