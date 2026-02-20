"""
02 - Ticker-Daten filtern und zusammenfuehren
Liest die heruntergeladenen CSV-Dateien, filtert ETFs und Duplikate heraus
und erstellt eine bereinigte Gesamtliste aller Tickersymbole.
"""
import os
import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")

NASDAQ_CSV = os.path.join(DATA_DIR, "nasdaq-listed.csv")
NYSE_CSV = os.path.join(DATA_DIR, "nyse-listed.csv")
OUTPUT_FILE = os.path.join(DATA_DIR, "NAS-NYSE-bereinigt.xlsx")


def process_nasdaq(filepath):
    """NASDAQ-Liste laden, ETFs filtern, Duplikate entfernen."""
    print("Verarbeite NASDAQ-Daten...")
    df = pd.read_csv(filepath)
    print(f"  Gesamteintraege: {len(df)}")

    # ETFs herausfiltern (Spalte 'ETF' == 'N' bedeutet kein ETF)
    if "ETF" in df.columns:
        df = df[df["ETF"] == "N"]
        print(f"  Nach ETF-Filter: {len(df)}")

    # Nur Symbol und Company Name behalten
    symbol_col = "Symbol" if "Symbol" in df.columns else df.columns[0]
    name_col = "Company Name" if "Company Name" in df.columns else df.columns[1]
    result = df[[symbol_col, name_col]].copy()
    result.columns = ["Symbol", "Company Name"]

    # Duplikate entfernen: nur erste Zeile pro Unternehmen behalten
    before = len(result)
    result = result.drop_duplicates(subset="Company Name", keep="first")
    print(f"  Duplikate entfernt: {before - len(result)}")
    print(f"  NASDAQ bereinigt: {len(result)} Eintraege")

    return result


def process_nyse(filepath):
    """NYSE-Liste laden, Duplikate entfernen."""
    print("\nVerarbeite NYSE-Daten...")
    df = pd.read_csv(filepath)
    print(f"  Gesamteintraege: {len(df)}")

    # Nur Symbol und Company Name behalten
    symbol_col = "ACT Symbol" if "ACT Symbol" in df.columns else df.columns[0]
    name_col = "Company Name" if "Company Name" in df.columns else df.columns[1]
    result = df[[symbol_col, name_col]].copy()
    result.columns = ["Symbol", "Company Name"]

    # NYSE: Unternehmensnamen auf erste 2 Woerter kuerzen fuer Duplikat-Erkennung
    result["Name_Short"] = result["Company Name"].apply(
        lambda x: " ".join(str(x).split()[:2]) if len(str(x).split()) > 2 else str(x)
    )

    before = len(result)
    result = result.drop_duplicates(subset="Name_Short", keep="first")
    result = result.drop(columns=["Name_Short"])
    print(f"  Duplikate entfernt: {before - len(result)}")
    print(f"  NYSE bereinigt: {len(result)} Eintraege")

    return result


def add_dollar_prefix(df):
    """Einzel-Buchstaben-Symbole mit $ versehen."""
    single_chars = df["Symbol"].str.len() == 1
    count = single_chars.sum()
    if count > 0:
        df.loc[single_chars, "Symbol"] = "$" + df.loc[single_chars, "Symbol"]
        print(f"\n{count} Einzel-Buchstaben-Symbole mit $ versehen")
    return df


def main():
    if not os.path.exists(NASDAQ_CSV):
        print(f"FEHLER: {NASDAQ_CSV} nicht gefunden.")
        print("Bitte zuerst 01_download_ticker_data.py ausfuehren.")
        return

    if not os.path.exists(NYSE_CSV):
        print(f"FEHLER: {NYSE_CSV} nicht gefunden.")
        print("Bitte zuerst 01_download_ticker_data.py ausfuehren.")
        return

    nasdaq = process_nasdaq(NASDAQ_CSV)
    nyse = process_nyse(NYSE_CSV)

    # Zusammenfuehren
    merged = pd.concat([nasdaq, nyse], ignore_index=True)
    print(f"\nZusammengefuehrt: {len(merged)} Eintraege")

    # Globale Duplikate entfernen (gleiches Symbol in beiden Boersen)
    before = len(merged)
    merged = merged.drop_duplicates(subset="Symbol", keep="first")
    print(f"Doppelte Symbole entfernt: {before - len(merged)}")

    # Einzel-Buchstaben mit $ versehen
    merged = add_dollar_prefix(merged)

    # Speichern
    merged.to_excel(OUTPUT_FILE, index=False, sheet_name="Tabelle1")
    print(f"\nBereinigte Liste gespeichert: {OUTPUT_FILE}")
    print(f"Gesamtzahl Tickersymbole: {len(merged)}")


if __name__ == "__main__":
    main()
