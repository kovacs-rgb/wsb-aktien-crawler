"""
03 - Pickle-Datei erstellen
Liest die bereinigte Excel-Datei und speichert die Tickersymbole als Pickle-Liste.
"""
import os
import pickle
import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")

EXCEL_FILE = os.path.join(DATA_DIR, "NAS-NYSE-bereinigt.xlsx")
PICKLE_FILE = os.path.join(DATA_DIR, "symbols_list.pkl")


def main():
    if not os.path.exists(EXCEL_FILE):
        print(f"FEHLER: {EXCEL_FILE} nicht gefunden.")
        print("Bitte zuerst 02_filter_and_merge.py ausfuehren.")
        return

    df = pd.read_excel(EXCEL_FILE)
    symbols_list = df.iloc[:, 0].dropna().tolist()

    with open(PICKLE_FILE, "wb") as f:
        pickle.dump(symbols_list, f)

    print(f"Pickle-Datei erstellt: {PICKLE_FILE}")
    print(f"Anzahl Symbole: {len(symbols_list)}")
    print(f"Erste 10: {symbols_list[:10]}")
    print(f"Letzte 10: {symbols_list[-10:]}")


if __name__ == "__main__":
    main()
