# pipelines/load/load_wetter.py
# ─────────────────────────────────────────────────────────────
# Lädt Wetterdaten aus DWD-Dateien
# → Tabellen: wetter_monatswert, wetter_tageswert
# ─────────────────────────────────────────────────────────────

import pandas as pd
from pathlib import Path
from db_utils import get_engine

DATA_DIR = Path(__file__).resolve().parents[2] / "data"

# DWD Stations-ID für Berlin (Berliner Wetterwarte / Tempelhof)
STATIONS_ID = 433


def load_wetter_monat():
    engine = get_engine()

    # ── 1. EXTRACT ───────────────────────────────────────────
    df = pd.read_csv(
        DATA_DIR / "combined_monthly_daten_2020_2024_minimal.csv",
        sep=";", encoding="utf-8", decimal=",",
    )

    # ── 2. TRANSFORM ─────────────────────────────────────────
    df["datum"] = pd.to_datetime(df["MESS_DATUM_BEGINN"], format="%d.%m.%Y")
    df["jahr"]  = df["datum"].dt.year
    df["monat"] = df["datum"].dt.month

    # -999 ist der DWD-Fehlerwert → als NaN behandeln
    df["MO_RR"] = pd.to_numeric(df["MO_RR"], errors="coerce").replace(-999, pd.NA)
    df["MO_TT"] = pd.to_numeric(df["MO_TT"], errors="coerce").replace(-999, pd.NA)

    df = df.rename(columns={"MO_RR": "niederschlag_mm", "MO_TT": "temp_avg"})
    df["stations_id"] = STATIONS_ID

    df = df[["stations_id", "datum", "jahr", "monat", "niederschlag_mm", "temp_avg"]]

    # ── 3. LOAD ───────────────────────────────────────────────
    df.to_sql("wetter_monatswert", engine, if_exists="append", index=False)
    print(f"✅ wetter_monatswert: {len(df)} Einträge geladen.")


def load_wetter_tag():
    engine = get_engine()

    # ── 1. EXTRACT ───────────────────────────────────────────
    df = pd.read_csv(
        DATA_DIR / "combined_daily_daten_2020_2024.csv",
        sep=";", encoding="utf-8",
    )

    # ── 2. TRANSFORM ─────────────────────────────────────────
    df["datum"] = pd.to_datetime(df["MESS_DATUM"], format="%Y-%m-%d", errors="coerce")

    # Spalten umbenennen auf DDL-Namen (Kleinschreibung)
    df = df.rename(columns={
        "STATIONS_ID": "stations_id",
        "FX":  "fx",  "FM":  "fm",
        "RSK": "rsk", "RSKF": "rskf",
        "SDK": "sdk", "SHK_TAG": "shk_tag",
        "NM":  "nm",  "VPM": "vpm",
        "PM":  "pm",  "TMK": "tmk",
        "UPM": "upm", "TXK": "txk",
        "TNK": "tnk", "TGK": "tgk",
    })

    # -999 ist der DWD-Fehlerwert → als NaN behandeln
    numeric_cols = ["fx","fm","rsk","sdk","nm","vpm","pm","tmk","upm","txk","tnk","tgk"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").replace(-999, pd.NA)

    df = df[["stations_id","datum","fx","fm","rsk","rskf","sdk",
             "shk_tag","nm","vpm","pm","tmk","upm","txk","tnk","tgk"]].copy()

    # ── 3. LOAD ───────────────────────────────────────────────
    df.to_sql("wetter_tageswert", engine, if_exists="replace", index=False)
    print(f"✅ wetter_tageswert: {len(df)} Einträge geladen.")


if __name__ == "__main__":
    load_wetter_monat()
    load_wetter_tag()