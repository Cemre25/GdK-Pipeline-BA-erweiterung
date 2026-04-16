# pipelines/load/load_baum_giessung.py
# ─────────────────────────────────────────────────────────────
# Lädt Baum-Stammdaten und Gieß-Ereignisse aus df_merged_final.csv
# → Tabellen: baum, giessung
#
# WICHTIG – Warum eine Datei für zwei Tabellen?
#   df_merged_final.csv ist eine "gemischte" Datei:
#   Jede Zeile steht für EIN Gieß-Ereignis an EINEM Baum.
#   Bäume, die noch nie gegossen wurden, haben NaN bei timestamp.
#   Bäume, die mehrfach gegossen wurden, kommen mehrfach vor.
#
#   Deshalb trennen wir hier:
#     baum     = einmalige Baum-Infos  (drop_duplicates auf gisid)
#     giessung = alle Gieß-Ereignisse  (Zeilen mit timestamp != NaN)
# ─────────────────────────────────────────────────────────────

import geopandas as gpd
import pandas as pd
from pathlib import Path
from db_utils import get_engine, get_bezirk_id_map

DATA_DIR = Path(__file__).resolve().parents[2] / "data"

# Spalten aus df_merged_final, die zur baum-Tabelle gehören
BAUM_COLS = [
    "gml_id", "gisid", "pitid", "standortnr", "kennzeich", "namenr",
    "art_dtsch", "art_bot", "gattung_deutsch", "gattung", "art_gruppe",
    "pflanzjahr", "standalter", "kronedurch", "stammumfg", "baumhoehe",
    "eigentuemer", "bezirk", "strname", "hausnr", "zusatz",
    "lng", "lat",
]


def load_baum_und_giessung():
    engine = get_engine()

    # ── 1. EXTRACT ───────────────────────────────────────────
    df = pd.read_csv(
        DATA_DIR / "df_merged_final.csv",
        sep=";", encoding="utf-8", decimal=",",
    )

    # Typen korrigieren
    df["pflanzjahr"]                 = pd.to_numeric(df["pflanzjahr"], errors="coerce")
    df["bewaesserungsmenge_in_liter"]= pd.to_numeric(df["bewaesserungsmenge_in_liter"], errors="coerce")
    df["timestamp"]                  = pd.to_datetime(df["timestamp"], errors="coerce")
    df["lng"]                        = pd.to_numeric(df["lng"], errors="coerce")
    df["lat"]                        = pd.to_numeric(df["lat"], errors="coerce")

    # ── 2a. TRANSFORM: baum ──────────────────────────────────
    df_baum = df[BAUM_COLS].drop_duplicates(subset="gisid").copy()

    # bezirk_id aus DB nachschlagen (bezirk-Tabelle muss bereits geladen sein!)
    bezirk_map = get_bezirk_id_map(engine)
    df_baum["bezirk_id"] = df_baum["bezirk"].map(bezirk_map)

    # Geometrie aus lng/lat erstellen
    gdf_baum = gpd.GeoDataFrame(
        df_baum,
        geometry=gpd.points_from_xy(df_baum["lng"], df_baum["lat"]),
        crs="EPSG:4326",
    )
    # Geometrie-Spalte umbenennen, damit sie zum DDL passt
    gdf_baum = gdf_baum.rename_geometry("standort")

    # Hilfsspalten entfernen (bezirk als Name brauchen wir nicht mehr,
    # da bezirk_id jetzt vorhanden ist; lng/lat stecken in standort)
    gdf_baum = gdf_baum.drop(columns=["bezirk", "lng", "lat"])

    # ── 3a. LOAD: baum ────────────────────────────────────────
    gdf_baum.to_postgis(
        name="baum",
        con=engine,
        if_exists="replace",
        index=False,
    )
    print(f"✅ baum: {len(gdf_baum)} einzigartige Bäume geladen.")

    # ── 2b. TRANSFORM: giessung ──────────────────────────────
    # Nur Zeilen mit einem gültigen Zeitstempel (= tatsächliche Gießereignisse)
    df_giessung = df[df["timestamp"].notna()][
        ["gisid", "timestamp", "bewaesserungsmenge_in_liter"]
    ].copy()

    df_giessung = df_giessung.rename(
        columns={"bewaesserungsmenge_in_liter": "menge_liter"}
    )

    # ── 3b. LOAD: giessung ────────────────────────────────────
    df_giessung.to_sql(
        name="giessung",
        con=engine,
        if_exists="replace",
        index=False,
    )
    print(f"✅ giessung: {len(df_giessung)} Gieß-Ereignisse geladen.")


if __name__ == "__main__":
    load_baum_und_giessung()