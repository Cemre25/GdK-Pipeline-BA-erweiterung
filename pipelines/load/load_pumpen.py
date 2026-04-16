# pipelines/load/load_pumpen.py
# ─────────────────────────────────────────────────────────────
# Lädt Pumpen-Daten aus pumpen_mit_lor.geojson
# → Tabelle: pumpe
# ─────────────────────────────────────────────────────────────

import geopandas as gpd
import pandas as pd
from pathlib import Path
from db_utils import get_engine, get_bezirk_id_map

DATA_DIR = Path(__file__).resolve().parents[2] / "data"

# Erlaubte Werte für den pump_status ENUM in der Datenbank
VALID_STATUS = {"ok", "unbekannt", "defekt", "stillgelegt"}


def load_pumpen():
    engine = get_engine()

    # ── 1. EXTRACT ───────────────────────────────────────────
    gdf = gpd.read_file(DATA_DIR / "pumpen_mit_lor.geojson")

    # ── 2. TRANSFORM ─────────────────────────────────────────
    # Spalten umbenennen auf DDL-Namen
    gdf = gdf.rename(columns={
        "id":           "pumpen_id",
        "pump":         "pump_type",
        "pump.style":   "pump_style",
        "pump.status":  "pump_status",
        "Gemeinde_name":"bezirk_name",   # temporär für FK-Lookup
        "bzr_id":       "bzr_id",
    })

    # pump_status: fehlende Werte als "unbekannt" füllen
    # und sicherstellen, dass nur ENUM-Werte vorkommen
    gdf["pump_status"] = (
        gdf["pump_status"]
        .fillna("unbekannt")
        .str.lower()
        .apply(lambda s: s if s in VALID_STATUS else "unbekannt")
    )

    # bezirk_id FK nachschlagen
    bezirk_map = get_bezirk_id_map(engine)
    gdf["bezirk_id"] = gdf["bezirk_name"].map(bezirk_map)

    # bzr_id als String (manchmal als int eingelesen)
    gdf["bzr_id"] = gdf["bzr_id"].astype(str)

    # Geometrie umbenennen
    gdf = gdf.rename_geometry("standort")

    # Nur DDL-Spalten behalten
    gdf = gdf[["pumpen_id", "pump_type", "pump_style", "pump_status",
               "bezirk_id", "bzr_id", "standort"]].copy()

    # Projektion sicherstellen
    gdf = gdf.to_crs(epsg=4326)

    # ── 3. LOAD ───────────────────────────────────────────────
    # ACHTUNG: pump_status ist ein PostgreSQL ENUM-Typ.
    # with if_exists="replace" wird die Tabelle ohne ENUM neu erstellt
    # (als TEXT). Das ist OK für Entwicklung – für Produktion
    # TRUNCATE + append + manueller DDL-CAST nötig.
    gdf.to_postgis(
        name="pumpe",
        con=engine,
        if_exists="replace",
        index=False,
    )
    print(f"✅ pumpe: {len(gdf)} Pumpen geladen.")


if __name__ == "__main__":
    load_pumpen()