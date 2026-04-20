# pipelines/load/load_bezirk.py
# ─────────────────────────────────────────────────────────────
# Lädt Berliner Bezirksgrenzen aus bezirksgrenzen.geojson
# → Tabelle: bezirk
# ─────────────────────────────────────────────────────────────

import geopandas as gpd
import pandas as pd
from pathlib import Path
from db_utils import get_engine

DATA_DIR = Path(__file__).resolve().parents[2] / "data"

# Flächen-Daten (aus transform.py übernommen)
BEZIRKSFLAECHEN = {
    "Mitte": 3940,
    "Friedrichshain-Kreuzberg": 2040,
    "Pankow": 10322,
    "Charlottenburg-Wilmersdorf": 6469,
    "Spandau": 9188,
    "Steglitz-Zehlendorf": 10256,
    "Tempelhof-Schöneberg": 5305,
    "Neukölln": 4493,
    "Treptow-Köpenick": 16773,
    "Marzahn-Hellersdorf": 6182,
    "Lichtenberg": 5212,
    "Reinickendorf": 8932,
}


def load_bezirk():
    engine = get_engine()

    # ── 1. EXTRACT ───────────────────────────────────────────
    gdf = gpd.read_file(DATA_DIR / "bezirksgrenzen.geojson")

    # ── 2. TRANSFORM ─────────────────────────────────────────
    # Nur die Spalten behalten, die unsere Tabelle braucht
    gdf = gdf[["Gemeinde_name", "geometry"]].copy()

    # Spalte umbenennen damit sie zum DDL passt
    gdf = gdf.rename(columns={"Gemeinde_name": "name"})

    # Fläche aus dem Dictionary hinzufügen
    gdf["flaeche_ha"] = gdf["name"].map(BEZIRKSFLAECHEN)

    # In WGS84 (EPSG:4326) projizieren – ist laut DDL erwartet
    gdf = gdf.to_crs(epsg=4326)

    # ── 3. LOAD ───────────────────────────────────────────────
    # to_postgis() schreibt die Geometrie automatisch als PostGIS-Typ.
    # if_exists="replace" → Tabelle wird geleert und neu befüllt.
    # ACHTUNG: Damit gehen DDL-Constraints verloren. Für Produktion
    # stattdessen TRUNCATE + if_exists="append" verwenden.
    gdf.to_postgis(
        name="bezirk",
        con=engine,
        if_exists="append",   # oder "append" nach manuellem TRUNCATE
        index=False,
    )

    print(f"✅ bezirk: {len(gdf)} Bezirke geladen.")


if __name__ == "__main__":
    load_bezirk()