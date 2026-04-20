# pipelines/load/load_lor.py
# ─────────────────────────────────────────────────────────────
# Lädt LOR-Planungsräume (Bezirksregionen)
# Quelle: df_merged_mit_lor_und_sum.geojson (hat Geometrie)
#         + pumpen_mit_lor.geojson (hat bzr_id als String)
# → Tabelle: lor_bezirksregion
#
# WARUM zwei Quellen?
#   df_merged_mit_lor_und_sum.geojson hat die Geometrie der LOR-Polygone.
#   Man brauchen aber auch bezirk_id (FK), die man aus der
#   bezirk-Tabelle nachschlagen müssen.
#   Die Bezirks-Zuordnung entnimmt man sozialindex_mit_Bewässerung.csv,
#   das ein "bez"-Feld hat (z.B. "03 - Pankow").
# ─────────────────────────────────────────────────────────────

import glob
import geopandas as gpd
import pandas as pd
from pathlib import Path
from db_utils import get_engine, get_bezirk_id_map

DATA_DIR = Path(__file__).resolve().parents[2] / "data"

# Mapping "bez"-Feld → Bezirksname (wie er in der bezirk-Tabelle steht) (simply the Bez)
BEZ_ZU_NAME = {
    "01 - Mitte":                       "Mitte",
    "02 - Friedrichshain-Kreuzberg":    "Friedrichshain-Kreuzberg",
    "03 - Pankow":                      "Pankow",
    "04 - Charlottenburg-Wilmersdorf":  "Charlottenburg-Wilmersdorf",
    "05 - Spandau":                     "Spandau",
    "06 - Steglitz-Zehlendorf":         "Steglitz-Zehlendorf",
    "07 - Tempelhof-Schöneberg":        "Tempelhof-Schöneberg",
    "08 - Neukölln":                    "Neukölln",
    "09 - Treptow-Köpenick":            "Treptow-Köpenick",
    "10 - Marzahn-Hellersdorf":         "Marzahn-Hellersdorf",
    "11 - Lichtenberg":                 "Lichtenberg",
    "12 - Reinickendorf":               "Reinickendorf",
}


def load_lor():
    engine = get_engine()

    # ── 1. EXTRACT ───────────────────────────────────────────
    # Geometrie der LOR-Flächen
    gdf_geom = gpd.read_file(DATA_DIR / "df_merged_mit_lor_und_sum.geojson")
    gdf_geom = gdf_geom[["bzr_id", "bzr_name", "geometry"]].copy()
    # bzr_id als String sicherstellen (manchmal als int eingelesen)
    gdf_geom["bzr_id"] = gdf_geom["bzr_id"].astype(str)

    # Bezirks-Zuordnung aus sozialindex_mit_Bewässerung.csv
    csv_path = glob.glob(str(DATA_DIR / "sozialindex_mit_Gesamtbew*.csv"))[0]
    df_soz = pd.read_csv(csv_path, sep=";", encoding="utf-8", decimal=",")
    df_soz["bzr_id"] = df_soz["bzr_id"].astype(str)
    df_soz = df_soz[["bzr_id", "bez"]].copy()

    # ── 2. TRANSFORM ─────────────────────────────────────────
    # Geometrie + Bezirkszuordnung zusammenführen
    gdf = gdf_geom.merge(df_soz, on="bzr_id", how="left")

    # bezirk_id aus der DB nachschlagen
    bezirk_map = get_bezirk_id_map(engine)   # {"Mitte": 1, ...}
    gdf["bezirk_name"] = gdf["bez"].map(BEZ_ZU_NAME)
    gdf["bezirk_id"]   = gdf["bezirk_name"].map(bezirk_map)

    # Spalten für DDL auswählen
    gdf = gdf[["bzr_id", "bzr_name", "bezirk_id", "geometry"]].copy()
    gdf = gdf.rename(columns={"bzr_name": "name"})

    # Projektion sicherstellen
    gdf = gdf.to_crs(epsg=4326)

    # ── 3. LOAD ───────────────────────────────────────────────
    gdf.to_postgis(
        name="lor_bezirksregion",
        con=engine,
        if_exists="append",
        index=False,
    )

    print(f"✅ lor_bezirksregion: {len(gdf)} LOR-Planungsräume geladen.")


if __name__ == "__main__":
    load_lor()