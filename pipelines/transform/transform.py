import pandas as pd
import geopandas as gpd
from pathlib import Path

LOR_URL = (
    "https://gdi.berlin.de/services/wfs/lor_2019"
    "?service=WFS&version=1.1.0&request=GetFeature"
    "&typeName=lor_2019:b_lor_bzr_2019"
)

BEZIRKSFLAECHEN = pd.DataFrame({
    "bezirk": [
        "Mitte", "Friedrichshain-Kreuzberg", "Pankow",
        "Charlottenburg-Wilmersdorf", "Spandau", "Steglitz-Zehlendorf",
        "Tempelhof-Schöneberg", "Neukölln", "Treptow-Köpenick",
        "Marzahn-Hellersdorf", "Lichtenberg", "Reinickendorf"
    ],
    "flaeche_ha": [
        3940, 2040, 10322, 6469, 9188,
        10256, 5305, 4493, 16773, 6182, 5212, 8932
    ]
})

DATA_DIR = Path("data")


def load_sozialindex_mit_Gesamtbewasserung_agg()-> pd.DataFrame:
    df = pd.read_csv(
        DATA_DIR / "sozialindex_mit_Gesamtbewässerung.csv",
        sep=";", encoding="utf-8", decimal=","
    )
    df["gesamt_bewaesserung_lor"] = pd.to_numeric(df["gesamt_bewaesserung_lor"].str.replace(",", ".", regex=False), errors = "coerce")
    df["GESIx_2022"] = pd.to_numeric(df["GESIx_2022"].str.replace(",",".", regex= False), errors="coerce")

def load_kpi() -> pd.DataFrame:
    return pd.read_csv(
        DATA_DIR / "KPI.csv", sep=";", encoding="utf-8"
    )

def load_lor() -> gpd.GeoDataFrame: 
    gdf = gpd.read_file(LOR_URL)
    gdf = gdf[["bzr_id", "bzr_name", "geometry"]]
    gdf = gdf.simplify(tolerance=0.001, preserve_topology=True)
    gdf = gdf.to_crs(epsg=4326)
    return gdf

def load_wetterdaten()-> pd.DataFrame:
    df = pd.read_csv(DATA_DIR / "combined_monthly_daten_2020_2024_minimal.csv", sep=";", encoding="utf-8", decimal=",")
    df["date"] =  pd.to_datetime(df["MESS_DATUM_BEGINN"], format="%d.%m.%Y")
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month

    df["MO_RR"] = pd.to_numeric(df["MO_RR"].str.replace(",",".", regex= False), errors="coerce").replace(-999, pd.NA)
    df["MO_TT"] = pd.to_numeric(df["MO_TT"].str.replace(",",".", regex= False), errors="coerce").replace(-999, pd.NA)

    return df.rename(columns={"MO_RR": "niederschlag", "MO_TT": "temp_avg"})[
        ["date", "year", "month", "niederschlag", "temp_avg"]
    ]
