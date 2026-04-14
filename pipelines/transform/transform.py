import pandas as pd
import geopandas as gpd
from pathlib import Path
import numpy as np
from functools import lru_cache

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

@lru_cache(maxsize=None)
def load_sozialindex_mit_Gesamtbewasserung_agg() -> pd.DataFrame:
    df = pd.read_csv(
        DATA_DIR / "sozialindex_mit_Gesamtbewässerung.csv",
        sep=";", encoding="utf-8", decimal=","
    )
    df["gesamt_bewaesserung_lor"] = pd.to_numeric(df["gesamt_bewaesserung_lor"], errors="coerce")
    df["GESIx_2022"] = pd.to_numeric(df["GESIx_2022"], errors="coerce")
    return df 

@lru_cache(maxsize=None)
def load_kpi() -> pd.DataFrame:
    return pd.read_csv(
        DATA_DIR / "KPI.csv", sep=";", encoding="utf-8"
    )

@lru_cache(maxsize=None)
def load_lor() -> gpd.GeoDataFrame: 
    gdf = gpd.read_file(LOR_URL)
    gdf = gdf[["bzr_id", "bzr_name", "geometry"]]
    gdf["geometry"] = gdf["geometry"].simplify(tolerance=0.001, preserve_topology=True)
    gdf = gdf.to_crs(epsg=4326)
    return gdf

@lru_cache(maxsize=None)
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

@lru_cache(maxsize=None)
def load_df_merged()-> pd.DataFrame:
    df = pd.read_csv(DATA_DIR / "df_merged_final.csv", sep=";", encoding="utf-8", decimal=",")
    df["pflanzjahr"] = pd.to_numeric(df["pflanzjahr"], errors="coerce")
    df["bewaesserungsmenge_in_liter"] = pd.to_numeric(df["bewaesserungsmenge_in_liter"], errors="coerce")
    df["baumalter"] = pd.Timestamp.now().year - df["pflanzjahr"]
    return df

@lru_cache(maxsize=None)
def load_df_merged_mit_lor_sum() -> gpd.GeoDataFrame:
    return gpd.read_file(DATA_DIR /"df_merged_mit_lor_und_sum.geojson")

def load_df_merged_unique(df: pd.DataFrame) -> pd.DataFrame:
    return df.drop_duplicates(subset="gisid")

@lru_cache(maxsize=None)
def load_pumpen_mit_bezirk() -> gpd.GeoDataFrame:
    df = gpd.read_file(DATA_DIR / "pumpen_mit_bezirk_minimal.geojson")
    df["label_text"] = df["id"].astype(str).where(df["id"].notna(), "Unbekannt")
    return df

@lru_cache(maxsize=None)
def load_pumpen_mit_lor() -> gpd.GeoDataFrame:
    return gpd.read_file(DATA_DIR / "pumpen_mit_lor.geojson")

@lru_cache(maxsize=None)
def load_df_merged_sum_distanz_umkreis_pump_ok_lor() -> pd.DataFrame:
    df = pd.read_csv(
        DATA_DIR / "df_merged_sum_mit_distanzen_mit_umkreis_gesamter_Baumbestand_nur_Pumpen_ok_lor.csv",
        sep=";", encoding="utf-8", decimal=","
    )
    df["lng"] = pd.to_numeric(df["lng"], errors="coerce")
    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["durchschnitts_intervall"] = pd.to_numeric(
        df["durchschnitts_intervall"], errors="coerce"
    )
    return df.dropna(subset=["lat", "lng"])


def transform_df_merged_sum_distanz_clean(df):
    return df.dropna(subset=["timestamp"])


def transform_df_merged_clean(df):
    return df.dropna(subset=["bezirk", "timestamp", "gattung_deutsch"])

@lru_cache(maxsize=None)
def load_sozialindex():
    return pd.read_csv("data/sozialindex.csv", sep=";", encoding="utf-8")

@lru_cache(maxsize=None)
def load_bezirksgrenzen():
    return gpd.read_file("data/bezirksgrenzen.geojson")

def load_df_with_flaeche(df, bezirksflaechen):
    return df.merge(bezirksflaechen, on="bezirk", how="left")

def load_baumanzahl_pro_bezirk(df):
    return (
        df.groupby("bezirk")["gisid"]
        .nunique()
        .reset_index(name="baumanzahl")
    )

def load_baum_dichte(df_baum, bezirksflaechen):
    df = df_baum.merge(bezirksflaechen, on="bezirk", how="left")
    df["baeume_pro_ha"] = df["baumanzahl"] / df["flaeche_ha"]
    return df

def transform_cleaned_data(df):
    if not isinstance(df, pd.DataFrame):
        raise ValueError(f"Daten sind kein DataFrame. Typ: {type(df)}")

    df = df.copy()

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["monat"] = df["timestamp"].dt.month

    def saison(m):
        if m in [12, 1, 2]:
            return "Winter"
        elif m in [3, 4, 5]:
            return "Frühling"
        elif m in [6, 7, 8]:
            return "Sommer"
        elif m in [9, 10, 11]:
            return "Herbst"
        return "Unbekannt"

    df["saison"] = df["monat"].apply(saison)

    return df

def transform_cleaned_data_light(df):
    cols = [
        "gisid", "gesamt_bewaesserung", "durchschnitts_intervall",
        "gattung_deutsch", "art_dtsch", "hausnr", "strname",
        "bezirk", "bzr_name", "lng", "lat", "timestamp"
    ]

    df = df[cols].copy()

    df["year"] = df["timestamp"].dt.year
    df["month"] = df["timestamp"].dt.month

    return df

def transform_wetter_monat(df):
    return df[["year", "month", "niederschlag", "temp_avg"]]

def load_gisid_check(df):
    grouped = df.groupby("gisid")

    # nur Gruppen mit mehr als 1 Eintrag
    df_multi = df[df["gisid"].isin(grouped.filter(lambda x: len(x) > 1)["gisid"])]

    result = grouped.nunique()

    # nur Zeilen behalten, wo irgendwo >1 unterschiedliche Werte
    return result[(result > 1).any(axis=1)]

def transform_merged_for_rating_base(df_clean, wetter, df_unique):
    df = df_clean.merge(wetter, on=["year", "month"], how="left")

    df = df.merge(
        df_unique[["gisid", "baumalter"]],
        on="gisid",
        how="left"
    )

    return df

def transform_merged_for_rating(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    conditions = [
        df["niederschlag"].isna() | df["temp_avg"].isna(),
        (df["baumalter"] < 10) & (df["niederschlag"] < 30) & (df["temp_avg"] > 25),
        (df["baumalter"] < 30) & (df["niederschlag"] < 60) & (df["temp_avg"] > 20),
    ]
    choices = ["Unbekannt", "Hoch", "Mittel"]
    df["gesamt_bewaesserung_rating"] = np.select(conditions, choices, default="Niedrig")
    return df

@lru_cache(maxsize=None)
def load_einwohnerGiessm():
    return pd.read_csv("data/Einwohner_Giessmenge_joined.csv", sep=";", encoding="utf-8")


@lru_cache(maxsize=None)
def load_einwohner():
    return pd.read_csv("data/GesamteEinwohnerzahlNachBezirk.csv", sep=";", encoding="utf-8")

@lru_cache(maxsize=None)
def load_einwohnerGiessm2020_24():
    return pd.read_csv("data/Einwohner_Giessmenge_joined_2020_2024.csv", sep=";", encoding="utf-8")