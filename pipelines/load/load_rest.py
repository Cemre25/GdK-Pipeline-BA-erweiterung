# pipelines/load/load_rest.py
# ─────────────────────────────────────────────────────────────
# Lädt alle verbleibenden Tabellen:
#   - sozialindex
#   - einwohner_bezirk
#   - kpi_snapshot
#   - bewaesserung_bezirk (aggregiert)
#   - bewaesserung_lor    (aggregiert)
# ─────────────────────────────────────────────────────────────

import glob
import geopandas as gpd
import pandas as pd
from pathlib import Path
from db_utils import get_engine, get_bezirk_id_map

DATA_DIR = Path(__file__).resolve().parents[2] / "data"


# ──────────────────────────────────────────────────────────────
# SOZIALINDEX
# Quelle: sozialindex.csv
# ──────────────────────────────────────────────────────────────
def load_sozialindex():
    engine = get_engine()

    df = pd.read_csv(DATA_DIR / "sozialindex.csv", sep=";", encoding="utf-8")

    # bzrID → bzr_id, und sicherstellen, dass es ein String ist
    df = df.rename(columns={"bzrID": "bzr_id"})
    df["bzr_id"] = df["bzr_id"].astype(str)

    # Spalten auf DDL reduzieren und umbenennen
    df = df.rename(columns={
        "GESIx_2013": "gesix_2013", "GESIx_2022": "gesix_2022",
        "ESIx_2013":  "esix_2013",  "ESIx_2022":  "esix_2022",
        "DI_2013":    "di_2013",    "DI_2022":    "di_2022",
        "DII_2013":   "dii_2013",   "DII_2022":   "dii_2022",
        "DIII_2013":  "diii_2013",  "DIII_2022":  "diii_2022",
    })

    cols = ["bzr_id","gesix_2013","gesix_2022","esix_2013","esix_2022",
            "di_2013","di_2022","dii_2013","dii_2022","diii_2013","diii_2022"]
    df = df[cols]

    df.to_sql("sozialindex", engine, if_exists="append", index=False)
    print(f"✅ sozialindex: {len(df)} LOR-Einträge geladen.")


# ──────────────────────────────────────────────────────────────
# EINWOHNER PRO BEZIRK
# Quelle: GesamteEinwohnerzahlNachBezirk.csv
# Das ist ein "breites" Format: Stichtag | Bezirk1 | Bezirk2 | ...
# Wir müssen es ins "lange" Format umwandeln: Stichtag | bezirk | einwohner
# ──────────────────────────────────────────────────────────────
def load_einwohner():
    engine = get_engine()

    df_wide = pd.read_csv(
        DATA_DIR / "GesamteEinwohnerzahlNachBezirk.csv",
        sep=";", encoding="utf-8",
    )

    # Datum parsen und Jahr extrahieren
    df_wide["stichtag"] = pd.to_datetime(df_wide["Stichtag"], format="%d.%m.%Y", errors="coerce")
    df_wide["jahr"]     = df_wide["stichtag"].dt.year

    # "Berlin"-Spalte rauswerfen (Gesamtwert, kein Bezirk)
    df_wide = df_wide.drop(columns=["Stichtag", "Berlin"], errors="ignore")

    # Wide → Long: jeder Bezirk wird eine eigene Zeile
    # before: stichtag | Mitte | Pankow | ...
    # after:  stichtag | bezirk_name | einwohner
    df_long = df_wide.melt(
        id_vars=["stichtag", "jahr"],
        var_name="bezirk_name",
        value_name="einwohner_str",
    )

    # Einwohnerzahl bereinigen: "3 517 424" → 3517424
    # (Tausendertrennzeichen ist ein Leerzeichen)
    df_long["einwohner"] = (
        df_long["einwohner_str"]
        .astype(str)
        .str.replace(r"\s+", "", regex=True)   # alle Leerzeichen entfernen
        .pipe(pd.to_numeric, errors="coerce")
    )

    # bezirk_id aus DB nachschlagen
    bezirk_map = get_bezirk_id_map(engine)
    df_long["bezirk_id"] = df_long["bezirk_name"].map(bezirk_map)

    # Zeilen ohne gültige bezirk_id verwerfen (z.B. unbekannte Namen)
    df_long = df_long.dropna(subset=["bezirk_id", "einwohner"])
    df_long["bezirk_id"] = df_long["bezirk_id"].astype(int)

    df_out = df_long[["stichtag", "bezirk_id", "jahr", "einwohner"]]

    df_out.to_sql("einwohner_bezirk", engine, if_exists="replace", index=False)
    print(f"✅ einwohner_bezirk: {len(df_out)} Einträge geladen.")


# ──────────────────────────────────────────────────────────────
# KPI SNAPSHOT
# Quelle: KPI.csv – passt fast 1:1 zur DDL
# ──────────────────────────────────────────────────────────────
def load_kpi():
    engine = get_engine()

    df = pd.read_csv(DATA_DIR / "KPI.csv", sep=";", encoding="utf-8")

    # Datum parsen
    df["datestamp"] = pd.to_datetime(df["datestamp"], format="%d.%m.%Y", errors="coerce")

    # Spalten auf DDL reduzieren (alle Spalten passen bereits)
    cols = [
        "datestamp", "adoptedtrees", "uniqueadoptedtrees", "uniqueusers",
        "treeswatered", "uniquetreeswatered", "uniquewateringusers", "totalwateramount",
    ]
    df = df[cols]

    df.to_sql("kpi_snapshot", engine, if_exists="replace", index=False)
    print(f"✅ kpi_snapshot: {len(df)} Snapshots geladen.")


# ──────────────────────────────────────────────────────────────
# BEWÄSSERUNG PRO BEZIRK (aggregiert)
# Quelle: Bewaesserung_mit_Einwohner_pro_Bezirk.csv
# ──────────────────────────────────────────────────────────────
def load_bewaesserung_bezirk():
    engine = get_engine()

    df = pd.read_csv(
        DATA_DIR / "Bewaesserung_mit_Einwohner_pro_Bezirk.csv",
        sep=";", encoding="utf-8",
    )

    bezirk_map = get_bezirk_id_map(engine)
    df["bezirk_id"] = df["bezirk"].map(bezirk_map)

    df = df.rename(columns={
        "year":                      "jahr",
        "gesamt_bewaesserung_liter": "gesamt_bewaesserung_liter",
        "liter_pro_einwohner":       "liter_pro_einwohner",
    })

    df = df[["bezirk_id", "jahr", "gesamt_bewaesserung_liter", "liter_pro_einwohner"]]
    df = df.dropna(subset=["bezirk_id"])
    df["bezirk_id"] = df["bezirk_id"].astype(int)

    df.to_sql("bewaesserung_bezirk", engine, if_exists="replace", index=False)
    print(f"✅ bewaesserung_bezirk: {len(df)} Einträge geladen.")


# ──────────────────────────────────────────────────────────────
# BEWÄSSERUNG PRO LOR (aggregiert)
# Quelle: df_merged_mit_lor_und_sum.geojson
# ──────────────────────────────────────────────────────────────
def load_bewaesserung_lor():
    engine = get_engine()

    # GeoJSON einlesen (wir brauchen nur die Sachdaten, keine Geometrie)
    gdf = gpd.read_file(DATA_DIR / "df_merged_mit_lor_und_sum.geojson")

    df = pd.DataFrame(gdf.drop(columns="geometry"))
    df["bzr_id"] = df["bzr_id"].astype(str)

    df = df.rename(columns={"gesamt_bewaesserung_lor": "gesamt_bewaesserung_liter"})

    # Kein Jahr in der Quelle → als NULL / ohne Jahr speichern
    # (Daten sind offenbar kumulativ über alle Jahre)
    df["jahr"] = pd.NA

    df = df[["bzr_id", "jahr", "gesamt_bewaesserung_liter"]]

    df.to_sql("bewaesserung_lor", engine, if_exists="replace", index=False)
    print(f"✅ bewaesserung_lor: {len(df)} LOR-Einträge geladen.")


if __name__ == "__main__":
    load_sozialindex()
    load_einwohner()
    load_kpi()
    load_bewaesserung_bezirk()
    load_bewaesserung_lor()