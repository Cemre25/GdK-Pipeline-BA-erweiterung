# pipelines/load/db_utils.py
# ─────────────────────────────────────────────────────────────
# Wiederverwendbare Hilfsfunktionen für alle Load-Skripte
# ─────────────────────────────────────────────────────────────

import os
import urllib.parse
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# ── .env laden ──────────────────────────────────────────────
# Path(__file__) = dieser Datei (db_utils.py)
# .parents[2]   = 2 Ebenen hoch → Projekt-Root
base_dir = Path(__file__).resolve().parents[2]
load_dotenv(dotenv_path=base_dir / ".env")


def get_engine():
    """
    Gibt eine SQLAlchemy Engine zurück.
    Sonderzeichen im Passwort (@ : %) werden mit quote_plus escaped.
    """
    user     = os.getenv("DB_USER")
    password = urllib.parse.quote_plus(os.getenv("DB_PASSWORD"))
    host     = os.getenv("DB_HOST", "localhost")
    port     = os.getenv("DB_PORT", "5432")
    db_name  = os.getenv("DB_NAME")

    url = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"
    return create_engine(url)


def get_bezirk_id_map(engine) -> dict:
    """
    Gibt ein dict zurück: { "Mitte": 1, "Neukölln": 2, ... }
    Wird benötigt, um den Bezirks-Namen in die bezirk_id (FK) umzuwandeln.
    Muss NACH load_bezirk.py aufgerufen werden!
    """
    with engine.connect() as conn:
        result = conn.execute(text("SELECT name, bezirk_id FROM bezirk"))
        return {row[0]: row[1] for row in result}


def get_lor_bezirk_id_map(engine) -> dict:
    """
    Gibt ein dict zurück: { "bzr_name_oder_id": bezirk_id, ... }
    Wird benötigt, um LOR → bezirk_id zuzuordnen.
    """
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT bzr_id, bezirk_id FROM lor_bezirksregion")
        )
        return {row[0]: row[1] for row in result}