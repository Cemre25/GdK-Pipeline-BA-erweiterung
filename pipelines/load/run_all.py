# pipelines/load/run_all.py
# ─────────────────────────────────────────────────────────────
# Führt alle Load-Skripte in der richtigen Reihenfolge aus.
#
# REIHENFOLGE IST WICHTIG wegen Foreign Keys (FK):
#
#   bezirk           ← wird von fast allem referenziert
#   lor_bezirksregion← braucht bezirk
#   baum + giessung  ← braucht bezirk (lor optional hier)
#   pumpen           ← braucht bezirk + lor
#   wetter           ← unabhängig
#   sozialindex      ← braucht lor
#   einwohner        ← braucht bezirk
#   kpi_snapshot     ← unabhängig
#   bewaesserung_*   ← braucht bezirk + lor
# ─────────────────────────────────────────────────────────────

import sys
import traceback

from sqlalchemy import text
from db_utils import get_engine

from load_bezirk        import load_bezirk
from load_lor           import load_lor
from load_baum_giessung import load_baum_und_giessung
from load_pumpen        import load_pumpen
from load_wetter        import load_wetter_monat, load_wetter_tag
from load_rest          import (
    load_sozialindex,
    load_einwohner,
    load_kpi,
    load_bewaesserung_bezirk,
    load_bewaesserung_lor,
)

# Liste aller Schritte: (Name, Funktion)
STEPS = [
    ("1. bezirk",              load_bezirk),
    ("2. lor_bezirksregion",   load_lor),
    ("3. baum + giessung",     load_baum_und_giessung),
    ("4. pumpe",               load_pumpen),
    ("5. wetter_monatswert",   load_wetter_monat),
    ("6. wetter_tageswert",    load_wetter_tag),
    ("7. sozialindex",         load_sozialindex),
    ("8. einwohner_bezirk",    load_einwohner),
    ("9. kpi_snapshot",        load_kpi),
    ("10. bewaesserung_bezirk",load_bewaesserung_bezirk),
    ("11. bewaesserung_lor",   load_bewaesserung_lor),
]


def reset_database():
    """
    Leert alle Tabellen mit TRUNCATE ... CASCADE.

    Warum TRUNCATE statt DROP?
      DROP TABLE würde die Tabellenstruktur (inkl. Foreign Key Constraints)
      löschen. Das wollen wir nicht – die Struktur haben wir ja im DDL
      sorgfältig definiert.
      TRUNCATE löscht nur die DATEN, lässt aber die Struktur stehen.
      CASCADE bedeutet: "und leere auch alle Tabellen, die über FK
      von dieser abhängen" – das ist nötig, weil z.B. giessung
      auf baum zeigt und PostgreSQL sonst blockiert.

    Reihenfolge: erst die "Kinder" (abhängige Tabellen), dann die "Eltern".
    Alternativ: ein einziges TRUNCATE ... CASCADE auf die Root-Tabellen
    reicht, PostgreSQL kaskadiert dann automatisch.
    """
    engine = get_engine()
    # Tabellen in Abhängigkeitsreihenfolge (Kinder zuerst)
    tabellen = [
        "bewaesserung_bezirk",
        "bewaesserung_lor",
        "einwohner_bezirk",
        "einwohner_herkunft",
        "kpi_snapshot",
        "sozialindex",
        "wetter_monatswert",
        "wetter_tageswert",
        "giessung",
        "baum",
        "pumpe",
        "lor_bezirksregion",
        "bezirk",
    ]
    with engine.begin() as conn:
        for t in tabellen:
            try:
                conn.execute(text(f"TRUNCATE TABLE {t} CASCADE"))
                print(f"   🧹 {t} geleert")
            except Exception:
                # Tabelle existiert noch nicht → ignorieren
                pass
    print("✅ Datenbank zurückgesetzt.\n")


def run_all():
    print("=" * 55)
    print("  GdK Pipeline – Lade alle Tabellen")
    print("=" * 55)

    print("\n▶  0. Datenbank zurücksetzen (TRUNCATE CASCADE)")
    reset_database()

    fehler = []

    for name, func in STEPS:
        print(f"\n▶  {name}")
        try:
            func()
        except Exception as e:
            # Fehler protokollieren, aber weitermachen
            print(f"   ❌ FEHLER bei {name}: {e}")
            traceback.print_exc()
            fehler.append(name)

    print("\n" + "=" * 55)
    if fehler:
        print(f"⚠️  Abgeschlossen mit {len(fehler)} Fehler(n):")
        for f in fehler:
            print(f"   - {f}")
        sys.exit(1)
    else:
        print("🎉  Alle Tabellen erfolgreich geladen!")


if __name__ == "__main__":
    run_all()