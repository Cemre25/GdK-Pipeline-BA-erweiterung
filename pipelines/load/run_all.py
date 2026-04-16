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


def run_all():
    print("=" * 55)
    print("  GdK Pipeline – Lade alle Tabellen")
    print("=" * 55)

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