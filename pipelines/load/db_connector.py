import os
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine
import urllib.parse  # Für Sonderzeichen im Passwort

# 1. Dynamisch den Pfad zum Hauptverzeichnis finden
# 'Path(__file__)' ist der Ort dieses Skripts (pipelines/load/db_connector.py)
# .parents[2] geht drei Ebenen hoch zum Hauptordner (GdK-Pipeline-BA-erweiterung)
base_dir = Path(__file__).resolve().parents[2]
dotenv_path = base_dir / ".env"

# 2. .env laden
load_dotenv(dotenv_path=dotenv_path)

# 3. Variablen abrufen
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
dbname = os.getenv("DB_NAME")

# --- DEBUGGING CHECK ---
if password is None:
    raise ValueError(f"FEHLER: .env Datei wurde unter {dotenv_path} nicht gefunden oder ist leer!")

# 2. Variablen aus der Umgebung abrufen
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
dbname = os.getenv("DB_NAME")

# 3. Sicherheits-Check 
# Falls Passwort Sonderzeichen wie @, :, / oder % enthält, 
# muss es für die URL "escaped" werden.
safe_password = urllib.parse.quote_plus(password)

# 4. Den Connection-String
connection_string = f"postgresql://{user}:{safe_password}@{host}:{port}/{dbname}"

# 5. Engine erstellen
engine = create_engine(connection_string)

# Test
print(f"Verbindung zu {dbname} auf {host} bereit!")