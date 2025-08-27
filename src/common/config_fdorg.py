# src/common/config_fdorg.py
import os
from pathlib import Path
from dotenv import load_dotenv

# carrega o .env da raiz, independente da pasta atual
ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env", override=True)

FDORG_BASE_URL = os.getenv("FDORG_BASE_URL", "https://api.football-data.org/v4")
FDORG_TOKEN = os.getenv("FDORG_TOKEN")
FDORG_COMPETITION = os.getenv("FDORG_COMPETITION", "BSA")
SEASON = os.getenv("SEASON", "2025")

def headers():
    return {"X-Auth-Token": FDORG_TOKEN, "Accept": "application/json"}

def validate_config():
    missing = [k for k, v in {
        "FDORG_TOKEN": FDORG_TOKEN,
        "FDORG_BASE_URL": FDORG_BASE_URL,
        "FDORG_COMPETITION": FDORG_COMPETITION,
        "SEASON": SEASON,
    }.items() if not v]
    if missing:
        raise ValueError(f"Variáveis ausentes: {', '.join(missing)}")
