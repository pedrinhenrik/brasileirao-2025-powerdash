import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_FOOTBALL_KEY")
BASE_URL = os.getenv("BASE_URL")
LEAGUE_ID = os.getenv("LEAGUE_ID")
SEASON = os.getenv("SEASON")

def validate_config():
    missing = [k for k, v in {
        "API_FOOTBALL_KEY": API_KEY,
        "BASE_URL": BASE_URL,
        "LEAGUE_ID": LEAGUE_ID,
        "SEASON": SEASON,
    }.items() if not v]
    if missing:
        raise ValueError(f"Variáveis ausentes: {', '.join(missing)}")
