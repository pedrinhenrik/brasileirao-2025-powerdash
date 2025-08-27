import sys, pathlib, requests
sys.path.append(str(pathlib.Path(__file__).resolve().parents[2]))

from src.common.config_fdorg import FDORG_BASE_URL, FDORG_COMPETITION, headers, validate_config
from src.common.io import save_json

def main():
    validate_config()
    url = f"{FDORG_BASE_URL}/competitions/{FDORG_COMPETITION}/teams"
    r = requests.get(url, headers=headers(), timeout=30)
    r.raise_for_status()
    save_json("data/raw/teams_fdorg.json", r.json())

if __name__ == "__main__":
    main()