import json
import requests
from pathlib import Path
from tenacity import retry, stop_after_attempt, wait_exponential

from src.common.config import API_KEY, BASE_URL, LEAGUE_ID, SEASON, validate_config
from src.common.logging_utils import info, ok, err

HEADERS = {
    "x-apisports-key": API_KEY,
    "Accept": "application/json"
}

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=8))
def _call_api():
    url = f"{BASE_URL}/standings"
    params = {"league": LEAGUE_ID, "season": SEASON}
    r = requests.get(url, params=params, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.json()

def main():
    validate_config()
    info("Coletando standings...")
    data = _call_api()
    out = Path("data/raw/standings.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(data, ensure_ascii=False, indent=2))
    ok("Standings salvos em data/raw/standings.json")

if __name__ == "__main__":
    try:
        main()
        ok("Concluído")
    except Exception as e:
        err(f"Falha: {e}")
        raise
