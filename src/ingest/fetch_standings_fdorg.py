import sys, pathlib, json, requests
from pathlib import Path
from tenacity import retry, stop_after_attempt, wait_exponential

# hotfix para execução direta: python src/ingest/fetch_standings_fdorg.py
sys.path.append(str(pathlib.Path(__file__).resolve().parents[2]))

from src.common.config_fdorg import FDORG_BASE_URL, FDORG_COMPETITION, SEASON, headers, validate_config
from src.common.logging_utils import info, ok, err

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=8))
def _call_api():
    url = f"{FDORG_BASE_URL}/competitions/{FDORG_COMPETITION}/standings"
    params = {"season": SEASON}
    r = requests.get(url, params=params, headers=headers(), timeout=30)
    r.raise_for_status()
    return r.json()

def main():
    validate_config()
    info("Coletando standings no Football-Data.org...")
    data = _call_api()
    out = Path("data/raw/standings_fdorg.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(data, ensure_ascii=False, indent=2),encoding="utf-8")
    ok("Standings salvos em data/raw/standings_fdorg.json")

if __name__ == "__main__":
    try:
        main()
        ok("Concluído")
    except Exception as e:
        err(f"Falha: {e}")
        raise