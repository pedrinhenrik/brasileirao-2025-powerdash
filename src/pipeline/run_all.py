# src/pipeline/run_all.py
import sys, pathlib, subprocess
from pathlib import Path

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2]))
from src.common.logging_utils import info, ok, err
from src.common.config_fdorg import FDORG_COMPETITION, SEASON

PY = sys.executable

STEPS = [
    # Ingestão (raw)
    (PY, "-m", "src.ingest.fetch_teams_fdorg",     "--out", "data/raw/teams_fdorg.json"),
    (PY, "-m", "src.ingest.fetch_standings_fdorg", "--out", "data/raw/standings_fdorg.json"),
    (PY, "-m", "src.ingest.fetch_matches_fdorg",   "--out", "data/raw/matches_fdorg.json"),
    (PY, "-m", "src.ingest.fetch_scorers_fdorg",   "--out", "data/raw/scorers_fdorg.json"),

    # Derivados baseados em TEAMS (clubes, cores, hex, etc.)
    (PY, "-m", "src.transform.club_info_from_teams_fdorg",
        "--in", "data/raw/teams_fdorg.json",
        "--out", "data/curated/info_clube.csv"),

    # Métricas (curated)
    (PY, "-m", "src.transform.metrics_team_performance_fdorg",
        "--in", "data/raw/standings_fdorg.json",
        "--out", "data/curated/desempenho_times.csv"),

    (PY, "-m", "src.transform.metrics_team_form_fdorg",
        "--in", "data/raw/matches_fdorg.json",
        "--out", "data/curated/forma_times.csv"),

    (PY, "-m", "src.transform.metrics_goal_trends_fdorg",
        "--in", "data/raw/matches_fdorg.json",
        "--out", "data/curated/tendencias_gols_times.csv"),

    (PY, "-m", "src.transform.metrics_comparative_rankings_fdorg",
        "--in", "data/raw/standings_fdorg.json",
        "--out", "data/curated/rankings_comparativos.csv"),

    (PY, "-m", "src.transform.metrics_calendar_strength_fdorg",
        "--matches-in","data/raw/matches_fdorg.json",
        "--standings-in","data/raw/standings_fdorg.json",
        "--out","data/curated/forca_calendario_proximos5.csv"),

    (PY, "-m", "src.transform.build_dim_calendario",
        "--matches-in","data/raw/matches_fdorg.json",
        "--out","data/curated/dim_calendario.csv"),

    (PY, "-m", "src.transform.metrics_matches_fdorg",
        "--in", "data/raw/matches_fdorg.json",
        "--out", "data/curated/matches_metrics.csv"),

    (PY, "-m", "src.transform.metrics_scorers_ranking_fdorg",
        "--scorers-in","data/raw/scorers_fdorg.json",
        "--teams-in","data/raw/teams_fdorg.json",
        "--out","data/curated/artilharia.csv",
        "--top","50"),

    # Merge com paths explícitos (saída em curated)
    (PY, "-m", "src.transform.merge_ogol_teams_fdorg",
    "--fdorg-json", "data/raw/teams_fdorg.json",
    "--ogol-csv",   "data/scraper/ogol_melhores_2025_full.csv",
    "--out",        "data/scraper/merged_players_2025.csv"),

    # Scraper ogol (Desempenhos)
    (PY, "-m", "src.scraper.scraper_ogol",
        "--url", "https://www.ogol.com.br/edicao/brasileirao-serie-a-2025/194851/melhores-desempenhos",
        "--out", "data/scraper/ogol_melhores_2025_full.csv"),

    # Scraper UFMG
    (PY, "-m", "src.scraper.scraper_ufmg", 
    "--out", "data/curated/prob_ufmg.csv",
    "--info", "data/curated/info_clube.csv"),

]

def run(cmd):
    info(" ".join(map(str, cmd)))
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        if res.stdout.strip(): print(res.stdout.strip())
        if res.stderr.strip(): err(res.stderr.strip())
        raise SystemExit(res.returncode)
    out = (res.stdout or "").strip()
    if out: print(out)

def main():
    info(f"Pipeline Brasileirão — competição={FDORG_COMPETITION}, temporada={SEASON}")
    Path("data/curated").mkdir(parents=True, exist_ok=True)
    for step in STEPS:
        run(step)
    ok("Pipeline concluído com sucesso.")

if __name__ == "__main__":
    main()