import sys, pathlib, json, argparse, pandas as pd
from pathlib import Path

# permite executar direto sem -m se quiser
sys.path.append(str(pathlib.Path(__file__).resolve().parents[2]))

from src.common.io import save_table
from src.common.logging_utils import info, ok, err

def _safe_div(a, b):
    try:
        a = float(a); b = float(b)
        return round(a/b, 3) if b else 0.0
    except Exception:
        return 0.0

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", default="data/raw/standings_fdorg.json")
    ap.add_argument("--out", default="data/curated/desempenho_times.csv")
    args = ap.parse_args()

    info("Lendo standings (TOTAL/HOME/AWAY)...")
    obj = json.loads(Path(args.inp).read_text(encoding="utf-8"))
    standings = obj.get("standings", [])

    # Indexa por tipo de tabela
    tables = {s.get("type"): s.get("table", []) for s in standings}

    total = { row["team"]["id"]: row for row in tables.get("TOTAL", []) if "team" in row }
    home  = { row["team"]["id"]: row for row in tables.get("HOME",  []) if "team" in row }
    away  = { row["team"]["id"]: row for row in tables.get("AWAY",  []) if "team" in row }

    info("Calculando métricas de desempenho por time...")
    rows = []
    for tid, t in total.items():
        team_name = (t.get("team") or {}).get("name")
        played    = t.get("playedGames", 0)
        won       = t.get("won", 0)
        draw      = t.get("draw", 0)
        lost      = t.get("lost", 0)
        gf        = t.get("goalsFor", 0)
        ga        = t.get("goalsAgainst", 0)
        gd        = t.get("goalDifference", 0)
        points    = t.get("points", 0)

        # PPG total
        ppg_total = _safe_div(points, played)

        # PPG casa (derivado de HOME: 3*won + 1*draw)
        h = home.get(tid, {})
        h_played = h.get("playedGames", 0)
        h_points = 3*(h.get("won",0)) + (h.get("draw",0))
        ppg_home = _safe_div(h_points, h_played)

        # PPG fora (derivado de AWAY)
        a = away.get(tid, {})
        a_played = a.get("playedGames", 0)
        a_points = 3*(a.get("won",0)) + (a.get("draw",0))
        ppg_away = _safe_div(a_points, a_played)

        rows.append({
            "id_time": tid,
            "time": team_name,
            "jogos": played,
            "vitorias": won,
            "empates": draw,
            "derrotas": lost,
            "gols_marcados": gf,
            "gols_sofridos": ga,
            "saldo_gols": gd,
            "pontos": points,
            "ppg_geral": ppg_total,
            "ppg_casa": ppg_home,
            "ppg_fora": ppg_away,
            "gols_por_jogo": _safe_div(gf, played),
            "gols_sofridos_por_jogo": _safe_div(ga, played),
        })

    df = pd.DataFrame(rows).sort_values(
        ["ppg_geral","saldo_gols","gols_marcados"], ascending=[False, False, False]
    ).reset_index(drop=True)

    info(f"Gerando tabela com {len(df)} times...")
    save_table(df, args.out)
    ok(f"Métricas salvas em {args.out}")

if __name__ == "__main__":
    try:
        main()
        ok("Concluído")
    except Exception as e:
        err(f"Falha: {e}")
        raise