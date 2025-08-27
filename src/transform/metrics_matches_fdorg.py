# src/transform/normalize_matches_fdorg.py
import sys, pathlib, argparse, json, pandas as pd
from pathlib import Path

# permite executar direto sem -m se quiser
sys.path.append(str(pathlib.Path(__file__).resolve().parents[2]))

from src.common.io import save_table
from src.common.logging_utils import info, ok, err

def _read_json_any(p):
    b = Path(p).read_bytes()
    for enc in ("utf-8","utf-8-sig","cp1252","latin-1"):
        try: return json.loads(b.decode(enc))
        except Exception: pass
    raise ValueError("Falha ao decodificar JSON.")

def _winner_label(status, winner_code):
    if status != "FINISHED" or not winner_code:
        return "Indefinido"
    return {"HOME_TEAM":"Mandante","AWAY_TEAM":"Visitante","DRAW":"Empate"}.get(winner_code, "Indefinido")

def _hda(ft_home, ft_away, status):
    if status != "FINISHED" or ft_home is None or ft_away is None:
        return None
    return "H" if ft_home > ft_away else ("A" if ft_home < ft_away else "D")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", default="data/raw/matches_fdorg.json")
    ap.add_argument("--out", default="data/curated/partidas.csv")
    args = ap.parse_args()

    try:
        info("Lendo partidas (Football-Data.org)...")
        obj = _read_json_any(args.inp)
        matches = obj.get("matches", [])
        rows = []
        for m in matches:
            home, away = (m.get("homeTeam") or {}), (m.get("awayTeam") or {})
            score = (m.get("score") or {})
            ft = (score.get("fullTime") or {})
            ts = pd.to_datetime(m.get("utcDate")) if m.get("utcDate") else pd.NaT

            hg, ag = ft.get("home"), ft.get("away")
            rows.append({
                "id_partida": m.get("id"),
                "data_utc": m.get("utcDate"),
                "data": (ts.date().isoformat() if not pd.isna(ts) else None),
                "hora_utc": (ts.strftime("%H:%M") if not pd.isna(ts) else None),
                "rodada": m.get("matchday"),
                "status": m.get("status"),
                "fase": m.get("stage"),
                "id_mandante": home.get("id"),
                "mandante": home.get("name"),
                "id_visitante": away.get("id"),
                "visitante": away.get("name"),
                "gols_mandante": hg,
                "gols_visitante": ag,
                "resultado": _hda(hg, ag, m.get("status")),          # H/D/A
                "vencedor": _winner_label(m.get("status"), score.get("winner")),
                "saldo_gols_mandante": (hg - ag) if (hg is not None and ag is not None) else None,
                "foi_finalizado": m.get("status") == "FINISHED",
            })

        df = pd.DataFrame(rows)
        if df.empty:
            err("Nenhuma partida encontrada."); raise SystemExit(1)

        df = df.sort_values(["data_utc","id_partida"]).reset_index(drop=True)

        info(f"Gerando tabela com {len(df)} partidas...")
        save_table(df, args.out)
        ok(f"Partidas salvas em {args.out}")
        ok("Concluído")
    except Exception as e:
        err(f"Falha: {e}")
        raise

if __name__ == "__main__":
    main()