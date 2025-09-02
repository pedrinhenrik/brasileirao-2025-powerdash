# src/transform/metrics_scorers_ranking_fdorg.py
import sys, pathlib, argparse, json, pandas as pd
from pathlib import Path
sys.path.append(str(pathlib.Path(__file__).resolve().parents[2]))
from src.common.io import save_table
from src.common.logging_utils import info, ok, err

def _read_json_any(p):
    b = Path(p).read_bytes()
    for enc in ("utf-8","utf-8-sig","cp1252","latin-1"):
        try:
            return json.loads(b.decode(enc))
        except Exception:
            pass
    raise ValueError("Decode error")

def _build_player_index(teams_obj):
    by_id, by_name = {}, {}
    teams = teams_obj.get("teams") or teams_obj.get("response") or []
    for t in teams:
        tid = (t or {}).get("id")
        tname = (t or {}).get("name")
        squad = (t or {}).get("squad") or []
        for pl in squad:
            pid = pl.get("id")
            nm  = (pl.get("name") or "").strip()
            pos = pl.get("position")
            nat = pl.get("nationality")
            if pid:
                by_id[pid] = {
                    "posicao": pos,
                    "nacionalidade": nat,
                    "time_id": tid,
                    "time": tname,
                    "nome": nm,
                }
            if nm:
                by_name.setdefault(nm.lower(), []).append(pid if pid else None)
    return by_id, by_name

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--scorers-in", dest="sc_in", default="data/raw/scorers_fdorg.json")
    ap.add_argument("--teams-in",   dest="tm_in", default="data/raw/teams_fdorg.json")
    ap.add_argument("--out", default="data/curated/artilharia.csv")
    ap.add_argument("--top", type=int, default=50, help="limitar top N (0 = todos)")
    args = ap.parse_args()

    try:
        info("Lendo artilharia (Football-Data.org)...")
        sc = _read_json_any(args.sc_in)
        raw = sc.get("scorers") or sc.get("response")
        if not raw:
            err("Lista de artilheiros vazia ou ausente em scorers_fdorg.json.")
            raise SystemExit(1)

        info("Indexando posições a partir de teams_fdorg.json...")
        tm = _read_json_any(args.tm_in)
        pidx, pbyname = _build_player_index(tm)

        rows = []
        for it in raw:
            player = (it.get("player") or {})
            team   = (it.get("team") or {})

            # campos variáveis entre respostas
            goals   = it.get("goals", it.get("numberOfGoals"))
            assists = it.get("assists", it.get("numberOfAssists"))
            pens    = it.get("penalties", it.get("penalty", it.get("penaltiesScored"))
            )
            # <-- NOVO: jogos (vários aliases)
            played  = (
                it.get("playedMatches")
                or it.get("appearances")
                or it.get("matches")
                or player.get("playedMatches")
                or player.get("appearances")
            )

            pid  = player.get("id")
            name = (player.get("name") or f"{player.get('firstName','')}".strip()) or None
            pos  = player.get("position")
            nat  = player.get("nationality")

            # enriquecer via teams.json
            if (not pos or not nat) and (pid or name):
                rec = None
                if pid and pid in pidx:
                    rec = pidx[pid]
                elif name:
                    for cid in pbyname.get(name.lower(), []):
                        if cid and cid in pidx:
                            rec = pidx[cid]; break
                if rec:
                    pos = pos or rec.get("posicao")
                    nat = nat or rec.get("nacionalidade")

            rows.append({
                "rank": None,  # preenchido depois
                "id_jogador": pid,
                "jogador": name,
                "nacionalidade": nat,
                "posicao": pos,
                "id_time": team.get("id"),
                "time": team.get("name"),
                "gols": goals if goals is not None else 0,
                "assistencias": assists if assists is not None else 0,
                "penaltis": pens if pens is not None else 0,
                "jogos": played if played is not None else None,  # <-- NOVO
            })

        df = pd.DataFrame(rows)
        if df.empty:
            err("Nenhum artilheiro encontrado após o parsing."); raise SystemExit(1)

        # tipos e ranking
        for c in ["gols","assistencias","penaltis","jogos"]:
            if c == "jogos":
                df[c] = pd.to_numeric(df[c], errors="coerce")  # pode ficar NaN se não existir no plano
            else:
                df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)

        df = df.sort_values(["gols","assistencias","penaltis","jogador"], ascending=[False, False, True, True]).reset_index(drop=True)
        df["rank"] = df["gols"].rank(ascending=False, method="dense").astype(int)

        if args.top and args.top > 0:
            df = df.head(args.top).reset_index(drop=True)

        cols = ["rank","id_jogador","jogador","nacionalidade","posicao","id_time","time","gols","assistencias","penaltis","jogos"]
        df = df[cols]

        info(f"Gerando ranking com {len(df)} jogadores...")
        save_table(df, args.out)
        ok(f"Artilharia salva em {args.out}")
        ok("Concluído")

    except Exception as e:
        err(f"Falha: {e}")
        raise

if __name__ == "__main__":
    main()
