import sys, pathlib, argparse, json, pandas as pd
from pathlib import Path
sys.path.append(str(pathlib.Path(__file__).resolve().parents[2]))
from src.common.io import save_table
from src.common.logging_utils import info, ok, err

def _read_json_any(p):
    b = Path(p).read_bytes()
    for enc in ("utf-8","utf-8-sig","cp1252","latin-1"):
        try: return json.loads(b.decode(enc))
        except: pass
    raise ValueError("Decode error")

def _rank_desc(s): return s.rank(ascending=False, method="dense").astype("Int64")
def _rank_asc(s):  return s.rank(ascending=True,  method="dense").astype("Int64")

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", default="data/raw/standings_fdorg.json")
    ap.add_argument("--out", default="data/curated/rankings_comparativos.csv")
    args=ap.parse_args()
    try:
        info("Lendo standings...")
        obj=_read_json_any(args.inp)
        blocks={s.get("type"):pd.DataFrame(s.get("table",[])) for s in obj.get("standings",[])}
        for k in list(blocks):
            if blocks[k].empty: del blocks[k]
        if "TOTAL" not in blocks: err("TOTAL não encontrado."); raise SystemExit(1)

        def _prep(df):
            df=df.copy()
            df["team_id"]=df["team"].apply(lambda x:x.get("id"))
            df["team_name"]=df["team"].apply(lambda x:x.get("name"))
            return df

        tot=_prep(blocks["TOTAL"])
        home=_prep(blocks.get("HOME", pd.DataFrame()))
        away=_prep(blocks.get("AWAY", pd.DataFrame()))

        if not home.empty:
            home["home_points"]=3*home["won"]+home["draw"]
            home["home_ppg"]=home["home_points"]/home["playedGames"]
            home_r=home[["team_id","home_points","home_ppg"]]
        else:
            home_r=pd.DataFrame(columns=["team_id","home_points","home_ppg"])

        if not away.empty:
            away["away_points"]=3*away["won"]+away["draw"]
            away["away_ppg"]=away["away_points"]/away["playedGames"]
            away_r=away[["team_id","away_points","away_ppg"]]
        else:
            away_r=pd.DataFrame(columns=["team_id","away_points","away_ppg"])

        base=(tot[["team_id","team_name","goalsFor","goalsAgainst"]]
              .merge(home_r, on="team_id", how="left")
              .merge(away_r, on="team_id", how="left"))

        base["rank_home"]  = _rank_desc(base["home_points"]) if "home_points"  in base.columns else pd.Series(pd.NA, index=base.index, dtype="Int64")
        base["rank_away"]  = _rank_desc(base["away_points"]) if "away_points"  in base.columns else pd.Series(pd.NA, index=base.index, dtype="Int64")
        base["rank_attack"]= _rank_desc(base["goalsFor"])
        base["rank_defense"]= _rank_asc(base["goalsAgainst"])

        for c in ["home_ppg","away_ppg"]:
            if c in base.columns:
                base[c]=base[c].round(3)

        res=base.rename(columns={
            "team_id":"id_time",
            "team_name":"time",
            "goalsFor":"gols_marcados",
            "goalsAgainst":"gols_sofridos",
            "home_points":"pontos_casa",
            "home_ppg":"ppg_casa",
            "away_points":"pontos_fora",
            "away_ppg":"ppg_fora",
            "rank_home":"rank_mandante",
            "rank_away":"rank_visitante",
            "rank_attack":"rank_ataque",
            "rank_defense":"rank_defesa",
        })

        res=res.sort_values(
            ["rank_mandante","rank_visitante","rank_ataque","rank_defesa","time"],
            na_position="last"
        ).reset_index(drop=True)

        info(f"Gerando {len(res)} linhas...")
        save_table(res, args.out)
        ok(f"Salvo em {args.out}"); ok("Concluído")
    except Exception as e:
        err(f"Falha: {e}"); raise

if __name__=="__main__": main()
