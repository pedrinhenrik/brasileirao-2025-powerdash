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

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--matches-in", default="data/raw/matches_fdorg.json")
    ap.add_argument("--standings-in", default="data/raw/standings_fdorg.json")
    ap.add_argument("--out", default="data/curated/forca_calendario_proximos5.csv")
    args=ap.parse_args()
    try:
        info("Lendo partidas e standings...")
        mob=_read_json_any(args.matches_in); sob=_read_json_any(args.standings_in)
        mrows=[]
        for m in mob.get("matches",[]):
            home,away=m.get("homeTeam",{}),m.get("awayTeam",{})
            mrows.append({"utc":m.get("utcDate"),"status":m.get("status"),
                          "home_id":home.get("id"),"home_name":home.get("name"),
                          "away_id":away.get("id"),"away_name":away.get("name")})
        df=pd.DataFrame(mrows)
        if df.empty: err("Sem partidas."); raise SystemExit(1)
        df["utc"]=pd.to_datetime(df["utc"])

        pos={}
        for s in sob.get("standings",[]):
            if s.get("type")!="TOTAL": continue
            for t in s.get("table",[]):
                pos[t["team"]["id"]]=t.get("position")

        fin=df[df["status"]=="FINISHED"]
        frames=[]
        long=pd.concat([
            df.assign(team_id=df.home_id, team_name=df.home_name, opp_id=df.away_id, opp_name=df.away_name),
            df.assign(team_id=df.away_id, team_name=df.away_name, opp_id=df.home_id, opp_name=df.home_name)
        ], ignore_index=True)

        for tid, g in long.groupby("team_id"):
            team_name=g["team_name"].iloc[0]
            hist=fin[(fin.home_id==tid)|(fin.away_id==tid)].sort_values("utc")
            future=g[g["status"].isin(["SCHEDULED","TIMED"])].sort_values("utc").head(5)
            if future.empty:
                frames.append({
                    "id_time": tid,
                    "time": team_name,
                    "proximos_jogos": 0,
                    "media_posicao_adversarios": None,
                    "melhor_posicao_adversario": None,
                    "pior_posicao_adversario": None,
                    "media_dias_descanso": None
                })
                continue

            opp_positions=[pos.get(x, None) for x in future["opp_id"].tolist()]
            dates=future["utc"].tolist()
            gaps=[(dates[i]-dates[i-1]).days for i in range(1,len(dates))] if len(dates)>=2 else []

            frames.append({
                "id_time": tid,
                "time": team_name,
                "proximos_jogos": len(future),
                "media_posicao_adversarios": (pd.Series(opp_positions).dropna().mean() if any(opp_positions) else None),
                "melhor_posicao_adversario": (pd.Series(opp_positions).dropna().min() if any(opp_positions) else None),
                "pior_posicao_adversario": (pd.Series(opp_positions).dropna().max() if any(opp_positions) else None),
                "media_dias_descanso": (pd.Series(gaps).mean() if gaps else None)
            })

        res=pd.DataFrame(frames)
        if not res.empty:
            res["media_posicao_adversarios"]=res["media_posicao_adversarios"].round(2)
            res["media_dias_descanso"]=res["media_dias_descanso"].round(2)
            res=res.sort_values(["media_posicao_adversarios","time"], na_position="last").reset_index(drop=True)

        info(f"Gerando {len(res)} linhas...")
        save_table(res, args.out)
        ok(f"Salvo em {args.out}"); ok("Concluído")
    except Exception as e:
        err(f"Falha: {e}"); raise

if __name__=="__main__": main()
