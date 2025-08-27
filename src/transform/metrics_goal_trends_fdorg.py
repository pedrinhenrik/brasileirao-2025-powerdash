# src/transform/metrics_goal_trends_fdorg.py
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
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", default="data/raw/matches_fdorg.json")
    ap.add_argument("--out", default="data/curated/tendencias_gols_times.csv")
    args = ap.parse_args()
    try:
        info("Lendo partidas...")
        obj = _read_json_any(args.inp)
        rows=[]
        for m in obj.get("matches", []):
            if m.get("status")!="FINISHED": continue
            home,away=m.get("homeTeam",{}),m.get("awayTeam",{})
            ft=(m.get("score") or {}).get("fullTime",{})
            rows.append({"utc":m.get("utcDate"),"home_id":home.get("id"),"home_name":home.get("name"),
                         "away_id":away.get("id"),"away_name":away.get("name"),
                         "hg":ft.get("home"),"ag":ft.get("away")})
        df=pd.DataFrame(rows)
        if df.empty: err("Nenhum jogo finalizado."); raise SystemExit(1)

        total_goals=df["hg"]+df["ag"]
        btts=(df["hg"]>0)&(df["ag"]>0)
        over15=total_goals>1; over25=total_goals>2; over35=total_goals>3

        home=df.assign(team_id=df.home_id,team_name=df.home_name,is_home=True,gf=df.hg,ga=df.ag)
        away=df.assign(team_id=df.away_id,team_name=df.away_name,is_home=False,gf=df.ag,ga=df.hg)
        long=pd.concat([home,away],ignore_index=True)

        # Alinhamento correto (home de todos os jogos, depois away de todos os jogos)
        long["btts"]   = pd.concat([btts,   btts],   ignore_index=True)
        long["over15"] = pd.concat([over15, over15], ignore_index=True)
        long["over25"] = pd.concat([over25, over25], ignore_index=True)
        long["over35"] = pd.concat([over35, over35], ignore_index=True)

        g=long.groupby("team_id", dropna=False)
        base=g.agg(team_name=("team_name","first"),matches=("team_id","count"),
                   btts_rate=("btts","mean"),over15_rate=("over15","mean"),
                   over25_rate=("over25","mean"),over35_rate=("over35","mean")).reset_index()

        gh=long[long.is_home].groupby("team_id").agg(home_btts_rate=("btts","mean"),
                                                     home_over25_rate=("over25","mean"))
        ga=long[~long.is_home].groupby("team_id").agg(away_btts_rate=("btts","mean"),
                                                      away_over25_rate=("over25","mean"))

        res=base.merge(gh,on="team_id",how="left").merge(ga,on="team_id",how="left")
        for c in ["btts_rate","over15_rate","over25_rate","over35_rate",
                  "home_btts_rate","home_over25_rate","away_btts_rate","away_over25_rate"]:
            res[c]=res[c].round(3)

        # nomes em português
        res = res.rename(columns={
            "team_id":"id_time",
            "team_name":"time",
            "matches":"jogos",
            "btts_rate":"taxa_btts",
            "over15_rate":"taxa_over_1_5",
            "over25_rate":"taxa_over_2_5",
            "over35_rate":"taxa_over_3_5",
            "home_btts_rate":"taxa_btts_casa",
            "home_over25_rate":"taxa_over_2_5_casa",
            "away_btts_rate":"taxa_btts_fora",
            "away_over25_rate":"taxa_over_2_5_fora",
        })

        res = res.sort_values(["taxa_over_2_5","taxa_btts"], ascending=False).reset_index(drop=True)

        info(f"Gerando {len(res)} linhas...")
        save_table(res,args.out)
        ok(f"Salvo em {args.out}"); ok("Concluído")
    except Exception as e:
        err(f"Falha: {e}"); raise

if __name__=="__main__": main()
