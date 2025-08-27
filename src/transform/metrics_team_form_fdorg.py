import sys, pathlib, argparse, json, pandas as pd, unicodedata
from pathlib import Path

# permite executar direto sem -m se quiser
sys.path.append(str(pathlib.Path(__file__).resolve().parents[2]))

from src.common.io import save_table
from src.common.logging_utils import info, ok, err

# ---- catálogo canônico (nome -> logo) ----
LOGOS = {
    "Atlético Mineiro": "https://upload.wikimedia.org/wikipedia/commons/5/5f/Atletico_mineiro_galo.png",
    "Botafogo": "https://upload.wikimedia.org/wikipedia/commons/5/52/Botafogo_de_Futebol_e_Regatas_logo.svg",
    "São Paulo": "https://upload.wikimedia.org/wikipedia/commons/6/6f/Brasao_do_Sao_Paulo_Futebol_Clube.svg",
    "Ceará": "https://upload.wikimedia.org/wikipedia/commons/3/38/Cear%C3%A1_Sporting_Club_logo.svg",
    "Corinthians": "https://upload.wikimedia.org/wikipedia/pt/b/b4/Corinthians_simbolo.png",
    "Cruzeiro": "https://upload.wikimedia.org/wikipedia/commons/9/90/Cruzeiro_Esporte_Clube_%28logo%29.svg",
    "Vasco": "https://upload.wikimedia.org/wikipedia/pt/a/ac/CRVascodaGama.png",
    "Juventude": "https://upload.wikimedia.org/wikipedia/commons/5/51/EC_Juventude.svg",
    "Bahia": "https://upload.wikimedia.org/wikipedia/pt/9/90/ECBahia.png",
    "Internacional": "https://upload.wikimedia.org/wikipedia/commons/a/ae/SC_Internacional_Brazil_Logo.svg",
    "Vitória": "https://upload.wikimedia.org/wikipedia/pt/3/34/Esporte_Clube_Vit%C3%B3ria_logo.png",
    "Fluminense": "https://upload.wikimedia.org/wikipedia/commons/1/1d/FFC_crest.svg",
    "Fortaleza": "https://upload.wikimedia.org/wikipedia/commons/9/9e/Escudo_do_Fortaleza_EC.png",
    "Grêmio": "https://upload.wikimedia.org/wikipedia/commons/0/08/Gremio_logo.svg",
    "Flamengo": "https://upload.wikimedia.org/wikipedia/commons/9/93/Flamengo-RJ_%28BRA%29.png",
    "Mirassol": "https://upload.wikimedia.org/wikipedia/commons/5/5b/Mirassol_FC_logo.png",
    "Palmeiras": "https://upload.wikimedia.org/wikipedia/commons/1/10/Palmeiras_logo.svg",
    "Red Bull Bragantino": "https://upload.wikimedia.org/wikipedia/pt/9/9e/RedBullBragantino.png",
    "Santos": "https://upload.wikimedia.org/wikipedia/commons/3/35/Santos_logo.svg",
    "Sport": "https://upload.wikimedia.org/wikipedia/pt/1/17/Sport_Club_do_Recife.png",
}

# ---- aliases (formas normalizadas -> nome canônico) ----
ALIASES = {
    # Atlético Mineiro
    "ca mineiro": "Atlético Mineiro",
    "clube atletico mineiro": "Atlético Mineiro",
    "atletico mg": "Atlético Mineiro",
    "atletico mineiro": "Atlético Mineiro",
    # Botafogo
    "botafogo fr": "Botafogo",
    "botafogo": "Botafogo",
    # São Paulo
    "sao paulo fc": "São Paulo",
    "sao paulo": "São Paulo",
    # Ceará
    "ceara sc": "Ceará",
    "ceara": "Ceará",
    # Corinthians
    "sc corinthians paulista": "Corinthians",
    "corinthians": "Corinthians",
    # Cruzeiro
    "cruzeiro ec": "Cruzeiro",
    "cruzeiro": "Cruzeiro",
    # Vasco
    "cr vasco da gama": "Vasco",
    "vasco da gama": "Vasco",
    "vasco": "Vasco",
    # Juventude
    "ec juventude": "Juventude",
    "juventude rs": "Juventude",
    "juventude": "Juventude",
    # Bahia
    "ec bahia": "Bahia",
    "bahia": "Bahia",
    # Internacional
    "sc internacional": "Internacional",
    "internacional": "Internacional",
    # Vitória
    "ec vitoria": "Vitória",
    "vitoria": "Vitória",
    # Fluminense
    "fluminense fc": "Fluminense",
    "fluminense": "Fluminense",
    # Fortaleza
    "fortaleza ec": "Fortaleza",
    "fortaleza": "Fortaleza",
    # Grêmio
    "gremio fbpa": "Grêmio",
    "gremio": "Grêmio",
    # Flamengo
    "cr flamengo": "Flamengo",
    "flamengo": "Flamengo",
    # Mirassol
    "mirassol fc": "Mirassol",
    "mirassol": "Mirassol",
    # Palmeiras
    "se palmeiras": "Palmeiras",
    "palmeiras": "Palmeiras",
    # Bragantino
    "rb bragantino": "Red Bull Bragantino",
    "red bull bragantino": "Red Bull Bragantino",
    "bragantino": "Red Bull Bragantino",
    # Santos
    "santos fc": "Santos",
    "santos": "Santos",
    # Sport
    "sc recife": "Sport",
    "sport recife": "Sport",
    "sport club do recife": "Sport",
    "sport": "Sport",
}

def _strip_accents(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))

def _norm(s: str) -> str:
    if not s: return ""
    s = _strip_accents(s).lower()
    s = s.replace("&", "e").replace("-", " ")
    s = " ".join(s.split())
    return s

def _canonical_and_logo(team_name: str) -> tuple[str, str]:
    key = _norm(team_name)
    # remoções leves de prefixos comuns
    for prefix in ["ec ", "sc ", "se ", "cr ", "ca ", "rb "]:
        if key.startswith(prefix):
            alias = key
            # tenta direto
            if alias in ALIASES: 
                name = ALIASES[alias]
                return name, LOGOS.get(name, "")
    # tentativa direta
    if key in ALIASES:
        name = ALIASES[key]
        return name, LOGOS.get(name, "")
    # fallback por heurística simples
    tokens = key.split()
    if "vasco" in tokens: return "Vasco", LOGOS["Vasco"]
    if "atletico" in tokens and "mineiro" in tokens: return "Atlético Mineiro", LOGOS["Atlético Mineiro"]
    if "red" in tokens and "bull" in tokens and "bragantino" in tokens: return "Red Bull Bragantino", LOGOS["Red Bull Bragantino"]
    if "bragantino" in tokens: return "Red Bull Bragantino", LOGOS["Red Bull Bragantino"]
    for cand in LOGOS.keys():
        if _norm(cand) == key:
            return cand, LOGOS[cand]
    return team_name, ""  # não mapeado

def _read_json_any(path):
    data = Path(path).read_bytes()
    for enc in ("utf-8", "utf-8-sig", "cp1252", "latin-1"):
        try:
            return json.loads(data.decode(enc))
        except Exception:
            continue
    raise ValueError("Não foi possível decodificar o JSON (tentado utf-8/utf-8-sig/cp1252/latin-1).")

def _result_for_team(gf, ga):
    if gf is None or ga is None: return None
    return "W" if gf > ga else ("L" if gf < ga else "D")

def _current_streak(seq, symbol):
    n = 0
    for r in reversed(seq):
        if r == symbol: n += 1
        else: break
    return n

def _longest_streak(seq, symbol):
    best = cur = 0
    for r in seq:
        if r == symbol:
            cur += 1; best = max(best, cur)
        else:
            cur = 0
    return best

def _last5(seq): return "".join(seq[-5:]) if seq else ""

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", default="data/raw/matches_fdorg.json")
    ap.add_argument("--out", default="data/curated/forma_times.csv")
    args = ap.parse_args()

    try:
        info("Lendo partidas do Football-Data.org...")
        obj = _read_json_any(args.inp)
        matches = obj.get("matches", [])

        rows = []
        for m in matches:
            if m.get("status") != "FINISHED": continue
            home = m.get("homeTeam", {}) or {}
            away = m.get("awayTeam", {}) or {}
            ft = (m.get("score") or {}).get("fullTime", {}) or {}
            rows.append({
                "utc_date": m.get("utcDate"),
                "matchday": m.get("matchday"),
                "home_id": home.get("id"),
                "home_name": home.get("name"),
                "away_id": away.get("id"),
                "away_name": away.get("name"),
                "home_goals": ft.get("home"),
                "away_goals": ft.get("away"),
            })
        df = pd.DataFrame(rows)
        if df.empty:
            err("Nenhum jogo finalizado encontrado no arquivo de entrada."); raise SystemExit(1)

        home = df.assign(team_id=df.home_id, team_name=df.home_name, gf=df.home_goals, ga=df.away_goals)
        away = df.assign(team_id=df.away_id, team_name=df.away_name, gf=df.away_goals, ga=df.home_goals)
        long = pd.concat([home, away], ignore_index=True)[["team_id","team_name","utc_date","matchday","gf","ga"]]

        long["res"] = long.apply(lambda r: _result_for_team(r.gf, r.ga), axis=1)
        long = long.sort_values(["team_id","utc_date"], kind="stable")

        info("Calculando forma (últimos 5) e padronizando nomes...")
        out_rows = []
        for tid, g in long.groupby("team_id", sort=False):
            team_name = g["team_name"].iloc[0]
            canon_name, logo = _canonical_and_logo(team_name)

            seq = g["res"].dropna().tolist()
            out_rows.append({
                "id_time": tid,
                "time": team_name,            # nome vindo da API
                "nome_time": canon_name,      # nome padronizado (para exibir no BI)
                "escudo_url": logo,           # logo correspondente
                "forma_ultimos5": _last5(seq),
                "sequencia_vitorias_atual": _current_streak(seq, "W"),
                "sequencia_derrotas_atual": _current_streak(seq, "L"),
                "maior_sequencia_vitorias": _longest_streak(seq, "W"),
                "maior_sequencia_derrotas": _longest_streak(seq, "L"),
            })

        res = pd.DataFrame(out_rows).sort_values(["nome_time","time"]).reset_index(drop=True)
        info(f"Gerando tabela com {len(res)} times...")
        save_table(res, args.out)
        ok(f"Forma com nomes padronizados salva em {args.out}")
        ok("Concluído")

    except Exception as e:
        err(f"Falha: {e}")
        raise

if __name__ == "__main__":
    main()