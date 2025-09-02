import sys, pathlib, argparse, json, pandas as pd, re, unicodedata
from pathlib import Path

# permite executar direto sem -m se quiser
sys.path.append(str(pathlib.Path(__file__).resolve().parents[2]))

from src.common.io import save_table
from src.common.logging_utils import info, ok, err

# === mapa canônico -> HEX (na ordem: primária, secundária, terciária) ===
COLORS_HEX = {
    "Atlético Mineiro": ["#000000", "#FFFFFF"],
    "Botafogo": ["#000000", "#FFFFFF"],
    "São Paulo": ["#FFFFFF", "#FE0000", "#000000"],
    "Ceará": ["#000000", "#FFFFFF"],
    "Corinthians": ["#000000", "#FFFFFF"],
    "Cruzeiro": ["#2F529E", "#FFFFFF"],
    "Vasco": ["#000000", "#FFFFFF"],
    "Juventude": ["#009846", "#FFFFFF"],
    "Bahia": ["#006CB5", "#FFFFFF", "#ED3237"],
    "Internacional": ["#E5050F", "#FFFFFF"],
    "Vitória": ["#FF1100", "#000000"],
    "Fluminense": ["#870A28", "#00613C", "#FFFFFF"],
    "Fortaleza": ["#ED3237", "#006CB5", "#FFFFFF"],
    "Grêmio": ["#0D80BF", "#000000", "#FFFFFF"],
    "Flamengo": ["#C52613", "#000000", "#FFFFFF"],
    "Mirassol": ["#126F3D", "#F3EC0A"],
    "Palmeiras": ["#006437", "#FFFFFF"],
    "Red Bull Bragantino": ["#FFFFFF", "#000000"],
    "Santos": ["#000000", "#FFFFFF"],
    "Sport": ["#D40019", "#000000"],
}

# aliases (nomes que podem vir da API -> canônico usado no dicionário)
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

def _norm_name(s: str | None) -> str:
    if not s: return ""
    s = _strip_accents(s).lower()
    s = s.replace("&", "e").replace("-", " ")
    s = " ".join(s.split())
    return s

def _canonical_name(name: str) -> str:
    key = _norm_name(name)
    if key in ALIASES:
        return ALIASES[key]
    # heurísticas leves
    toks = key.split()
    if "vasco" in toks: return "Vasco"
    if "atletico" in toks and "mineiro" in toks: return "Atlético Mineiro"
    if "red" in toks and "bull" in toks and "bragantino" in toks: return "Red Bull Bragantino"
    if "bragantino" in toks: return "Red Bull Bragantino"
    for k in COLORS_HEX.keys():
        if _norm_name(k) == key:
            return k
    return name  # fallback: mantém original

def _read_json_any(p):
    b = Path(p).read_bytes()
    for enc in ("utf-8","utf-8-sig","cp1252","latin-1"):
        try: return json.loads(b.decode(enc))
        except Exception: pass
    raise ValueError("Falha ao decodificar JSON.")

def _split_colors(s: str | None) -> tuple[str|None, str|None, str|None]:
    if not s: return (None, None, None)
    norm = re.sub(r"\s*/\s*|\s*-\s*|;\s*|,\s*", ",", s.strip())
    parts = [p.strip() for p in norm.split(",") if p.strip()]
    parts = parts[:3] + [None] * (3 - len(parts))
    return tuple(parts[:3])

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", default="data/raw/teams_fdorg.json")
    ap.add_argument("--out", default="data/curated/info_clube.csv")
    args = ap.parse_args()

    try:
        info("Lendo teams_fdorg.json...")
        obj = _read_json_any(args.inp)
        teams = obj.get("teams") or obj.get("response") or []
        if not teams:
            err("Objeto 'teams' vazio ou ausente."); raise SystemExit(1)

        rows = []
        for t in teams:
            if not isinstance(t, dict): 
                continue

            name        = t.get("name")
            name_canon  = _canonical_name(name)

            cor1_txt, cor2_txt, cor3_txt = _split_colors(t.get("clubColors"))
            hexes = COLORS_HEX.get(name_canon, [])
            hex1, hex2, hex3 = (hexes + [None, None, None])[:3]

            area        = t.get("area") or {}
            coach       = t.get("coach") or {}
            contract    = coach.get("contract") or {}
            comps       = t.get("runningCompetitions") or []

            rows.append({
                "id_time": t.get("id"),
                "time": name,
                "nome_canonico": name_canon,
                "apelido": t.get("shortName"),
                "tla": t.get("tla"),
                "escudo_url": t.get("crest"),
                "endereco": t.get("address"),
                "site": t.get("website"),
                "fundado": t.get("founded"),
                "estadio": t.get("venue"),
                "cor_primaria": cor1_txt,
                "cor_secundaria": cor2_txt,
                "cor_terciaria": cor3_txt,
                "hex1": hex1,
                "hex2": hex2,
                "hex3": hex3,
                "cores_raw": t.get("clubColors"),
                "pais": area.get("name"),
                "pais_codigo": area.get("code"),
                "pais_bandeira_url": area.get("flag"),
                "tecnico": coach.get("name"),
                "tecnico_nacionalidade": coach.get("nationality"),
                "contrato_inicio": contract.get("start"),
                "contrato_fim": contract.get("until"),
                "competicoes_codigos": "; ".join([c.get("code") for c in comps if isinstance(c, dict) and c.get("code")]),
                "competicoes_nomes": "; ".join([c.get("name") for c in comps if isinstance(c, dict) and c.get("name")]),
                "last_updated": t.get("lastUpdated"),
            })

        df = pd.DataFrame(rows).sort_values(["nome_canonico","time"]).reset_index(drop=True)
        info(f"Gerando tabela de clubes ({len(df)} registros)...")
        save_table(df, args.out)
        ok(f"Informações dos clubes salvas em {args.out}")
        ok("Concluído")
    except Exception as e:
        err(f"Falha: {e}")
        raise

if __name__ == "__main__":
    main()
