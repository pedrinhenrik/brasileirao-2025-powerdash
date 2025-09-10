# src/transform/merge_ogol_teams_fdorg.py
from __future__ import annotations

# --- bootstrap sys.path: garante 'src/' no path ---
import sys
from pathlib import Path
THIS_FILE = Path(__file__).resolve()
SRC_DIR = THIS_FILE.parents[1]
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))
# --- fim bootstrap ---

import os
import re
import json
import unicodedata
from typing import Dict, Tuple, List
import pandas as pd

from common.logging_utils import info, ok, warn, err

PROJECT_ROOT = SRC_DIR.parent
FDORG_JSON = PROJECT_ROOT / "data" / "raw" / "teams_fdorg.json"
OGOL_CSV   = PROJECT_ROOT / "data" / "scraper" / "ogol_melhores_2025_full.csv"
OUT_CSV    = PROJECT_ROOT / "data" / "scraper" / "merged_players_2025.csv"

_punct_re = re.compile(r"[^\w\s]", flags=re.UNICODE)

def norm(s) -> str:
    # trata None/NaN/qualquer tipo
    if s is None:
        return ""
    try:
        import pandas as pd  # seguro mesmo se já importado
        if isinstance(s, float) and pd.isna(s):
            return ""
    except Exception:
        pass
    if not isinstance(s, str):
        s = str(s)

    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.casefold()
    s = _punct_re.sub(" ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def first_team(raw_team: str) -> str:
    if not isinstance(raw_team, str):
        return ""
    return raw_team.split("/")[0].strip()

def jaccard(a: str, b: str) -> float:
    ta = set(a.split()); tb = set(b.split())
    if not ta or not tb: return 0.0
    return len(ta & tb) / len(ta | tb)

# --- Mapa de IDs por time (nomes como o torcedor usa) ---
TEAM_ID_MAP: Dict[str, int] = {
    "flamengo": 1783,                 # CR Flamengo
    "palmeiras": 1769,                # SE Palmeiras
    "cruzeiro": 1771,                 # Cruzeiro EC
    "bahia": 1777,                    # EC Bahia
    "botafogo": 1770,                 # Botafogo FR
    "mirassol": 4364,                 # Mirassol FC
    "sao paulo": 1776,                # São Paulo FC
    "fluminense": 1765,               # Fluminense FC
    "bragantino": 4286,               # RB Bragantino
    "red bull bragantino": 4286,      # Alias
    "internacional": 6884,            # SC Internacional
    "ceara": 1837,                    # Ceará SC
    "atletico mineiro": 1766,         # CA/Clube Atlético Mineiro
    "gremio": 1767,                   # Grêmio FBPA
    "corinthians": 1779,              # SC Corinthians Paulista
    "vasco": 1780,                    # CR Vasco da Gama
    "santos": 6685,                   # Santos FC
    "vitoria": 1782,                  # EC Vitória
    "juventude": 4245,                # EC Juventude
    "fortaleza": 3984,                # Fortaleza EC
    "sport": 1778,                    # Sport (SC Recife)
}

def team_to_id(raw_name) -> int | None:
    # bloqueia None/NaN logo de cara
    try:
        import pandas as pd
        if raw_name is None or (isinstance(raw_name, float) and pd.isna(raw_name)):
            return None
    except Exception:
        if raw_name is None:
            return None

    key = norm(raw_name)
    if not key:
        return None

    if key in TEAM_ID_MAP:
        return TEAM_ID_MAP[key]

    junk_terms = (
        "clube de regatas", "sociedade esportiva", "esporte clube",
        "sport club", "clube atletico", "futebol clube",
        "clube", "futebol", "regatas", "sport", "sc", "ec", "fc", "fr", "ca", "cr"
    )
    k2 = key
    for j in junk_terms:
        k2 = k2.replace(" " + j + " ", " ")
        if k2.startswith(j + " "): k2 = k2[len(j)+1:]
        if k2.endswith(" " + j): k2 = k2[:-(len(j)+1)]
        k2 = re.sub(r"\s+", " ", k2).strip()

    return TEAM_ID_MAP.get(k2)

def load_fdorg_players(path: Path) -> pd.DataFrame:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    rows: List[Dict] = []
    for team in data.get("teams", []):
        team_name = team.get("shortName") or team.get("name") or ""
        team_norm = norm(team_name)
        for p in team.get("squad", []):
            rows.append({
                "fdorg_player_id": p.get("id"),
                "fdorg_player_name": p.get("name") or "",
                "fdorg_player_name_norm": norm(p.get("name") or ""),
                "fdorg_position": p.get("position") or "",
                "fdorg_nationality": p.get("nationality") or "",
                "fdorg_dob": p.get("dateOfBirth") or "",
                "fdorg_team": team_name,
                "fdorg_team_norm": team_norm,
            })
    return pd.DataFrame(rows)

def load_ogol(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str, encoding="utf-8")
    if "Equipe" in df.columns:
        df["Equipe"] = df["Equipe"].astype(str).apply(first_team)
    if "Jogador" not in df.columns:
        raise RuntimeError("CSV do oGol sem coluna 'Jogador'.")
    df["ogol_player_name"] = df["Jogador"].astype(str)
    df["ogol_player_name_norm"] = df["ogol_player_name"].apply(norm)
    df["ogol_team"] = df.get("Equipe", "").astype(str)
    df["ogol_team_norm"] = df["ogol_team"].apply(norm)
    return df

# ---------- matching (mantém 1:N) ----------
def merge_many(fd: pd.DataFrame, og: pd.DataFrame, thr_fuzzy: float = 0.60) -> Tuple[pd.DataFrame, pd.DataFrame]:
    # índices auxiliares
    fd_by_name = {n: g for n, g in fd.groupby("fdorg_player_name_norm")}
    fd_by_team = {t: g for t, g in fd.groupby("fdorg_team_norm")}

    matched_rows: List[Dict] = []
    unmatched_rows: List[Dict] = []

    for _, row in og.iterrows():
        og_name_n = row["ogol_player_name_norm"]
        og_team_n = row["ogol_team_norm"]

        candidates: List[pd.Series] = []

        # 1) exato: (nome, time)
        g1 = fd_by_name.get(og_name_n)
        if g1 is not None and not g1.empty:
            exact_team_hits = g1[g1["fdorg_team_norm"] == og_team_n]
            if not exact_team_hits.empty:
                candidates.extend(list(exact_team_hits.itertuples(index=False)))

        # 2) exato por nome (se ainda não tem candidato) — mantém todos os times
        if not candidates and (g1 is not None and not g1.empty):
            candidates.extend(list(g1.itertuples(index=False)))

        # 3) fuzzy por time (se nada ainda)
        if not candidates:
            gteam = fd_by_team.get(og_team_n)
            best_score = 0.0
            best_rows: List[pd.Series] = []
            if gteam is not None and not gteam.empty:
                for _, prow in gteam.iterrows():
                    score = jaccard(og_name_n, prow["fdorg_player_name_norm"])
                    if score > best_score:
                        best_score = score
                        best_rows = [prow]
                    elif score == best_score and score >= thr_fuzzy:
                        best_rows.append(prow)
            if best_score >= thr_fuzzy:
                candidates.extend(best_rows)

        if candidates:
            # expande 1:N: 1 linha do oGol para *cada* candidato FD.org
            for cand in candidates:
                prow = cand._asdict() if hasattr(cand, "_asdict") else cand.to_dict()
                mrow = row.to_dict()
                mrow.update(prow)
                mrow["match_score"] = jaccard(og_name_n, prow.get("fdorg_player_name_norm", ""))
                mrow["matched"] = True
                matched_rows.append(mrow)
        else:
            r = row.to_dict()
            r["fdorg_player_id"] = pd.NA
            r["fdorg_player_name"] = pd.NA
            r["fdorg_position"] = pd.NA
            r["fdorg_nationality"] = pd.NA
            r["fdorg_dob"] = pd.NA
            r["fdorg_team"] = pd.NA
            r["fdorg_team_norm"] = pd.NA
            r["fdorg_player_name_norm"] = pd.NA
            r["match_score"] = 0.0
            r["matched"] = False
            unmatched_rows.append(r)

    matched_df = pd.DataFrame(matched_rows) if matched_rows else pd.DataFrame(columns=list(og.columns)+["match_score","matched"])
    unmatched_df = pd.DataFrame(unmatched_rows) if unmatched_rows else pd.DataFrame(columns=list(og.columns)+["match_score","matched"])

    info(f"[dbg] matched (1:N): {len(matched_df)}")
    info(f"[dbg] unmatched: {len(unmatched_df)}")
    return matched_df, unmatched_df

def run(fdorg_json: Path = FDORG_JSON, ogol_csv: Path = OGOL_CSV, out_csv: Path = OUT_CSV):
    if not fdorg_json.exists():
        raise FileNotFoundError(fdorg_json)
    if not ogol_csv.exists():
        raise FileNotFoundError(ogol_csv)

    info("carregando FD.org…")
    fd = load_fdorg_players(fdorg_json)
    info(f"[dbg] FD.org jogadores: {len(fd)}")

    info("carregando oGol…")
    og = load_ogol(ogol_csv)
    info(f"[dbg] oGol linhas: {len(og)}")

    matched, og_rest = merge_many(fd, og, thr_fuzzy=0.60)

    # saída: todos os matched + os não casados (mantém métricas do oGol)
    output = pd.concat([matched, og_rest], ignore_index=True, sort=False)

    # --- atribui id_time usando ogol_team e fallback em fdorg_team ---
    def resolve_id(row: pd.Series) -> int | None:
        tid = team_to_id(row.get("ogol_team"))
        if tid is None:
            tid = team_to_id(row.get("fdorg_team"))
        return tid


    output["id_time"] = output.apply(resolve_id, axis=1)

    miss = int(output["id_time"].isna().sum())
    if miss > 0:
        warn(f"[id_time] {miss} linhas sem id_time (verifique nomes/aliases).")

    # ordenação de colunas
    fd_cols = ["fdorg_player_id","fdorg_player_name","fdorg_position","fdorg_nationality","fdorg_dob","fdorg_team"]
    og_aux_drop = {"ogol_player_name_norm","ogol_team_norm","fdorg_team_norm","fdorg_player_name_norm"}
    og_cols = [c for c in og.columns if c not in og_aux_drop]
    tech_cols = ["ogol_player_name","ogol_team","match_score","matched"]

    ordered = (
        [c for c in fd_cols if c in output.columns]
        + ["id_time"]  # garantir que id_time saia destacado
        + [c for c in og_cols if c in output.columns]
        + [c for c in tech_cols if c in output.columns]
    )
    output = output[[c for c in ordered if c in output.columns]]

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    output.to_csv(out_csv, index=False, encoding="utf-8-sig")

    ok(f"merge gerado: {out_csv}")
    ok(f"linhas totais no resultado: {len(output)}")

if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        err(f"falha no merge: {e}")
        raise
