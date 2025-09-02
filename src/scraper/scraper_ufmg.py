# src/scraper/scraper.py
import os, sys
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.common import logging_utils as log

import unicodedata
import requests
import pandas as pd
from bs4 import BeautifulSoup

REBAIX_URL = "https://www.mat.ufmg.br/futebol/rebaixamento_seriea/"
CAMPEAO_URL = "https://www.mat.ufmg.br/futebol/campeao_seriea/"
TABLE_ID = "tabelaCL"
HEADERS = {"User-Agent": "Mozilla/5.0"}

OUTPUT_DIR = os.path.join("data", "prob")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "prob_ufmg.csv")

INFO_CLUBE_PATH = os.path.join("data", "curated", "info_clube.csv")  # deve conter colunas: id_time, nome_canonico

# ----------------- helpers -----------------
def _strip_accents(s: str) -> str:
    if not isinstance(s, str):
        return s
    nfkd = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in nfkd if not unicodedata.combining(ch))

def _norm_name(s: str) -> str:
    # trim, remove acentos e upper para join robusto
    s = (s or "").strip()
    s = _strip_accents(s)
    return s.upper()

# ----------------- scraping -----------------
def get_html(url: str) -> str:
    log.info(f"Baixando {url} ...")
    resp = requests.get(url, headers=HEADERS, timeout=20)
    resp.raise_for_status()
    log.ok(f"Sucesso no download: {url}")
    return resp.text

def parse_table(html: str, metric: str) -> pd.DataFrame:
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", id=TABLE_ID) or soup.find("table")
    if table is None:
        log.err("Nenhuma <table> encontrada.")
        raise ValueError("Tabela não encontrada no HTML.")

    rows = []
    body = table.find("tbody") or table
    for tr in body.find_all("tr"):
        tds = [td.get_text(strip=True) for td in tr.find_all("td")]
        if len(tds) < 3:
            continue
        team = tds[1].strip().replace("\xa0", " ")
        prob_txt = tds[2].replace("%", "").replace(",", ".").strip()
        try:
            prob = float(prob_txt)
        except ValueError:
            log.warn(f"Valor inválido de prob: '{prob_txt}' para {team}; setando 0.0")
            prob = 0.0
        rows.append({"time": team, metric: prob})

    if not rows:
        log.err("Tabela encontrada, mas sem linhas de dados.")
        raise ValueError("Tabela vazia.")

    df = (
        pd.DataFrame(rows)
        .assign(time=lambda d: d["time"].str.strip())
        .sort_values(["time", metric], ascending=[True, False])
        .drop_duplicates(subset=["time"], keep="first")
        .reset_index(drop=True)
    )
    log.ok(f"Extraído {len(df)} times para {metric}")
    return df

# ----------------- pipeline -----------------
def main():
    log.info("Iniciando coleta de probabilidades do Brasileirão 2025...")

    # 1) Scrape
    df_reb = parse_table(get_html(REBAIX_URL), "prob_rebaixamento_pct")
    df_cam = parse_table(get_html(CAMPEAO_URL), "prob_campeao_pct")

    # 2) Consolidado
    df = (
        pd.merge(df_reb, df_cam, on="time", how="outer")
        .fillna(0)
        .sort_values("time")
        .reset_index(drop=True)
    )

    # 3) Timestamp BRT
    ts = pd.Timestamp.now("America/Sao_Paulo")
    df.insert(0, "coleta_ts_brt", ts.strftime("%Y-%m-%d %H:%M:%S"))

        # 4) Enriquecimento com id_time (info_clube.csv)
    if not os.path.exists(INFO_CLUBE_PATH):
        log.err(f"Arquivo de referência não encontrado: {INFO_CLUBE_PATH}")
        raise FileNotFoundError(INFO_CLUBE_PATH)

    ref = pd.read_csv(INFO_CLUBE_PATH)

    # sanity check
    cols_lower = {c.lower(): c for c in ref.columns}
    if not {"id_time", "nome_canonico"}.issubset(set(cols_lower.keys())):
        log.err("info_clube.csv deve conter colunas: id_time, nome_canonico")
        raise ValueError("Estrutura inesperada em info_clube.csv")

    # normaliza e cria chave da referência
    ref = ref.rename(columns={cols_lower["id_time"]: "id_time",
                              cols_lower["nome_canonico"]: "nome_canonico"})
    ref["__key"] = ref["nome_canonico"].map(_norm_name)

    # chave crua do UFMG (ainda sem sinônimos)
    df["__key"] = df["time"].map(_norm_name)

    # ---- SINÔNIMOS (UFMG -> nome_canonico) ----
    # mapeie pela forma NORMALIZADA (_norm_name) para evitar acentos/caixa
    synonyms = {
        _norm_name("VASCO DA GAMA"): _norm_name("Vasco"),
        _norm_name("BRAGANTINO"): _norm_name("Red Bull Bragantino"),
        _norm_name("ATLÉTICO"): _norm_name("Atlético Mineiro"),
        # adicione aqui outros casos problemáticos se aparecerem
        # _norm_name("AMÉRICA"): _norm_name("América Mineiro"),
        # _norm_name("ATHLETICO"): _norm_name("Athletico Paranaense"),
    }

    # aplica sinônimos
    df["__key"] = df["__key"].replace(synonyms)

    # merge m:1
    df = df.merge(
        ref[["__key", "id_time"]],
        on="__key",
        how="left",
        validate="m:1"
    )

    # housekeeping
    df.drop(columns=["__key"], inplace=True)
    cols = ["coleta_ts_brt", "id_time", "time", "prob_rebaixamento_pct", "prob_campeao_pct"]
    df = df.reindex(columns=cols)

    # diagnóstico
    unmatched = df["id_time"].isna()
    if unmatched.any():
        qtd = int(unmatched.sum())
        log.warn(f"{qtd} time(s) sem id_time — verifique nome_canonico em info_clube.csv.")
        # opcional: exportar pendências para auditoria
        os.makedirs("data/curated", exist_ok=True)
        df.loc[unmatched, ["time"]].drop_duplicates().to_csv(
            "data/curated/pending_prob_times.csv", index=False, encoding="utf-8-sig"
        )
        log.info("Pendências exportadas em data/curated/pending_prob_times.csv")

    # 5) Persistência
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")

    log.ok(f"Arquivo consolidado salvo em: {OUTPUT_FILE}")
    log.info("Prévia dos dados:")
    print(df.head(10))

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.err(f"Falha crítica: {e}")
        raise
