# src/transform/build_dim_calendario.py
import sys, pathlib, argparse, json, pandas as pd
from pathlib import Path

# permite executar direto sem -m se quiser
sys.path.append(str(pathlib.Path(__file__).resolve().parents[2]))

from src.common.logging_utils import info, ok, err
from src.common.io import save_table

PT_MESES = ["janeiro","fevereiro","março","abril","maio","junho",
            "julho","agosto","setembro","outubro","novembro","dezembro"]
PT_DIAS  = ["segunda","terca","quarta","quinta","sexta","sabado","domingo"]

def _read_json_any(p):
    b = Path(p).read_bytes()
    for enc in ("utf-8","utf-8-sig","cp1252","latin-1"):
        try: return json.loads(b.decode(enc))
        except Exception: pass
    raise ValueError("Falha ao decodificar JSON.")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--matches-in", default="data/raw/matches_fdorg.json")
    ap.add_argument("--out", default="data/curated/dim_calendario.csv")
    ap.add_argument("--inicio", help="YYYY-MM-DD (opcional)")
    ap.add_argument("--fim",    help="YYYY-MM-DD (opcional)")
    args = ap.parse_args()

    try:
        if args.inicio and args.fim:
            dt_start = pd.Timestamp(args.inicio).normalize()
            dt_end   = pd.Timestamp(args.fim).normalize()
        else:
            info("Inferindo intervalo de datas a partir de matches...")
            mob = _read_json_any(args.matches_in)
            ds = pd.to_datetime(pd.Series([m.get("utcDate") for m in mob.get("matches", []) if m.get("utcDate")]))
            if ds.empty:
                err("Não foi possível inferir datas. Informe --inicio e --fim.")
                raise SystemExit(1)
            dt_start, dt_end = ds.min().normalize(), ds.max().normalize()

        info(f"Criando calendário de {dt_start.date()} a {dt_end.date()}...")
        rng = pd.date_range(dt_start, dt_end, freq="D")
        df = pd.DataFrame({"data": rng})
        df["data"] = df["data"].dt.date  # YYYY-MM-DD
        dt = pd.to_datetime(df["data"])

        df["ano"] = dt.dt.year
        df["mes"] = dt.dt.month
        df["dia"] = dt.dt.day
        df["semana"] = dt.dt.isocalendar().week.astype(int)
        df["nome_dia"] = dt.dt.dayofweek.map(lambda i: PT_DIAS[i])
        df["nome_mes"] = df["mes"].map(lambda m: PT_MESES[m-1])
        df["eh_fim_de_semana"] = df["nome_dia"].isin(["sabado","domingo"])
        df["ano_mes"] = dt.dt.to_period("M").astype(str)

        save_table(df, args.out)
        ok(f"Dimensão calendário salva em {args.out}")
        ok("Concluído")
    except Exception as e:
        err(f"Falha: {e}")
        raise

if __name__ == "__main__":
    main()