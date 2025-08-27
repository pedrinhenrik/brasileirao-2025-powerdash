# src/common/io.py
from pathlib import Path
import json
import pandas as pd

def save_json(path: str | Path, obj) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    txt = json.dumps(obj, ensure_ascii=False, indent=2)
    p.write_text(txt, encoding="utf-8")

def save_table(df: pd.DataFrame, out_path: str | Path) -> None:
    p = Path(out_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    if p.suffix.lower() == ".parquet":
        df.to_parquet(p, index=False)
    else:
        df.to_csv(p, index=False, encoding="utf-8-sig") 
