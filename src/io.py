\
from __future__ import annotations

import os
import pandas as pd

def read_csv(path: str, *, chunksize: int | None = None) -> pd.DataFrame | pd.io.parsers.TextFileReader:
    """
    Read CSV. For large files, caller may pass chunksize.
    """
    if chunksize:
        return pd.read_csv(path, chunksize=chunksize)
    return pd.read_csv(path)

def write_parquet(df: pd.DataFrame, path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_parquet(path, index=False)

def write_json(obj: dict, path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    import json
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, default=str)
