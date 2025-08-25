import re
import os
import io
import json
import math
import random
from pathlib import Path
from typing import Dict, Any, Optional, Tuple

import numpy as np
import pandas as pd

def ensure_dir(p: str | Path) -> Path:
    p = Path(p)
    p.mkdir(parents=True, exist_ok=True)
    return p

def read_config(path: str | Path) -> dict:
    import yaml
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def normalize_columns(df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
    cols = {c: mapping[c] for c in df.columns if c in mapping}
    df = df.rename(columns=cols)
    for src, tgt in mapping.items():
        if tgt not in df.columns:
            df[tgt] = pd.NA
    return df

def parse_decimal_series(s: pd.Series) -> pd.Series:
    def _parse(x):
        if pd.isna(x):
            return np.nan
        if isinstance(x, (int, float, np.integer, np.floating)):
            return float(x)
        if isinstance(x, str):
            x2 = x.strip().replace(" ", "")
            if "," in x2 and "." in x2:
                if x2.rfind(".") > x2.rfind(","):
                    x2 = x2.replace(",", "")
                else:
                    x2 = x2.replace(".", "").replace(",", ".")
            else:
                x2 = x2.replace(",", ".")
            x2 = re.sub(r"(?<=\d)\.(?=\d{3}\b)", "", x2)
            try:
                return float(x2)
            except:
                return np.nan
        return np.nan
    return s.apply(_parse)

def coerce_numeric(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = parse_decimal_series(df[c])
    return df

def to_parquet(df: pd.DataFrame, path: str | Path) -> None:
    path = Path(path)
    ensure_dir(path.parent)
    try:
        df.to_parquet(path, index=False)
    except Exception:
        csv_path = path.with_suffix(".csv")
        df.to_csv(csv_path, index=False, encoding="utf-8")
        
def month_floor(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s).dt.to_period("M").dt.to_timestamp()

def build_dim_date(dates: pd.Series) -> pd.DataFrame:
    s = pd.to_datetime(dates).dropna().unique()
    if len(s) == 0:
        return pd.DataFrame(columns=["date_id","date","year","quarter","month","day_of_month","day_of_week","is_weekend"])
    idx = pd.date_range(pd.to_datetime(dates).min(), pd.to_datetime(dates).max(), freq="D")
    df = pd.DataFrame({"date": idx})
    df["date_id"] = (df["date"].dt.strftime("%Y%m%d")).astype(int)
    df["year"] = df["date"].dt.year
    df["quarter"] = df["date"].dt.quarter
    df["month"] = df["date"].dt.month
    df["day_of_month"] = df["date"].dt.day
    df["day_of_week"] = df["date"].dt.dayofweek + 1
    df["is_weekend"] = df["day_of_week"] >= 6
    return df[["date_id","date","year","quarter","month","day_of_month","day_of_week","is_weekend"]]

def rfm_segmentation(sales_df: pd.DataFrame, today: Optional[pd.Timestamp] = None) -> pd.DataFrame:
    if today is None:
        today = pd.to_datetime(sales_df["sale_date"]).max() + pd.Timedelta(days=1)
    grp = sales_df.groupby("customer_id").agg(
        recency_days = ("sale_date", lambda s: (today - pd.to_datetime(s).max()).days),
        frequency = ("sale_id", "count"),
        monetary = ("sales_amount", "sum"),
    ).reset_index()
    quantiles = grp.quantile(q=[0.2,0.4,0.6,0.8])
    def r_score(x):
        if x <= quantiles.loc[0.2, "recency_days"]: return 5
        elif x <= quantiles.loc[0.4, "recency_days"] : return 4
        elif x <= quantiles.loc[0.6, "recency_days"] : return 3
        elif x <= quantiles.loc[0.8, "recency_days"] : return 2
        else: return 1
    def fm_score(x, col):
        if x <= quantiles.loc[0.2, col]: return 1
        elif x <= quantiles.loc[0.4, col] : return 2
        elif x <= quantiles.loc[0.6, col] : return 3
        elif x <= quantiles.loc[0.8, col] : return 4
        else: return 5
    grp["r_score"] = grp["recency_days"].apply(r_score)
    grp["f_score"] = grp["frequency"].apply(lambda x: fm_score(x, "frequency"))
    grp["m_score"] = grp["monetary"].apply(lambda x: fm_score(x, "monetary"))
    grp["segment"] = grp[["r_score","f_score","m_score"]].sum(axis=1)
    def label(s):
        if s >= 12: return "VIP"
        elif s >= 9: return "Loyal"
        elif s >= 6: return "Regular"
        else: return "At Risk"
    grp["segment_label"] = grp["segment"].apply(label)
    return grp

def add_unknown_row(df: pd.DataFrame, id_col: str, unknown_id: int = 0, **kwargs) -> pd.DataFrame:
    if (df[id_col] == unknown_id).any():
        return df
    unknown = {c: pd.NA for c in df.columns}
    unknown[id_col] = unknown_id
    for k, v in kwargs.items():
        if k in df.columns:
            unknown[k] = v
    return pd.concat([pd.DataFrame([unknown]), df], ignore_index=True)