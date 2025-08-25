import os
from pathlib import Path
from typing import Optional
import pandas as pd

from .utils import normalize_columns, coerce_numeric, ensure_dir, read_config, to_parquet

def _download_gdrive_folder(url: str, dest: Path) -> None:
    """
    Download a public Google Drive folder using gdown (no auth required).
    """
    import gdown
    dest.mkdir(parents=True, exist_ok=True)
    gdown.download_folder(url=url, output=str(dest), quiet=False, use_cookies=False)

def _load_xlsx(path: Path) -> pd.DataFrame:
    return pd.read_excel(path)

def run(output_dir: str = "data/processed", config_path: str = "config/config.yml", prefer_gdrive: bool = True) -> dict:
    cfg = read_config(config_path)
    out = Path(output_dir)
    ensure_dir(out)

    # Google Drive
    raw_dir = Path("data/raw")
    ensure_dir(raw_dir)
    if prefer_gdrive and cfg["sources"].get("gdrive_folder_url"):
        try:
            _download_gdrive_folder(cfg["sources"]["gdrive_folder_url"], raw_dir)
        except Exception as e:
            print(f"[WARN] Could not download from Google Drive: {e}. Falling back to local files...")

    # Local
    paths = {k: Path(cfg["sources"]["local_files"][k]) for k in ["sales","inventory","hr"]}
    for k,v in paths.items():
        if not v.exists():
            raise FileNotFoundError(f"Missing required file {k}: {v}. Ensure Google Drive download or place locally.")

    sales = _load_xlsx(paths["sales"])
    inv = _load_xlsx(paths["inventory"])
    hr = _load_xlsx(paths["hr"])

    # Normalizar columnas
    sales = normalize_columns(sales, cfg["columns"]["sales"])
    inv = normalize_columns(inv, cfg["columns"]["inventory"])
    hr = normalize_columns(hr, cfg["columns"]["hr"])

    sales = coerce_numeric(sales, ["quantity","unit_price","discount_percent","sales_amount","profit_margin"])
    inv = coerce_numeric(inv, ["stock_qty","reorder_level","unit_cost","total_value"])
    hr = coerce_numeric(hr, ["performance_score","hours_worked","overtime_hours","salary","bonus"])

    # Fechas
    for c in ["sale_date"]:
        if c in sales.columns:
            sales[c] = pd.to_datetime(sales[c], errors="coerce")
    for c in ["snapshot_date"]:
        if c in inv.columns:
            inv[c] = pd.to_datetime(inv[c], errors="coerce")
    for c in ["review_date"]:
        if c in hr.columns:
            hr[c] = pd.to_datetime(hr[c], errors="coerce")

    # Duplicates
    sales = sales.drop_duplicates()
    inv = inv.drop_duplicates()
    hr = hr.drop_duplicates()

    # Save parquet
    to_parquet(sales, out / "stg_sales.parquet")
    to_parquet(inv, out / "stg_inventory.parquet")
    to_parquet(hr, out / "stg_hr.parquet")

    return {
        "sales_rows": len(sales),
        "inventory_rows": len(inv),
        "hr_rows": len(hr),
        "processed_dir": str(out)
    }