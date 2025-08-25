# Importación de librerias
from pathlib import Path
import sqlite3
import pandas as pd
import numpy as np

from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA

from .utils import ensure_dir

def _connect(db_path: str):
    con = sqlite3.connect(db_path)
    return con

def _monthly_features(con) -> pd.DataFrame:
    # Agregación Sales mensual
    sales = pd.read_sql("""
        SELECT date(substr(date_id,1,4)||'-'||substr(date_id,5,2)||'-'||substr(date_id,7,2)) as date,
               quantity, unit_price, discount_percent, sales_amount, profit_margin
        FROM fact_sales
    """, con, parse_dates=["date"])
    if len(sales) == 0:
        return pd.DataFrame()
    sales["month"] = sales["date"].dt.to_period("M").dt.to_timestamp()
    s_agg = sales.groupby("month").agg(
        sales_amount_total=("sales_amount","sum"),
        sales_qty_total=("quantity","sum"),
        avg_discount=("discount_percent","mean"),
        avg_profit_margin=("profit_margin","mean")
    )
    # Agregación HR mensual
    hr = pd.read_sql("""
        SELECT * FROM dim_employee
    """, con)

    hr_month = None
    stg_hr_path = Path("data/processed/stg_hr.parquet")
    if stg_hr_path.exists():
        stg_hr = pd.read_parquet(stg_hr_path)
        if "review_date" in stg_hr.columns:
            stg_hr["month"] = pd.to_datetime(stg_hr["review_date"]).dt.to_period("M").dt.to_timestamp()
            hr_month = stg_hr.groupby("month").agg(
                perf_score_avg=("performance_score","mean"),
                hours_worked_avg=("hours_worked","mean"),
                overtime_ratio=("overtime_hours", lambda s: (s.mean() / (s.mean()+1e-9)) ),
                salary_avg=("salary","mean"),
                bonus_avg=("bonus","mean"),
            )
    if hr_month is None:
        hr_month = pd.DataFrame(index=s_agg.index, data={
            "perf_score_avg": 0.0,
            "hours_worked_avg": 0.0,
            "overtime_ratio": 0.0,
            "salary_avg": 0.0,
            "bonus_avg": 0.0,
        })
    # Agregación Inventory mensual
    inv_df = None
    try:
        inv = pd.read_sql("""
            SELECT date(substr(date_id,1,4)||'-'||substr(date_id,5,2)||'-'||substr(date_id,7,2)) as date,
                   stock_qty, reorder_level, unit_cost, total_value
            FROM fact_inventory_snapshot
        """, con, parse_dates=["date"])
        inv["month"] = inv["date"].dt.to_period("M").dt.to_timestamp()
        inv_df = inv.groupby("month").agg(
            inv_stock_avg=("stock_qty","mean"),
            inv_stockouts=("stock_qty", lambda s: (s<=0).sum()),
            inv_reorder_gap_avg=(("stock_qty"), "mean")
        )
        inv_df["inv_reorder_gap_avg"] = inv.groupby("month").apply(lambda g: (g["stock_qty"] - g["reorder_level"]).mean())
        inv_df["unit_cost_avg"] = inv.groupby("month")["unit_cost"].mean()
        inv_df["inv_value_total"] = inv.groupby("month")["total_value"].sum()
    except Exception:
        inv_df = pd.DataFrame(index=s_agg.index, data={
            "inv_stock_avg": 0.0, "inv_stockouts": 0.0, "inv_reorder_gap_avg": 0.0, "unit_cost_avg": 0.0, "inv_value_total": 0.0
        })

    features = s_agg.join(hr_month, how="outer").join(inv_df, how="outer").sort_index()
    return features

def run_pca(warehouse_path: str = "data/warehouse/warehouse.db", out_dir: str = "report", n_components: int = 5):
    ensure_dir(out_dir)
    con = _connect(warehouse_path)
    try:
        feats = _monthly_features(con)
    finally:
        con.close()
    if feats.empty:
        raise ValueError("Check that the model step populated the warehouse.")

    # Faltantes
    feats = feats.copy()
    feats = feats.fillna(feats.median(numeric_only=True))

    num_cols = feats.select_dtypes(include=[np.number]).columns.tolist()
    X = feats[num_cols].values

    scaler = StandardScaler()
    Xs = scaler.fit_transform(X)

    pca = PCA(n_components=min(n_components, Xs.shape[1]))
    Z = pca.fit_transform(Xs)

    # Varianza y componentes
    explained = pd.DataFrame({
        "component": [f"PC{i+1}" for i in range(pca.n_components_)],
        "explained_variance_ratio": pca.explained_variance_ratio_,
        "cumulative_variance": np.cumsum(pca.explained_variance_ratio_),
    })

    loadings = pd.DataFrame(pca.components_.T, index=num_cols, columns=[f"PC{i+1}" for i in range(pca.n_components_)])

    # Save
    feats.to_csv(Path(out_dir) / "features_monthly.csv", index=True)
    explained.to_csv(Path(out_dir) / "pca_explained_variance.csv", index=False)
    loadings.to_csv(Path(out_dir) / "pca_loadings.csv")

    # Reporte
    md = ["# PCA Resumen",
          "",
          "## Varianza Explicada",
          explained.to_markdown(index=False),
          "",
          "## Top Loadings (absolutas) por Componentes"
         ]

    for c in explained["component"]:
        top = loadings[c].abs().sort_values(ascending=False).head(8)
        md.append(f"### {c}")
        md.append(top.to_markdown())
        md.append("")

    (Path(out_dir) / "pca_summary.md").write_text("\n".join(md))

    return {
        "features_path": str(Path(out_dir) / "features_monthly.csv"),
        "explained_path": str(Path(out_dir) / "pca_explained_variance.csv"),
        "loadings_path": str(Path(out_dir) / "pca_loadings.csv"),
        "summary_md": str(Path(out_dir) / "pca_summary.md"),
        "n_rows": len(feats),
        "n_features": len(num_cols)
    }