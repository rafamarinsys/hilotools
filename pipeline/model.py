from pathlib import Path
import sqlite3
import pandas as pd
import numpy as np

from .utils import ensure_dir, build_dim_date, add_unknown_row

def _connect(db_path: str):
    ensure_dir(Path(db_path).parent)
    con = sqlite3.connect(db_path)
    return con

def build_star(processed_dir: str = "data/processed", warehouse_path: str = "data/warehouse/warehouse.db"):
    # Cargar staging
    
    def _read_staging(name: str):
        p_parquet = Path(processed_dir) / f"{name}.parquet"
        p_csv = Path(processed_dir) / f"{name}.csv"
        if p_parquet.exists():
            try:
                return pd.read_parquet(p_parquet)
            except Exception:
                pass
        if p_csv.exists():
            return pd.read_csv(p_csv, parse_dates=[c for c in ["sale_date","snapshot_date","review_date"] if c in pd.read_csv(p_csv, nrows=0).columns])
        raise FileNotFoundError(f"Could not load staging file for {name}.")
    sales = _read_staging("stg_sales")
    inv = _read_staging("stg_inventory")
    hr = _read_staging("stg_hr")

    # -----------------
    # Dim Date
    # -----------------
    dim_date = build_dim_date(sales["sale_date"])

    # -----------------
    # Dim Product
    # -----------------
    inv = inv.copy()
    inv["product_id_from_code"] = inv["product_code"].str.extract(r'(\d+)$').astype(float).astype("Int64")
    prod_from_sales = sales[["product_id"]].dropna().drop_duplicates().astype(int)
    prod_from_inv = inv[["product_id_from_code","product_code","category_id"]].dropna().drop_duplicates()
    prod_from_inv = prod_from_inv.rename(columns={"product_id_from_code":"product_id"})

    dim_product = pd.merge(prod_from_sales, prod_from_inv, on="product_id", how="left")

    missing_mask = dim_product["product_code"].isna()
    dim_product.loc[missing_mask, "product_code"] = dim_product.loc[missing_mask, "product_id"].apply(lambda x: f"PRD_{int(x):04d}")
    dim_product["category_id"] = dim_product["category_id"].astype("Int64")
    dim_product = dim_product.drop_duplicates().reset_index(drop=True)
    dim_product["product_key"] = dim_product["product_id"]
    dim_product = add_unknown_row(dim_product, "product_key", unknown_id=0, product_id=0, product_code="UNKNOWN", category_id=pd.NA)
    dim_product = dim_product[["product_key","product_id","product_code","category_id"]]

    # -----------------
    # Dim Customer
    # -----------------
    from .utils import rfm_segmentation
    rfm = rfm_segmentation(sales)
    dim_customer = rfm.rename(columns={"customer_id":"customer_key", "segment":"segment_score"})
    dim_customer = add_unknown_row(dim_customer, "customer_key", unknown_id=0, segment="UNKNOWN")

    dim_customer = dim_customer[["customer_key","recency_days","frequency","monetary","r_score","f_score","m_score","segment_score","segment_label"]]

    # -----------------
    # Dim Store
    # -----------------
    dim_store = sales[["store_id"]].dropna().drop_duplicates().astype(int)
    dim_store["store_key"] = dim_store["store_id"]
    dim_store["store_name"] = dim_store["store_id"].apply(lambda s: f"Store {int(s)}")
    dim_store["store_type"] = "retail"
    dim_store = add_unknown_row(dim_store, "store_key", unknown_id=0, store_name="UNKNOWN", store_type="UNKNOWN", store_id=0)
    dim_store = dim_store[["store_key","store_id","store_name","store_type"]]

    # -----------------
    # Dim Employee
    # -----------------
    dim_employee = hr.groupby("employee_id", as_index=False).agg({
        "department_id":"first",
        "salary":"median",
        "bonus":"median"
    })
    dim_employee["employee_key"] = dim_employee["employee_id"]
    dim_employee = add_unknown_row(dim_employee, "employee_key", unknown_id=0, department_id=pd.NA, salary=pd.NA, bonus=pd.NA, employee_id=0)
    dim_employee = dim_employee[["employee_key","employee_id","department_id","salary","bonus"]]

    # -----------------
    # Fact Sales
    # -----------------
    fact_sales = sales.copy()
    fact_sales["date_id"] = pd.to_datetime(fact_sales["sale_date"]).dt.strftime("%Y%m%d").astype(int)
    fact_sales["product_key"] = fact_sales["product_id"].fillna(0).astype(int).where(fact_sales["product_id"].notna(), 0)
    fact_sales["customer_key"] = fact_sales["customer_id"].fillna(0).astype(int).where(fact_sales["customer_id"].notna(), 0)
    fact_sales["store_key"] = fact_sales["store_id"].fillna(0).astype(int).where(fact_sales["store_id"].notna(), 0)

    fact_sales["employee_key"] = 0

    fact_sales = fact_sales[[
        "sale_id","date_id","product_key","customer_key","store_key","employee_key",
        "quantity","unit_price","discount_percent","sales_amount","profit_margin"
    ]]

    # -----------------
    # Fact Inventory
    # -----------------
    fact_inventory = inv.copy()
    fact_inventory["date_id"] = pd.to_datetime(fact_inventory["snapshot_date"]).dt.strftime("%Y%m%d").astype(int)
    fact_inventory["product_key"] = fact_inventory["product_id_from_code"].fillna(0).astype(int)
    fact_inventory["warehouse_key"] = fact_inventory["warehouse_id"].fillna(0).astype(int)
    fact_inventory = fact_inventory[[
        "inventory_id","date_id","product_key","warehouse_key","stock_qty","reorder_level","unit_cost","total_value"
    ]]

    # -----------------
    # Warehouse
    # -----------------
    con = _connect(warehouse_path)
    try:
        dim_date.to_sql("dim_date", con, if_exists="replace", index=False)
        dim_product.to_sql("dim_product", con, if_exists="replace", index=False)
        dim_customer.to_sql("dim_customer", con, if_exists="replace", index=False)
        dim_store.to_sql("dim_store", con, if_exists="replace", index=False)
        dim_employee.to_sql("dim_employee", con, if_exists="replace", index=False)
        fact_sales.to_sql("fact_sales", con, if_exists="replace", index=False)
        fact_inventory.to_sql("fact_inventory_snapshot", con, if_exists="replace", index=False)
    finally:
        con.close()

    return {
        "dim_date_rows": len(dim_date),
        "dim_product_rows": len(dim_product),
        "dim_customer_rows": len(dim_customer),
        "dim_store_rows": len(dim_store),
        "dim_employee_rows": len(dim_employee),
        "fact_sales_rows": len(fact_sales),
        "fact_inventory_rows": len(fact_inventory),
        "warehouse_path": warehouse_path
    }