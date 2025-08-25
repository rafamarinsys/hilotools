# Diccionario de datos

## Ventas (stg_sales.parquet → fact_sales)

- `sale_id` — Identificador de la venta (PK)
- `sale_date` → `date_id` — Fecha de la venta (FK `dim_date` en formato `YYYYMMDD`)
- `product_id` → `product_key` — Producto (FK `dim_product`)
- `customer_id` → `customer_key` — Cliente (FK `dim_customer`)
- `store_id` → `store_key` — Tienda (FK `dim_store`)
- `employee_key` — Empleado (FK `dim_employee`). En el dataset se completa con `0` (*Unknown*).
- Métricas: `quantity`, `unit_price`, `discount_percent`, `sales_amount`, `profit_margin`

## Inventario (stg_inventory.parquet → fact_inventory_snapshot)

- `inventory_id` — Identificador del snapshot de inventario
- `snapshot_date` → `date_id` — Fecha del snapshot (FK `dim_date`)
- `product_code` → `product_key` — Mapeo `PRD_00XX → XX`
- `warehouse_id` → `warehouse_key` — Almacén
- Métricas: `stock_qty`, `reorder_level`, `unit_cost`, `total_value`

## RRHH (stg_hr.parquet → dim_employee)

- `employee_id` → `employee_key` — Empleado
- `department_id`, `salary`, `bonus` — Atributos
- Métricas temporales (solo en staging): `performance_score`, `hours_worked`, `overtime_hours` (se usan para features mensuales del PCA)

## Dimensiones auxiliares

- `dim_date` — Granularidad diaria
- `dim_customer` — Segmentación **RFM**
- `dim_store` — Identidad y tipo de tienda