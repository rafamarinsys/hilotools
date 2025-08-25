# Hilo Tools Pipeline de datos

**Objetivo:** Construir un **pipeline de datos end-to-end** que integre tres dominios:
- Ventas (`sales_sample.xlsx`)
- Recursos Humanos (`hr_sample.xlsx`)
- Inventario (`inventory_sample.xlsx`)

El sistema debía:
1. Ingerir automáticamente los datos desde una **fuente pública en Google Drive**.
2. Estandarizar y transformar los datos en un formato uniforme.
3. Crear un **esquema en estrella** en un almacén de datos.
4. Generar **features mensuales** combinadas de ventas, RRHH e inventario.
5. Ejecutar un **PCA** para identificar patrones.
6. Ser **reproducible** en Conda, con documentación y tests básicos.

## Estructura

```
.
├── config/
│   └── config.yml           # mapeos de columnas, parámetros, fuente Google Drive
├── data/
│   ├── processed/           # staging parquet
│   ├── raw/                 # xlsx descargados/locales
│   └── warehouse/           # SQLite
├── docs/
│   └── data_dictionary.md
├── pipeline/
│   ├── __init__.py
│   ├── analytics.py         # PCA mensual
│   ├── cli.py               # Typer CLI
│   ├── ingest.py            # gdown + normalización + parquet
│   ├── model.py             # esquema en estrella en SQLite
│   └── utils.py
├── report/
│   └── (pca outputs)
├── scripts/
│   └── run_all.sh
├── environment.yml
├── Makefile
├── requirements.txt
└── README.md
```

## Setup (Conda)

```bash
conda env create -f environment.yml
conda activate hilo-data
```

## Ejecución Pipeline
```bash
python -m pipeline run-all --prefer-gdrive
```

> **Nota:** La ingesta intentará descargar desde la carpeta pública de Google Drive definida en `config/config.yml` con **gdown**. Alternativamente, están los 3 ficheros en `data/raw/` con los nombres:
> - `sales_sample.xlsx`
> - `inventory_sample.xlsx`
> - `hr_sample.xlsx`


### Ejecución por pasos (Opcional)

```bash
# 1) Ingesta
python -m pipeline ingest --prefer-gdrive

# 2) Modelado en estrella
python -m pipeline model

# 3) Analítica PCA (mensual)
python -m pipeline analytics --n-components 5
```

## Esquema en estrella

- **fact_sales**(sale_id, date_id, product_key, customer_key, store_key, employee_key, quantity, unit_price, discount_percent, sales_amount, profit_margin)
- **dim_date**(date_id, date, year, quarter, month, day_of_month, day_of_week, is_weekend)
- **dim_product**(product_key, product_id, product_code, category_id)
- **dim_customer**(customer_key, recency_days, frequency, monetary, r_score, f_score, m_score, segment, segment_label)
- **dim_store**(store_key, store_id, store_name, store_type)
- **dim_employee**(employee_key, employee_id, department_id, salary, bonus)
- **fact_inventory_snapshot**(inventory_id, date_id, product_key, warehouse_key, stock_qty, reorder_level, unit_cost, total_value)

Decisiones:
- **Parsing de números**:  
   - Soporta formatos con coma, punto y miles 
   - Ejemplo: `"1.234,56"` → `1234.56`.   

- **Integración Productos**:  
   - `Product_Code` → sufijo numérico (`PRD_0084` → `84`).  
   - Cobertura: 88% de productos de ventas encontrados en inventario 

- **Empleados**:  
   - Dataset de ventas no tiene `Employee_ID`
   - Se incluyó dimensión `dim_employee` por requisito → `fact_sales.employee_key=0` Unknown.  


## Reporte

Tras `run-all` se generan:
- `report/features_monthly.csv`
- `report/pca_explained_variance.csv`
- `report/pca_loadings.csv`
- `report/pca_summary.md`

## Reproducibilidad

- **Conda**.
- Transformaciones no modifican los ficheros fuente.
- Manejo de nulos, vacíos, decimales con coma, duplicados y heterogeneidad de formatos.

## Tests rápidos

```bash
pytest -q
```