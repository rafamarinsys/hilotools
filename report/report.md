# Informe Técnico - Hilo Tools Pipeline de Datos

## 1) Objetivo
Construir un **pipeline de datos end-to-end** que integre tres dominios:
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

---

## 2) Arquitectura

### 2.1) Componentes principales
- **Ingesta**  
  - `pipeline/ingest.py` descarga automáticamente desde la carpeta pública de Google Drive usando `gdown`.  
  - Archivos almacenados en `data/raw/`. 
  - Transformación inicial → **Staging** (`data/processed/` en Parquet.  
  - Limpieza aplicada:  
    - Normalización de columnas.  
    - Conversión de decimales con coma o punto.  
    - Eliminación de duplicados.

- **Modelado**  
  - `pipeline/model.py` construye un **esquema en estrella** en SQLite (`data/warehouse/warehouse.db`).  
  - Dimensiones:  
    - **dim_date**: calendario completo diario
    - **dim_product**: unión entre `Product_ID` (ventas) y `Product_Code` (inventario).
    - **dim_customer**: enriquecida con **RFM segmentation** (Recency, Frequency, Monetary).
    - **dim_store**: tiendas derivadas de ventas.
    - **dim_employee**: empleados de RRHH (no presentes en ventas → `employee_key=0` Unknown). 
  - Hechos:  
    - **fact_sales** (ventas transaccionales).  
    - **fact_inventory_snapshot** (snapshots de inventario).

- **Analítica**  
  - `pipeline/analytics.py`:  
    - Genera **features mensuales** de los 3 dominios:  
      - Ventas: importe total, cantidad, descuento medio, margen medio.  
      - RRHH: desempeño medio, horas trabajadas, horas extra, salario y bonus.  
      - Inventario: stock medio, gaps de reposición, stockouts, valor inventario.  
    - Escalado estándar (`StandardScaler`) → PCA (`sklearn`).  
    - Output:  
      - `report/features_monthly.csv`  
      - `report/pca_explained_variance.csv`  
      - `report/pca_loadings.csv`  
      - `report/pca_summary.md` (tabla con varianza y top loadings por componente)

- **Orquestación**  
  - CLI con `Typer` (`python -m pipeline ...`). 
  - Comandos: `ingest`, `model`, `analytics`, `run-all`.  
  - Compatible con **Conda**.  

---

## 3) Esquema en estrella

### 3.1) Fact Tables
- **fact_sales**  
  - `sale_id`, `date_id`, `product_key`, `customer_key`, `store_key`, `employee_key`  
  - Métricas: `quantity`, `unit_price`, `discount_percent`, `sales_amount`, `profit_margin`

- **fact_inventory_snapshot**  
  - `inventory_id`, `date_id`, `product_key`, `warehouse_key`  
  - Métricas: `stock_qty`, `reorder_level`, `unit_cost`, `total_value`

### 3.2) Dimension Tables
- **dim_date**: granularidad diaria, incluye año, mes, trimestre, día de semana, etc.  
- **dim_product**: integración Ventas ↔ Inventario 
- **dim_customer**: enriquecido con RFM y etiquetas
- **dim_store**: tiendas derivadas de ventas 
- **dim_employee**: empleados de RRHH, mediana de salario y bonus.

---

## 4) Principales decisiones de ingeniería
1. **Parsing de números**:  
   - Soporta formatos con coma, punto y miles 
   - Ejemplo: `"1.234,56"` → `1234.56`.   

2. **Integración Productos**:  
   - `Product_Code` → sufijo numérico (`PRD_0084` → `84`).  
   - Cobertura: 88% de productos de ventas encontrados en inventario 

4. **Empleados**:  
   - Dataset de ventas no tiene `Employee_ID`
   - Se incluyó dimensión `dim_employee` por requisito → `fact_sales.employee_key=0` Unknown.  

5. **PCA**:  
   - Se combinaron features de ventas, RRHH e inventario.  
   - Primeros 5 componentes explican ~93% de la varianza (muestra).  
   - **PC1**: carga en ventas + márgenes + inventario → “intensidad comercial”.  
   - **PC2**: correlación con desempeño RRHH → “eficiencia laboral”.  

---

## 5) Validación

- **Ingesta**: 1000 ventas, 500 inventarios, 800 evaluaciones RRHH.  
- **Modelo**:  
  - `fact_sales`: 1000 filas  
  - `dim_customer`: 200 clientes  
  - `dim_product`: 99 productos  
  - `dim_employee`: 196 empleados  
- **PCA**:  
  - 184 períodos mensuales  
  - 14 features consolidadas  

---

## 6) Reproducibilidad

### Conda
```bash
conda env create -f environment.yml
conda activate hilo-data
python -m pipeline run-all --prefer-gdrive
```

---

## 7) Testing
- pytest -q valida funciones críticas (parse_decimal_series).
- conftest.py asegura imports correctos (pipeline visible).
- Futuros tests sugeridos:
    - Validación de integridad de claves en fact/dim.

---

## 8) Próximos pasos
1. Orquestación en Airflow
2. Data Quality
3. Versionado
4. CI/CD

---

**El pipeline:**
- Integra múltiples dominios (ventas, RRHH, inventario).
- Crea un esquema en estrella.
- Permite PCA sobre features combinadas mensuales.
- Es reproducible, documentado y extensible.