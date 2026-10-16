# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a51ca50c-7c84-4d0d-976c-6fdc12c5484d",
# META       "default_lakehouse_name": "LS_UPB_GRUPO_5",
# META       "default_lakehouse_workspace_id": "7cf07885-5568-45be-9013-44bd9fb2af6e",
# META       "known_lakehouses": [
# META         {
# META           "id": "a51ca50c-7c84-4d0d-976c-6fdc12c5484d"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "default_warehouse": "23b6bfe4-c29a-435e-a02b-cb0668f2ba71",
# META       "known_warehouses": [
# META         {
# META           "id": "23b6bfe4-c29a-435e-a02b-cb0668f2ba71",
# META           "type": "Lakewarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# ============================================================
# NOTEBOOK 1 DE GOLD: Creacion de tablas (Spark SQL)
# ============================================================
# Pega cada bloque %%sql en una celda SQL distinta del notebook
# (o marca la celda como SQL con %%sql si tu notebook esta en modo PySpark).

# CELDA 1
%%sql
CREATE TABLE IF NOT EXISTS gold.dim_fecha (
    FechaKey INT, Fecha DATE, Anio INT, Trimestre INT, Mes INT,
    NombreMes STRING, Dia INT, DiaSemana STRING) USING DELTA;

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_cliente (
# MAGIC     ClienteKey INT, IdCliente STRING, Cliente STRING, Genero STRING) USING DELTA;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_ciudad (
# MAGIC     CiudadKey INT, IdCiudad STRING, Ciudad STRING) USING DELTA;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_producto (
# MAGIC     ProductoKey INT, IdProducto STRING, Producto STRING) USING DELTA;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_canal (
# MAGIC     CanalKey INT, CanalVenta STRING) USING DELTA;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_estado_factura (
# MAGIC     EstadoKey INT, EstadoFactura STRING) USING DELTA;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.fact_ventas (
# MAGIC     IdVenta INT, FechaKey INT, ClienteKey INT, CiudadKey INT,
# MAGIC     ProductoKey INT, CanalKey INT, EstadoKey INT,
# MAGIC     Cantidad DOUBLE, PrecioUnitario DOUBLE, ValorVenta DOUBLE) USING DELTA;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SHOW TABLES IN gold;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
