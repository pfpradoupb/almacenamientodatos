# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "59e8fa3e-2041-4a88-977a-0ce8e3ec397e",
# META       "default_lakehouse_name": "LS_Bronze",
# META       "default_lakehouse_workspace_id": "65b80d66-c34b-4fe8-ab4c-0e77aae74692",
# META       "known_lakehouses": [
# META         {
# META           "id": "59e8fa3e-2041-4a88-977a-0ce8e3ec397e"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS Gold.dim_fecha;
# MAGIC CREATE TABLE Gold.dim_fecha (
# MAGIC     SkFecha       INT,      -- yyyyMMdd
# MAGIC     Fecha         DATE,
# MAGIC     Anio          INT,
# MAGIC     Trimestre     INT,
# MAGIC     Mes           INT,
# MAGIC     NombreMes     STRING,
# MAGIC     Dia           INT,
# MAGIC     DiaSemana     INT,
# MAGIC     NombreDia     STRING
# MAGIC ) USING DELTA

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS Gold.dim_cliente;
# MAGIC CREATE TABLE Gold.dim_cliente (
# MAGIC     SkCliente   INT,
# MAGIC     IdCliente   STRING,
# MAGIC     Cliente     STRING,
# MAGIC     Genero      STRING,
# MAGIC     GeneroDesc  STRING
# MAGIC ) USING DELTA

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS Gold.dim_ciudad;
# MAGIC CREATE TABLE Gold.dim_ciudad (
# MAGIC     SkCiudad  INT,
# MAGIC     IdCiudad  STRING,
# MAGIC     Ciudad    STRING
# MAGIC ) USING DELTA

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS Gold.dim_producto;
# MAGIC CREATE TABLE Gold.dim_producto (
# MAGIC     SkProducto  INT,
# MAGIC     IdProducto  STRING,
# MAGIC     Producto    STRING
# MAGIC ) USING DELTA

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS Gold.dim_canal;
# MAGIC CREATE TABLE Gold.dim_canal (
# MAGIC     SkCanal     INT,
# MAGIC     CanalVenta  STRING
# MAGIC ) USING DELTA

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS Gold.dim_estado_factura;
# MAGIC CREATE TABLE Gold.dim_estado_factura (
# MAGIC     SkEstado           INT,
# MAGIC     EstadoFactura      STRING,
# MAGIC     EstadoFacturaDesc  STRING
# MAGIC ) USING DELTA

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS Gold.fact_ventas;
# MAGIC CREATE TABLE Gold.fact_ventas (
# MAGIC     IdVenta         INT,      -- dimensión degenerada (número de factura)
# MAGIC     SkFecha         INT,
# MAGIC     SkCliente       INT,
# MAGIC     SkCiudad        INT,
# MAGIC     SkProducto      INT,
# MAGIC     SkCanal         INT,
# MAGIC     SkEstado        INT,
# MAGIC     Cantidad        INT,
# MAGIC     PrecioUnitario  DECIMAL(18,2),
# MAGIC     ValorVenta      DECIMAL(18,2)
# MAGIC ) USING DELTA

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SHOW TABLES

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
