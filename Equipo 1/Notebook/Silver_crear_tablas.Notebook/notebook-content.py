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
# MAGIC DROP TABLE IF EXISTS Silver.ventas;
# MAGIC CREATE TABLE Silver.ventas (
# MAGIC     IdVenta            INT,
# MAGIC     FechaVenta         DATE,
# MAGIC     IdCliente          STRING,
# MAGIC     Cliente            STRING,
# MAGIC     Genero             STRING,
# MAGIC     GeneroDesc         STRING,
# MAGIC     IdCiudad           STRING,
# MAGIC     Ciudad             STRING,
# MAGIC     EstadoFactura      STRING,
# MAGIC     EstadoFacturaDesc  STRING,
# MAGIC     CanalVenta         STRING,
# MAGIC     IdProducto         STRING,
# MAGIC     Producto           STRING,
# MAGIC     Cantidad           INT,
# MAGIC     PrecioUnitario     DECIMAL(18,2),
# MAGIC     ValorVenta         DECIMAL(18,2),
# MAGIC     FechaCarga         TIMESTAMP
# MAGIC ) USING DELTA

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS Silver.ventas_rechazadas;
# MAGIC CREATE TABLE Silver.ventas_rechazadas (
# MAGIC     IdVenta         STRING,
# MAGIC     FechaVenta      STRING,
# MAGIC     IdCliente       STRING,
# MAGIC     IdProducto      STRING,
# MAGIC     Cantidad        STRING,
# MAGIC     PrecioUnitario  STRING,
# MAGIC     MotivoRechazo   STRING,
# MAGIC     FechaCarga      TIMESTAMP
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
