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
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.ventas (
# MAGIC     IdVenta        BIGINT,
# MAGIC     FechaVenta     DATE,
# MAGIC     IdCliente      STRING,
# MAGIC     Cliente        STRING,
# MAGIC     Genero         STRING,
# MAGIC     IdCiudad       STRING,
# MAGIC     Ciudad         STRING,
# MAGIC     EstadoFactura  STRING,
# MAGIC     CanalVenta     STRING,
# MAGIC     IdProducto     STRING,
# MAGIC     Producto       STRING,
# MAGIC     Cantidad       DOUBLE,
# MAGIC     PrecioUnitario DOUBLE,
# MAGIC     ValorVenta     DOUBLE
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.ventas_rechazadas (
# MAGIC     IdVenta BIGINT, FechaVenta DATE, IdCliente STRING, Cliente STRING,
# MAGIC     Genero STRING, IdCiudad STRING, Ciudad STRING, EstadoFactura STRING,
# MAGIC     CanalVenta STRING, IdProducto STRING, Producto STRING,
# MAGIC     Cantidad DOUBLE, PrecioUnitario DOUBLE, ValorVenta DOUBLE
# MAGIC ) USING DELTA;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
