# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a37dd33e-8bd4-48a6-b85b-a5520113c9d9",
# META       "default_lakehouse_name": "Lakehouse_completo",
# META       "default_lakehouse_workspace_id": "e0d0c8fa-81d6-4aaa-aa38-b365c2c816a2",
# META       "known_lakehouses": [
# META         {
# META           "id": "a37dd33e-8bd4-48a6-b85b-a5520113c9d9"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Silver_ventas_DDL · Creación de tablas Silver (Spark SQL)
# 
# * **Lakehouse predeterminado:** el lakehouse único del equipo (p. ej. `Lakehouse_Total`). El lakehouse debe tener **Lakehouse schemas** activado: las tablas Silver van en el schema `Silver`.
# * Crea las tablas Delta de la capa Silver:
#   * `Silver.ventas` → datos limpios y estandarizados.
#   * `Silver.ventas_rechazados` → registros que no pasan las reglas de calidad (cuarentena), con el motivo.
# * Es idempotente (`IF NOT EXISTS`): se puede ejecutar varias veces sin error.

# CELL ********************

# MAGIC %%sql
# MAGIC -- Descomenta estas dos líneas solo si necesitas recrear las tablas desde cero
# MAGIC -- DROP TABLE IF EXISTS Silver.ventas;
# MAGIC -- DROP TABLE IF EXISTS Silver.ventas_rechazados;
# MAGIC SHOW SCHEMAS

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE SCHEMA IF NOT EXISTS Silver

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS Silver.ventas (
# MAGIC     IdVenta                   INT            COMMENT 'Número de factura',
# MAGIC     FechaVenta                DATE           COMMENT 'Fecha de la venta (tipo Date)',
# MAGIC     IdCliente                 STRING,
# MAGIC     Cliente                   STRING,
# MAGIC     Genero                    STRING         COMMENT 'F / M',
# MAGIC     GeneroDescripcion         STRING         COMMENT 'Femenino / Masculino',
# MAGIC     IdCiudad                  STRING,
# MAGIC     Ciudad                    STRING         COMMENT 'Bogotá / Medellín / Cali',
# MAGIC     EstadoFactura             STRING         COMMENT 'C / R / A',
# MAGIC     EstadoFacturaDescripcion  STRING         COMMENT 'Cancelado / Rechazado / Anulado',
# MAGIC     CanalVenta                STRING,
# MAGIC     IdProducto                STRING,
# MAGIC     Producto                  STRING,
# MAGIC     Cantidad                  INT,
# MAGIC     PrecioUnitario            DECIMAL(18,2),
# MAGIC     ValorVenta                DECIMAL(18,2)  COMMENT 'Cantidad x PrecioUnitario',
# MAGIC     ClienteImputado           BOOLEAN        COMMENT 'TRUE si el nombre venía nulo y se completó con el catálogo',
# MAGIC     FechaCarga                TIMESTAMP
# MAGIC ) USING DELTA

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS Silver.ventas_rechazados (
# MAGIC     IdVenta         STRING,
# MAGIC     FechaVenta      STRING,
# MAGIC     IdCliente       STRING,
# MAGIC     Cliente         STRING,
# MAGIC     Genero          STRING,
# MAGIC     IdCiudad        STRING,
# MAGIC     Ciudad          STRING,
# MAGIC     EstadoFactura   STRING,
# MAGIC     CanalVenta      STRING,
# MAGIC     IdProducto      STRING,
# MAGIC     Producto        STRING,
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
# MAGIC SHOW TABLES IN Silver

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
