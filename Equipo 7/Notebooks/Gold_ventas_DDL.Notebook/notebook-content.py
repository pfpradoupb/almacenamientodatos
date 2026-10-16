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

# # Gold_ventas_DDL · Creación del modelo dimensional (Spark SQL)
# 
# * **Lakehouse predeterminado:** el lakehouse único del equipo (p. ej. `Lakehouse_Total`, con schemas). Las tablas van en el schema `Gold`.
# * Modelo estrella. **Grano de la tabla de hechos:** una línea de factura (un producto dentro de una venta).
# 
# | Tabla | Tipo | Llave |
# |---|---|---|
# | `Gold.dim_fecha` | Dimensión | `sk_fecha` (aaaammdd) |
# | `Gold.dim_cliente` | Dimensión | `sk_cliente` |
# | `Gold.dim_producto` | Dimensión | `sk_producto` |
# | `Gold.dim_ciudad` | Dimensión | `sk_ciudad` |
# | `Gold.dim_canal_venta` | Dimensión | `sk_canal_venta` |
# | `Gold.dim_estado_factura` | Dimensión | `sk_estado_factura` |
# | `Gold.fact_ventas` | Hechos | `id_venta` + `sk_producto` |
# 
# `id_venta` queda en la tabla de hechos como **dimensión degenerada** (número de factura).

# CELL ********************

# MAGIC %%sql
# MAGIC -- Descomenta solo si necesitas recrear el modelo
# MAGIC -- DROP TABLE IF EXISTS Gold.fact_ventas;
# MAGIC -- DROP TABLE IF EXISTS Gold.dim_fecha;
# MAGIC -- DROP TABLE IF EXISTS Gold.dim_cliente;
# MAGIC -- DROP TABLE IF EXISTS Gold.dim_producto;
# MAGIC -- DROP TABLE IF EXISTS Gold.dim_ciudad;
# MAGIC -- DROP TABLE IF EXISTS Gold.dim_canal_venta;
# MAGIC -- DROP TABLE IF EXISTS Gold.dim_estado_factura;
# MAGIC SHOW SCHEMAS

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE SCHEMA IF NOT EXISTS Gold

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS Gold.dim_fecha (
# MAGIC     sk_fecha          INT      COMMENT 'Llave aaaammdd',
# MAGIC     fecha             DATE,
# MAGIC     anio              INT,
# MAGIC     semestre          INT,
# MAGIC     trimestre         INT,
# MAGIC     mes               INT,
# MAGIC     nombre_mes        STRING,
# MAGIC     anio_mes          STRING   COMMENT 'aaaa-mm',
# MAGIC     dia               INT,
# MAGIC     dia_semana        INT      COMMENT '1 = lunes ... 7 = domingo',
# MAGIC     nombre_dia        STRING,
# MAGIC     es_fin_de_semana  BOOLEAN
# MAGIC ) USING DELTA

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS Gold.dim_cliente (
# MAGIC     sk_cliente      INT,
# MAGIC     id_cliente      STRING,
# MAGIC     nombre_cliente  STRING,
# MAGIC     genero_codigo   STRING,
# MAGIC     genero          STRING
# MAGIC ) USING DELTA

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS Gold.dim_producto (
# MAGIC     sk_producto   INT,
# MAGIC     id_producto   STRING,
# MAGIC     producto      STRING,
# MAGIC     precio_lista  DECIMAL(18,2)
# MAGIC ) USING DELTA

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS Gold.dim_ciudad (
# MAGIC     sk_ciudad  INT,
# MAGIC     id_ciudad  STRING,
# MAGIC     ciudad     STRING
# MAGIC ) USING DELTA

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS Gold.dim_canal_venta (
# MAGIC     sk_canal_venta  INT,
# MAGIC     canal_venta     STRING
# MAGIC ) USING DELTA

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS Gold.dim_estado_factura (
# MAGIC     sk_estado_factura  INT,
# MAGIC     estado_codigo      STRING,
# MAGIC     estado_factura     STRING
# MAGIC ) USING DELTA

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS Gold.fact_ventas (
# MAGIC     id_venta           INT            COMMENT 'Dimensión degenerada: número de factura',
# MAGIC     sk_fecha           INT,
# MAGIC     sk_cliente         INT,
# MAGIC     sk_producto        INT,
# MAGIC     sk_ciudad          INT,
# MAGIC     sk_canal_venta     INT,
# MAGIC     sk_estado_factura  INT,
# MAGIC     cantidad           INT,
# MAGIC     precio_unitario    DECIMAL(18,2),
# MAGIC     valor_venta        DECIMAL(18,2),
# MAGIC     fecha_carga        TIMESTAMP
# MAGIC ) USING DELTA

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SHOW TABLES IN Gold

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
