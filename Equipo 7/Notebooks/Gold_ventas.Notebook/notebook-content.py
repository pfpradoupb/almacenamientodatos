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

# # Gold_ventas · Carga del modelo dimensional
# 
# * **Lakehouse predeterminado:** el lakehouse único del equipo (p. ej. `Lakehouse_Total`).
# * Requiere `Silver_ventas` (Silver cargado) y `Gold_ventas_DDL` (tablas Gold creadas).
# * Primero se cargan las dimensiones (llaves sustitutas con `ROW_NUMBER()`), luego la tabla de hechos buscando cada llave en su dimensión.
# * Carga completa con `INSERT OVERWRITE` → se puede re-ejecutar sin duplicar datos.

# MARKDOWN ********************

# ## 1. Dimensiones

# CELL ********************

# MAGIC %%sql
# MAGIC -- Gold.dim_fecha: calendario completo desde el 1 de enero del primer año hasta el 31 de diciembre del último
# MAGIC INSERT OVERWRITE TABLE Gold.dim_fecha
# MAGIC SELECT
# MAGIC     CAST(date_format(fecha, 'yyyyMMdd') AS INT)              AS sk_fecha,
# MAGIC     fecha,
# MAGIC     year(fecha)                                              AS anio,
# MAGIC     CASE WHEN month(fecha) <= 6 THEN 1 ELSE 2 END            AS semestre,
# MAGIC     quarter(fecha)                                           AS trimestre,
# MAGIC     month(fecha)                                             AS mes,
# MAGIC     CASE month(fecha)
# MAGIC         WHEN 1 THEN 'Enero'      WHEN 2 THEN 'Febrero'   WHEN 3 THEN 'Marzo'
# MAGIC         WHEN 4 THEN 'Abril'      WHEN 5 THEN 'Mayo'      WHEN 6 THEN 'Junio'
# MAGIC         WHEN 7 THEN 'Julio'      WHEN 8 THEN 'Agosto'    WHEN 9 THEN 'Septiembre'
# MAGIC         WHEN 10 THEN 'Octubre'   WHEN 11 THEN 'Noviembre' ELSE 'Diciembre' END AS nombre_mes,
# MAGIC     date_format(fecha, 'yyyy-MM')                            AS anio_mes,
# MAGIC     dayofmonth(fecha)                                        AS dia,
# MAGIC     ((dayofweek(fecha) + 5) % 7) + 1                         AS dia_semana,
# MAGIC     CASE ((dayofweek(fecha) + 5) % 7) + 1
# MAGIC         WHEN 1 THEN 'Lunes'  WHEN 2 THEN 'Martes'  WHEN 3 THEN 'Miércoles'
# MAGIC         WHEN 4 THEN 'Jueves' WHEN 5 THEN 'Viernes' WHEN 6 THEN 'Sábado' ELSE 'Domingo' END AS nombre_dia,
# MAGIC     dayofweek(fecha) IN (1, 7)                               AS es_fin_de_semana
# MAGIC FROM (
# MAGIC     SELECT explode(sequence(
# MAGIC                make_date(year(MIN(FechaVenta)), 1, 1),
# MAGIC                make_date(year(MAX(FechaVenta)), 12, 31),
# MAGIC                INTERVAL 1 DAY)) AS fecha
# MAGIC     FROM Silver.ventas
# MAGIC ) cal

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC INSERT OVERWRITE TABLE Gold.dim_cliente
# MAGIC SELECT
# MAGIC     ROW_NUMBER() OVER (ORDER BY IdCliente) AS sk_cliente,
# MAGIC     IdCliente                              AS id_cliente,
# MAGIC     MAX(Cliente)                           AS nombre_cliente,
# MAGIC     MAX(Genero)                            AS genero_codigo,
# MAGIC     MAX(GeneroDescripcion)                 AS genero
# MAGIC FROM Silver.ventas
# MAGIC GROUP BY IdCliente

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC INSERT OVERWRITE TABLE Gold.dim_producto
# MAGIC SELECT
# MAGIC     ROW_NUMBER() OVER (ORDER BY IdProducto) AS sk_producto,
# MAGIC     IdProducto                              AS id_producto,
# MAGIC     MAX(Producto)                           AS producto,
# MAGIC     MAX(PrecioUnitario)                     AS precio_lista
# MAGIC FROM Silver.ventas
# MAGIC GROUP BY IdProducto

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC INSERT OVERWRITE TABLE Gold.dim_ciudad
# MAGIC SELECT
# MAGIC     ROW_NUMBER() OVER (ORDER BY IdCiudad) AS sk_ciudad,
# MAGIC     IdCiudad                              AS id_ciudad,
# MAGIC     MAX(Ciudad)                           AS ciudad
# MAGIC FROM Silver.ventas
# MAGIC GROUP BY IdCiudad

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC INSERT OVERWRITE TABLE Gold.dim_canal_venta
# MAGIC SELECT
# MAGIC     ROW_NUMBER() OVER (ORDER BY CanalVenta) AS sk_canal_venta,
# MAGIC     CanalVenta                              AS canal_venta
# MAGIC FROM (SELECT DISTINCT CanalVenta FROM Silver.ventas) c

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC INSERT OVERWRITE TABLE Gold.dim_estado_factura
# MAGIC SELECT
# MAGIC     ROW_NUMBER() OVER (ORDER BY EstadoFactura) AS sk_estado_factura,
# MAGIC     EstadoFactura                              AS estado_codigo,
# MAGIC     EstadoFacturaDescripcion                   AS estado_factura
# MAGIC FROM (SELECT DISTINCT EstadoFactura, EstadoFacturaDescripcion FROM Silver.ventas) e

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Tabla de hechos

# CELL ********************

# MAGIC %%sql
# MAGIC INSERT OVERWRITE TABLE Gold.fact_ventas
# MAGIC SELECT
# MAGIC     s.IdVenta                                         AS id_venta,
# MAGIC     f.sk_fecha,
# MAGIC     cl.sk_cliente,
# MAGIC     p.sk_producto,
# MAGIC     ci.sk_ciudad,
# MAGIC     ca.sk_canal_venta,
# MAGIC     e.sk_estado_factura,
# MAGIC     s.Cantidad                                        AS cantidad,
# MAGIC     s.PrecioUnitario                                  AS precio_unitario,
# MAGIC     s.ValorVenta                                      AS valor_venta,
# MAGIC     current_timestamp()                               AS fecha_carga
# MAGIC FROM Silver.ventas s
# MAGIC JOIN Gold.dim_fecha          f  ON f.fecha          = s.FechaVenta
# MAGIC JOIN Gold.dim_cliente        cl ON cl.id_cliente    = s.IdCliente
# MAGIC JOIN Gold.dim_producto       p  ON p.id_producto    = s.IdProducto
# MAGIC JOIN Gold.dim_ciudad         ci ON ci.id_ciudad     = s.IdCiudad
# MAGIC JOIN Gold.dim_canal_venta    ca ON ca.canal_venta   = s.CanalVenta
# MAGIC JOIN Gold.dim_estado_factura e  ON e.estado_codigo  = s.EstadoFactura

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Validaciones
# La tabla de hechos debe tener **las mismas filas y el mismo valor total** que `Silver.ventas` (si no, alguna llave no encontró su dimensión).

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT
# MAGIC     (SELECT COUNT(*)        FROM Silver.ventas) AS filas_silver,
# MAGIC     (SELECT COUNT(*)        FROM Gold.fact_ventas)     AS filas_fact,
# MAGIC     (SELECT SUM(ValorVenta) FROM Silver.ventas) AS valor_silver,
# MAGIC     (SELECT SUM(valor_venta) FROM Gold.fact_ventas)    AS valor_fact

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT 'Gold.dim_fecha' AS tabla, COUNT(*) AS filas FROM Gold.dim_fecha
# MAGIC UNION ALL SELECT 'Gold.dim_cliente', COUNT(*) FROM Gold.dim_cliente
# MAGIC UNION ALL SELECT 'Gold.dim_producto', COUNT(*) FROM Gold.dim_producto
# MAGIC UNION ALL SELECT 'Gold.dim_ciudad', COUNT(*) FROM Gold.dim_ciudad
# MAGIC UNION ALL SELECT 'Gold.dim_canal_venta', COUNT(*) FROM Gold.dim_canal_venta
# MAGIC UNION ALL SELECT 'Gold.dim_estado_factura', COUNT(*) FROM Gold.dim_estado_factura
# MAGIC UNION ALL SELECT 'Gold.fact_ventas', COUNT(*) FROM Gold.fact_ventas

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Consulta de negocio de ejemplo (ventas canceladas por ciudad y mes)

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT ci.ciudad, f.anio_mes, SUM(v.valor_venta) AS ventas, SUM(v.cantidad) AS unidades
# MAGIC FROM Gold.fact_ventas v
# MAGIC JOIN Gold.dim_ciudad ci         ON ci.sk_ciudad = v.sk_ciudad
# MAGIC JOIN Gold.dim_fecha f           ON f.sk_fecha = v.sk_fecha
# MAGIC JOIN Gold.dim_estado_factura e  ON e.sk_estado_factura = v.sk_estado_factura
# MAGIC WHERE e.estado_factura = 'Cancelado'
# MAGIC GROUP BY ci.ciudad, f.anio_mes
# MAGIC ORDER BY f.anio_mes, ci.ciudad

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
