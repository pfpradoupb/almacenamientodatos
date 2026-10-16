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
# MAGIC -- Dimensión Fecha: un registro por día entre la fecha mínima y máxima de ventas
# MAGIC INSERT OVERWRITE Gold.dim_fecha
# MAGIC SELECT
# MAGIC     CAST(date_format(f, 'yyyyMMdd') AS INT) AS SkFecha,
# MAGIC     f                                        AS Fecha,
# MAGIC     year(f)                                  AS Anio,
# MAGIC     quarter(f)                               AS Trimestre,
# MAGIC     month(f)                                 AS Mes,
# MAGIC     CASE month(f) WHEN 1 THEN 'Enero' WHEN 2 THEN 'Febrero' WHEN 3 THEN 'Marzo'
# MAGIC                   WHEN 4 THEN 'Abril' WHEN 5 THEN 'Mayo' WHEN 6 THEN 'Junio'
# MAGIC                   WHEN 7 THEN 'Julio' WHEN 8 THEN 'Agosto' WHEN 9 THEN 'Septiembre'
# MAGIC                   WHEN 10 THEN 'Octubre' WHEN 11 THEN 'Noviembre' ELSE 'Diciembre' END AS NombreMes,
# MAGIC     day(f)                                   AS Dia,
# MAGIC     dayofweek(f)                             AS DiaSemana,   -- 1 = domingo
# MAGIC     CASE dayofweek(f) WHEN 1 THEN 'Domingo' WHEN 2 THEN 'Lunes' WHEN 3 THEN 'Martes'
# MAGIC                       WHEN 4 THEN 'Miércoles' WHEN 5 THEN 'Jueves' WHEN 6 THEN 'Viernes'
# MAGIC                       ELSE 'Sábado' END     AS NombreDia
# MAGIC FROM (
# MAGIC     SELECT explode(sequence(MIN(FechaVenta), MAX(FechaVenta), INTERVAL 1 DAY)) AS f
# MAGIC     FROM Silver.ventas
# MAGIC ) t

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC INSERT OVERWRITE Gold.dim_cliente
# MAGIC SELECT ROW_NUMBER() OVER (ORDER BY IdCliente) AS SkCliente, IdCliente, Cliente, Genero, GeneroDesc
# MAGIC FROM (
# MAGIC     SELECT IdCliente, MAX(Cliente) AS Cliente, MAX(Genero) AS Genero, MAX(GeneroDesc) AS GeneroDesc
# MAGIC     FROM Silver.ventas
# MAGIC     GROUP BY IdCliente
# MAGIC ) c

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC INSERT OVERWRITE Gold.dim_ciudad
# MAGIC SELECT ROW_NUMBER() OVER (ORDER BY IdCiudad) AS SkCiudad, IdCiudad, Ciudad
# MAGIC FROM (SELECT IdCiudad, MAX(Ciudad) AS Ciudad FROM Silver.ventas GROUP BY IdCiudad) c

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC INSERT OVERWRITE Gold.dim_producto
# MAGIC SELECT ROW_NUMBER() OVER (ORDER BY IdProducto) AS SkProducto, IdProducto, Producto
# MAGIC FROM (SELECT IdProducto, MAX(Producto) AS Producto FROM Silver.ventas GROUP BY IdProducto) p

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC INSERT OVERWRITE Gold.dim_canal
# MAGIC SELECT ROW_NUMBER() OVER (ORDER BY CanalVenta) AS SkCanal, CanalVenta
# MAGIC FROM (SELECT DISTINCT CanalVenta FROM Silver.ventas) c

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC INSERT OVERWRITE Gold.dim_estado_factura
# MAGIC SELECT ROW_NUMBER() OVER (ORDER BY EstadoFactura) AS SkEstado, EstadoFactura, EstadoFacturaDesc
# MAGIC FROM (SELECT DISTINCT EstadoFactura, EstadoFacturaDesc FROM Silver.ventas) e

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC INSERT OVERWRITE Gold.fact_ventas
# MAGIC SELECT
# MAGIC     s.IdVenta,
# MAGIC     CAST(date_format(s.FechaVenta, 'yyyyMMdd') AS INT) AS SkFecha,
# MAGIC     dc.SkCliente,
# MAGIC     dci.SkCiudad,
# MAGIC     dp.SkProducto,
# MAGIC     dca.SkCanal,
# MAGIC     de.SkEstado,
# MAGIC     s.Cantidad,
# MAGIC     s.PrecioUnitario,
# MAGIC     s.ValorVenta
# MAGIC FROM Silver.ventas s
# MAGIC JOIN Gold.dim_cliente        dc  ON s.IdCliente     = dc.IdCliente
# MAGIC JOIN Gold.dim_ciudad         dci ON s.IdCiudad      = dci.IdCiudad
# MAGIC JOIN Gold.dim_producto       dp  ON s.IdProducto    = dp.IdProducto
# MAGIC JOIN Gold.dim_canal          dca ON s.CanalVenta    = dca.CanalVenta
# MAGIC JOIN Gold.dim_estado_factura de  ON s.EstadoFactura = de.EstadoFactura

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT 'silver' AS capa, COUNT(*) AS filas, SUM(ValorVenta) AS total FROM Silver.ventas
# MAGIC UNION ALL
# MAGIC SELECT 'gold',  COUNT(*), SUM(ValorVenta) FROM Gold.fact_ventas

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- Ventas canceladas (efectivas) por ciudad y mes
# MAGIC SELECT ci.Ciudad, f.Anio, f.Mes, f.NombreMes, SUM(v.ValorVenta) AS Ventas
# MAGIC FROM Gold.fact_ventas v
# MAGIC JOIN Gold.dim_ciudad ci        ON v.SkCiudad = ci.SkCiudad
# MAGIC JOIN Gold.dim_fecha f          ON v.SkFecha  = f.SkFecha
# MAGIC JOIN Gold.dim_estado_factura e ON v.SkEstado = e.SkEstado
# MAGIC WHERE e.EstadoFactura = 'C'
# MAGIC GROUP BY ci.Ciudad, f.Anio, f.Mes, f.NombreMes
# MAGIC ORDER BY ci.Ciudad, f.Anio, f.Mes

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
