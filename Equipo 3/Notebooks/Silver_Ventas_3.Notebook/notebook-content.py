# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "6ab51063-fb51-4300-881d-8af622a3c12a",
# META       "default_lakehouse_name": "LS_Bronze_Silver",
# META       "default_lakehouse_workspace_id": "2fda67b6-b1ee-4cd2-952c-be05ec8df05f",
# META       "known_lakehouses": [
# META         {
# META           "id": "6ab51063-fb51-4300-881d-8af622a3c12a"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# LECTURA DE DATOS BRONZE

from pyspark.sql import functions as F

df = spark.read.parquet("Files/Parquet/ventas3.parquet")

print("Total de registros Bronze:", df.count())

display(df.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# TRANSFORMACION SILVER

from pyspark.sql import functions as F


df_silver = df

for columna in df_silver.columns:
    df_silver = df_silver.withColumn(
        columna, F.trim(F.col(columna))
    )

# Estandarizar ciudades
ciudad = F.lower(F.col("Ciudad"))

df_silver = df_silver.withColumn(
    "Ciudad",
    F.when(ciudad.isin("bogota", "bogotá"), "Bogotá")
     .when(ciudad.isin("medellin", "medellín"), "Medellín")
     .when(ciudad == "cali", "Cali")
     .otherwise(F.col("Ciudad"))
)

# Estandarizar género y estado de factura
df_silver = df_silver.withColumn(
    "Genero", F.upper(F.col("Genero"))
).withColumn(
    "EstadoFactura", F.upper(F.col("EstadoFactura"))
)

# Crear valores descriptivos
df_silver = df_silver.withColumn(
    "GeneroDescripcion",
    F.when(F.col("Genero") == "F", "Femenino")
     .when(F.col("Genero") == "M", "Masculino")
).withColumn(
    "EstadoFacturaDescripcion",
    F.when(F.col("EstadoFactura") == "C", "Cancelado")
     .when(F.col("EstadoFactura") == "R", "Rechazado")
     .when(F.col("EstadoFactura") == "A", "Anulado")
)

# Convertir tipos de datos
df_silver = df_silver.withColumn(
    "Cantidad", F.expr("try_cast(Cantidad AS INT)")
).withColumn(
    "PrecioUnitario",
    F.expr("try_cast(PrecioUnitario AS DECIMAL(18,2))")
)

# Convertir fecha
df_silver = df_silver.withColumn(
    "FechaVenta",
    F.coalesce(
        F.expr("try_to_timestamp(FechaVenta, 'yyyy-MM-dd')"),
        F.expr("try_to_timestamp(FechaVenta, 'dd-MMM-yyyy')"),
        F.expr("try_to_timestamp(FechaVenta, 'dd/MM/yyyy')")
    ).cast("date")
)

# Tratar clientes nulos
df_silver = df_silver.fillna({
    "Cliente": "No informado"
})

# Calcular valor de venta
df_silver = df_silver.withColumn(
    "ValorVenta",
    F.col("Cantidad") * F.col("PrecioUnitario")
)

print("TOTAL REGISTROS SILVER:", df_silver.count())

display(df_silver.limit(10))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# VALIDACION DE CALIDAD SILVER

from pyspark.sql import functions as F

print("=== CONTROL DE CALIDAD SILVER ===")

print("Total registros:", df_silver.count())

print("\nVALORES NULOS:")
df_silver.select([
    F.sum(F.col(c).isNull().cast("int")).alias(c)
    for c in [
        "FechaVenta", "Cliente", "Cantidad",
        "PrecioUnitario", "ValorVenta"
    ]
]).show()

print("\nCIUDADES NORMALIZADAS:")
df_silver.groupBy("Ciudad").count().show()

print("\nCANTIDADES CERO O NEGATIVAS:")
print(df_silver.filter(F.col("Cantidad") <= 0).count())

print("\nREGISTROS CON CANTIDAD NULA:")
print(df_silver.filter(F.col("Cantidad").isNull()).count())

print("\nVENTAS POR AÑO:")
df_silver.groupBy(
    F.year("FechaVenta").alias("Anio")
).count().orderBy("Anio").show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# CREAR Y GUARDAR TABLA SILVER

# Crear el esquema Silver si no existe
spark.sql("CREATE DATABASE IF NOT EXISTS Silver")

# Registrar las transformaciones como vista temporal
df_silver.createOrReplaceTempView("vw_ventas_silver")

# Crear tabla Silver mediante Spark SQL
spark.sql("""
CREATE OR REPLACE TABLE Silver.Ventas
USING DELTA
AS
SELECT *
FROM vw_ventas_silver
""")

print("TABLA SILVER CREADA CORRECTAMENTE")

# Verificar los registros almacenados
spark.sql("""
SELECT COUNT(*) AS TotalRegistros
FROM Silver.Ventas
""").show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# ANALISIS PREVIO AL MODELO GOLD

spark.sql("""
SELECT
    COUNT(*) AS TotalFilas,
    COUNT(DISTINCT IdVenta) AS FacturasUnicas,
    COUNT(DISTINCT IdCliente) AS ClientesUnicos,
    COUNT(DISTINCT IdProducto) AS ProductosUnicos,
    COUNT(DISTINCT IdCiudad) AS CiudadesUnicas,
    COUNT(DISTINCT CanalVenta) AS CanalesUnicos
FROM Silver.Ventas
""").show()

spark.sql("""
SELECT
    IdVenta,
    COUNT(*) AS NumeroFilas
FROM Silver.Ventas
GROUP BY IdVenta
HAVING COUNT(*) > 1
ORDER BY NumeroFilas DESC
LIMIT 10
""").show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# VALIDACION PARA MODELO GOLD

print("=== CANALES DE VENTA ===")
spark.sql("""
SELECT
    CanalVenta,
    COUNT(*) AS Registros
FROM Silver.Ventas
GROUP BY CanalVenta
ORDER BY CanalVenta
""").show(30, False)

print("=== ESTADOS DE FACTURA ===")
spark.sql("""
SELECT
    EstadoFactura,
    EstadoFacturaDescripcion,
    COUNT(*) AS Registros
FROM Silver.Ventas
GROUP BY EstadoFactura, EstadoFacturaDescripcion
ORDER BY EstadoFactura
""").show(30, False)

print("=== VALIDACION CLIENTES ===")
spark.sql("""
SELECT
    IdCliente,
    COUNT(DISTINCT Cliente) AS NombresDiferentes,
    COUNT(DISTINCT Genero) AS GenerosDiferentes
FROM Silver.Ventas
GROUP BY IdCliente
HAVING COUNT(DISTINCT Cliente) > 1
    OR COUNT(DISTINCT Genero) > 1
""").show(30, False)

print("=== VALIDACION PRODUCTOS ===")
spark.sql("""
SELECT
    IdProducto,
    COUNT(DISTINCT Producto) AS NombresDiferentes
FROM Silver.Ventas
GROUP BY IdProducto
HAVING COUNT(DISTINCT Producto) > 1
""").show(30, False)

print("=== FILAS EXACTAMENTE DUPLICADAS ===")
spark.sql("""
SELECT
    COUNT(*) AS TotalFilas,
    COUNT(DISTINCT *) AS FilasDistintas
FROM Silver.Ventas
""").show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# ANALISIS DE INCONSISTENCIAS

print("=== NOMBRES POR CLIENTE ===")

spark.sql("""
SELECT
    IdCliente,
    Cliente,
    COUNT(*) AS Registros
FROM Silver.Ventas
GROUP BY IdCliente, Cliente
ORDER BY IdCliente, Registros DESC
""").show(50, False)


print("=== DUPLICADOS EXACTOS ===")

# Comparar todas las columnas de Silver
columnas = spark.table("Silver.Ventas").columns

df_revision = spark.table("Silver.Ventas")

duplicados = (
    df_revision
    .groupBy(*columnas)
    .count()
    .filter("count > 1")
)

print("Grupos de filas duplicadas:",
      duplicados.count())

print("Filas duplicadas adicionales:")
duplicados.selectExpr(
    "sum(count - 1) AS DuplicadosAdicionales"
).show()

print("=== DUPLICADOS CON CANTIDAD NULA ===")

duplicados.groupBy(
    "Cantidad"
).agg(
    F.sum(F.col("count") - 1)
    .alias("DuplicadosAdicionales")
).orderBy("Cantidad").show(30, False)


print("=== CANALES NORMALIZADOS ===")

spark.sql("""
SELECT
    UPPER(TRIM(CanalVenta)) AS CanalNormalizado,
    COUNT(*) AS Registros
FROM Silver.Ventas
GROUP BY UPPER(TRIM(CanalVenta))
ORDER BY CanalNormalizado
""").show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# MEJORA DE CALIDAD DE DATOS SILVER

from pyspark.sql import functions as F
from pyspark.sql.window import Window

df_silver = spark.table("Silver.Ventas")

# nombre válido más frecuente por cliente
nombres = (
    df_silver
    .filter(
        (F.col("Cliente").isNotNull()) &
        (F.col("Cliente") != "No informado")
    )
    .groupBy("IdCliente", "Cliente")
    .count()
)

ventana = Window.partitionBy("IdCliente").orderBy(
    F.desc("count"),
    F.asc("Cliente")
)

nombres = (
    nombres
    .withColumn("rn", F.row_number().over(ventana))
    .filter(F.col("rn") == 1)
    .select(
        "IdCliente",
        F.col("Cliente").alias("NombreRecuperado")
    )
)

# Recuperar nombres faltantes
df_silver = df_silver.join(
    nombres, on="IdCliente", how="left"
)

df_silver = df_silver.withColumn(
    "Cliente",
    F.when(
        (F.col("Cliente") == "No informado") |
        F.col("Cliente").isNull(),
        F.coalesce(
            F.col("NombreRecuperado"),
            F.col("Cliente")
        )
    ).otherwise(F.col("Cliente"))
).drop("NombreRecuperado")

# Normalizar canales
df_silver = df_silver.withColumn(
    "CanalVenta",
    F.upper(F.trim(F.col("CanalVenta")))
)

# Verificar resultados
print("TOTAL REGISTROS:", df_silver.count())

print("CLIENTES NO INFORMADOS:")
print(
    df_silver.filter(
        F.col("Cliente") == "No informado"
    ).count()
)

print("CANALES NORMALIZADOS:")
df_silver.groupBy("CanalVenta").count().show()

print("CANTIDADES NULAS:")
print(
    df_silver.filter(
        F.col("Cantidad").isNull()
    ).count()
)

print("FILAS DISTINTAS:")
print(df_silver.dropDuplicates().count())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


#GUARDAR SILVER

from pyspark import StorageLevel

# Materializar el DataFrame antes de sobrescribir
df_silver = df_silver.persist(StorageLevel.MEMORY_AND_DISK)
total_antes = df_silver.count()

# Guardar temporalmente los datos transformados
ruta_temporal = "Files/Temporal/ventas_silver_final"

df_silver.write.mode("overwrite").parquet(ruta_temporal)

# Leer desde una fuente independiente
df_final = spark.read.parquet(ruta_temporal)

# Registrar vista temporal para Spark SQL
df_final.createOrReplaceTempView("vw_silver_final")

# Actualizar tabla Silver
spark.sql("""
CREATE OR REPLACE TABLE Silver.Ventas
USING DELTA
AS
SELECT *
FROM vw_silver_final
""")

# Verificar la tabla persistida
resultado = spark.sql("""
SELECT
    COUNT(*) AS TotalRegistros,
    SUM(CASE WHEN Cliente = 'No informado'
        THEN 1 ELSE 0 END) AS ClientesSinNombre,
    SUM(CASE WHEN Cantidad IS NULL
        THEN 1 ELSE 0 END) AS CantidadesNulas,
    COUNT(DISTINCT CanalVenta) AS Canales
FROM Silver.Ventas
""")

print("=== VALIDACION SILVER FINAL ===")
resultado.show()

print("Registros antes de guardar:", total_antes)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
