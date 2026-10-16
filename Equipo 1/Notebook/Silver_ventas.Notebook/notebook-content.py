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

# Welcome to your new notebook
# Type here in the cell editor to add code!

df_bronze = spark.read.parquet("Files/Parquet/ventas.parquet")
# df now is a Spark DataFrame containing parquet data from "Files/Parquet/ventas.parquet".
display(df_bronze)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Se convierte todo a string primero (Bronze conserva la estructura original) y se aplica trim
df = df_bronze.select([F.trim(F.col(c).cast("string")).alias(c) for c in df_bronze.columns])
# Cadenas vacías -> NULL
df = df.select([F.when(F.col(c) == "", None).otherwise(F.col(c)).alias(c) for c in df.columns])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Perfilamiento de datos
display(df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]))
display(df.groupBy("Ciudad").count().orderBy("Ciudad"))
display(df.groupBy("Genero").count())
display(df.groupBy("EstadoFactura").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Estandarización de ciudades, códigos y fecha
ciudad_norm = F.translate(F.lower(F.col("Ciudad")), "áéíóú", "aeiou")

df = (df
    .withColumn("Ciudad",
        F.when(ciudad_norm == "bogota",   "BOGOTA")
         .when(ciudad_norm == "medellin", "MEDELLIN")
         .when(ciudad_norm == "cali",     "CALI")
         .otherwise(F.initcap(F.col("Ciudad"))))
    .withColumn("Genero",        F.upper("Genero"))
    .withColumn("EstadoFactura", F.upper("EstadoFactura"))
    .withColumn("CanalVenta",    F.upper("CanalVenta"))
    .withColumn("IdCliente",     F.upper("IdCliente"))
    .withColumn("IdCiudad",      F.upper("IdCiudad"))
    .withColumn("IdProducto",    F.upper("IdProducto"))
    # Fecha: se prueba cada formato presente en el archivo
    .withColumn("FechaVenta_dt", F.coalesce(
        F.to_date(F.expr("try_to_timestamp(FechaVenta, 'yyyy-MM-dd')")),
        F.to_date(F.expr("try_to_timestamp(FechaVenta, 'dd/MM/yyyy')")),
        F.to_date(F.expr("try_to_timestamp(FechaVenta, 'dd-MMM-yyyy')"))))
)
display(df.groupBy("Ciudad").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Descripcion de genero y estado factura
df = (df
    .withColumn("GeneroDesc",
        F.when(F.col("Genero") == "F", "Femenino")
         .when(F.col("Genero") == "M", "Masculino")
         .otherwise("No definido"))
    .withColumn("EstadoFacturaDesc",
        F.when(F.col("EstadoFactura") == "C", "Cancelado")
         .when(F.col("EstadoFactura") == "R", "Rechazado")
         .when(F.col("EstadoFactura") == "A", "Anulado")
         .otherwise("No definido"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Tratamientos de valores nulos en columna cliente
w = Window.partitionBy("IdCliente")
df = df.withColumn("Cliente",
        F.coalesce(F.col("Cliente"), F.first("Cliente", ignorenulls=True).over(w), F.lit("Sin nombre")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Validación numerica de cantidad y precio unitario
df = (df
    .withColumn("Cantidad_num", F.expr("try_cast(Cantidad AS DOUBLE)"))
    .withColumn("Precio_num",   F.expr("try_cast(PrecioUnitario AS DECIMAL(18,2))"))
    .withColumn("IdVenta_num",  F.expr("try_cast(try_cast(IdVenta AS DOUBLE) AS INT)"))
)

df = df.withColumn("MotivoRechazo", F.concat_ws("; ",
    F.when(F.col("Cantidad").isNull(), F.lit("Cantidad nula")),
    F.when(F.col("Cantidad").isNotNull() & F.col("Cantidad_num").isNull(), F.lit("Cantidad no numérica")),
    F.when((F.col("Cantidad_num") <= 0) | (F.col("Cantidad_num") != F.floor("Cantidad_num")), F.lit("Cantidad no entera o <= 0")),
    F.when(F.col("Precio_num").isNull(), F.lit("Precio inválido")),
    F.when(F.col("FechaVenta_dt").isNull(), F.lit("Fecha inválida")),
    F.when(F.col("IdVenta_num").isNull(), F.lit("IdVenta inválido"))
))
df = df.withColumn("MotivoRechazo", F.when(F.col("MotivoRechazo") == "", None).otherwise(F.col("MotivoRechazo")))

df_validos    = df.filter(F.col("MotivoRechazo").isNull())
df_rechazados = df.filter(F.col("MotivoRechazo").isNotNull())
print("Válidos:", df_validos.count(), " | Rechazados:", df_rechazados.count())
display(df_rechazados.groupBy("MotivoRechazo").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


df_silver = (df_validos
    .withColumn("Cantidad", F.col("Cantidad_num").cast("int"))
    .withColumn("PrecioUnitario", F.col("Precio_num"))
    .withColumn("ValorVenta", (F.col("Cantidad") * F.col("PrecioUnitario")).cast("decimal(18,2)"))
    .select(
        F.col("IdVenta_num").alias("IdVenta"),
        F.col("FechaVenta_dt").alias("FechaVenta"),
        "IdCliente", "Cliente", "Genero", "GeneroDesc",
        "IdCiudad", "Ciudad", "EstadoFactura", "EstadoFacturaDesc",
        "CanalVenta", "IdProducto", "Producto",
        "Cantidad", "PrecioUnitario", "ValorVenta",
        F.current_timestamp().alias("FechaCarga"))
)


df_silver.write.format("delta").mode("overwrite").saveAsTable("LS_Bronze.Silver.ventas")

(df_rechazados
    .select("IdVenta", "FechaVenta", "IdCliente", "IdProducto", "Cantidad", "PrecioUnitario",
            "MotivoRechazo", F.current_timestamp().alias("FechaCarga"))
    .write.format("delta").mode("overwrite").saveAsTable("LS_Bronze.Silver.ventas_rechazadas"))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT Ciudad, GeneroDesc, EstadoFacturaDesc, COUNT(*) AS Lineas, SUM(ValorVenta) AS Ventas
# MAGIC FROM LS_Bronze.Silver.ventas
# MAGIC GROUP BY Ciudad, GeneroDesc, EstadoFacturaDesc
# MAGIC ORDER BY Ciudad

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT MIN(FechaVenta), MAX(FechaVenta), COUNT(*) AS filas,
# MAGIC        SUM(CASE WHEN Cliente IS NULL THEN 1 ELSE 0 END) AS clientes_nulos
# MAGIC FROM LS_Bronze.Silver.ventas

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
