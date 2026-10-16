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

# # Silver_ventas · Transformaciones (lee el parquet de Bronze)
# 
# * **Lakehouse predeterminado:** el lakehouse único del equipo (p. ej. `Lakehouse_Total`, con schemas).
# * Lee `Files/Parquet/Ventas.parquet` (Bronze) y escribe en el schema `Silver` del mismo lakehouse.
# * Requiere haber ejecutado antes `Silver_ventas_DDL`.
# * Si el pipeline guardó el parquet con columnas `Prop_0, Prop_1, …` (sin marcar *First row as header*), el notebook detecta la fila de encabezados y la usa como nombres de columna.
# 
# Reglas de calidad aplicadas:
# 
# | # | Regla del enunciado | Cómo se resuelve |
# |---|---|---|
# | 1 | Espacios al inicio/fin | `trim()` a todas las columnas; texto vacío → `NULL` |
# | 2 | Estandarizar ciudades | Se normaliza (minúsculas, sin tildes) y se mapea a `Bogotá`, `Medellín`, `Cali` |
# | 3 | Fecha a tipo `Date` | La fuente trae 3 formatos: `yyyy-MM-dd`, `dd/MM/yyyy`, `dd-MMM-yyyy` |
# | 4 | Nulos | `Cliente` nulo → se completa con el nombre del mismo `IdCliente`; `Cantidad` nula → cuarentena |
# | 5 | Cantidad numérica | Debe ser numérica, entera y > 0; si no → cuarentena |
# | 6 | Descripciones | `Genero` F/M → Femenino/Masculino; `EstadoFactura` C/R/A → Cancelado/Rechazado/Anulado |
# | 7 | Columna calculada | `ValorVenta = Cantidad × PrecioUnitario` |


# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql import Window

# Ruta del parquet de Bronze dentro de Files del lakehouse.
# Si el tuyo está en otra carpeta: clic en "..." sobre el archivo > "Copy relative path for Spark" y pégala aquí.
BRONZE_PATH = "Files/Parquet/Ventas.parquet"

df_bronze = spark.read.parquet(BRONZE_PATH)

# Si el Copy data se ejecutó sin "First row as header", las columnas se llaman Prop_0, Prop_1, ...
# y los nombres reales vienen como una fila de datos. Se detecta esa fila y se usa como encabezado.
if "IdVenta" not in df_bronze.columns:
    primera_col = df_bronze.columns[0]
    fila_header = df_bronze.filter(F.trim(F.col(primera_col).cast("string")) == "IdVenta").first()
    nombres = [str(v).strip() for v in fila_header]
    df_bronze = (df_bronze
                 .filter(F.col(primera_col).isNull() | (F.trim(F.col(primera_col).cast("string")) != "IdVenta"))
                 .toDF(*nombres))
    print("Encabezados recuperados de la primera fila:", nombres)

print("Filas leídas de Bronze:", df_bronze.count())
df_bronze.printSchema()
display(df_bronze.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1. Perfilamiento: identificar problemas de calidad
# Conteo de nulos/vacíos por columna y valores distintos de las columnas categóricas (antes de limpiar).

# CELL ********************

def es_vacio(c):
    return F.col(c).isNull() | (F.trim(F.col(c).cast("string")) == "")

df_nulos = df_bronze.select([F.sum(F.when(es_vacio(c), 1).otherwise(0)).alias(c) for c in df_bronze.columns])
display(df_nulos)

for c in ["Ciudad", "Genero", "EstadoFactura", "CanalVenta"]:
    display(df_bronze.groupBy(c).count().orderBy(c))

# Formatos de fecha presentes
display(
    df_bronze.select(
        F.when(F.col("FechaVenta").rlike(r"^\d{4}-\d{2}-\d{2}$"), "yyyy-MM-dd")
         .when(F.col("FechaVenta").rlike(r"^\d{2}/\d{2}/\d{4}$"), "dd/MM/yyyy")
         .when(F.col("FechaVenta").rlike(r"^\d{2}-[A-Za-z]{3}-\d{4}$"), "dd-MMM-yyyy")
         .otherwise("otro").alias("formato_fecha")
    ).groupBy("formato_fecha").count()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Limpieza de texto: `trim` y vacíos → `NULL`
# Todas las columnas se pasan a texto primero; así el notebook funciona igual si el pipeline guardó el parquet con tipos numéricos o como texto.

# CELL ********************

df = df_bronze
for c in df.columns:
    df = df.withColumn(c, F.trim(F.col(c).cast("string")))
    df = df.withColumn(c, F.when(F.col(c) == "", F.lit(None)).otherwise(F.col(c)))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Estandarización de ciudades, códigos y fechas

# CELL ********************

# --- Ciudades: clave normalizada (minúsculas y sin tildes) -> nombre oficial
ciudad_clave = F.lower(F.translate(F.col("Ciudad"), "áéíóúÁÉÍÓÚ", "aeiouAEIOU"))
df = df.withColumn(
    "Ciudad",
    F.when(ciudad_clave == "bogota",   F.lit("Bogotá"))
     .when(ciudad_clave == "medellin", F.lit("Medellín"))
     .when(ciudad_clave == "cali",     F.lit("Cali"))
     .otherwise(F.initcap(F.col("Ciudad")))
)

# --- Códigos en mayúscula
for c in ["Genero", "EstadoFactura", "CanalVenta", "IdCliente", "IdCiudad", "IdProducto"]:
    df = df.withColumn(c, F.upper(F.col(c)))

# --- Fecha: la fuente mezcla 3 formatos -> tipo DATE
fv = F.col("FechaVenta")
df = df.withColumn(
    "FechaVenta_dt",
    F.when(fv.rlike(r"^\d{4}-\d{2}-\d{2}"),        F.to_date(F.substring(fv, 1, 10), "yyyy-MM-dd"))
     .when(fv.rlike(r"^\d{2}/\d{2}/\d{4}$"),       F.to_date(fv, "dd/MM/yyyy"))
     .when(fv.rlike(r"^\d{2}-[A-Za-z]{3}-\d{4}$"),  F.to_date(fv, "dd-MMM-yyyy"))
)

# --- Descripciones
df = (df
    .withColumn("GeneroDescripcion",
        F.when(F.col("Genero") == "F", "Femenino")
         .when(F.col("Genero") == "M", "Masculino"))
    .withColumn("EstadoFacturaDescripcion",
        F.when(F.col("EstadoFactura") == "C", "Cancelado")
         .when(F.col("EstadoFactura") == "R", "Rechazado")
         .when(F.col("EstadoFactura") == "A", "Anulado"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Tratamiento de nulos y validación numérica
# * **Cliente nulo:** cada `IdCliente` tiene un único nombre, así que el nombre faltante se completa con el de su mismo `IdCliente` (`ClienteImputado = TRUE`).
# * **Cantidad:** debe ser numérica, entera y mayor que 0. Si es nula o inválida el registro va a cuarentena (`Silver.ventas_rechazados`) — no se inventa un valor porque afectaría `ValorVenta`.

# CELL ********************

# Imputación de Cliente desde el propio catálogo de IdCliente
w_cli = Window.partitionBy("IdCliente")
df = (df
    .withColumn("ClienteImputado", F.col("Cliente").isNull())
    .withColumn("Cliente", F.coalesce(F.col("Cliente"), F.max("Cliente").over(w_cli)))
)

# Validación numérica (cast a double devuelve NULL si el texto no es numérico)
df = (df
    .withColumn("Cantidad_num", F.col("Cantidad").cast("double"))
    .withColumn("Precio_num",   F.col("PrecioUnitario").cast("double"))
    .withColumn("IdVenta_num",  F.col("IdVenta").cast("double"))
)

# Motivo de rechazo (vacío = registro válido)
df = df.withColumn("MotivoRechazo", F.concat_ws("; ",
    F.when(F.col("IdVenta_num").isNull(), F.lit("IdVenta inválido")),
    F.when(F.col("FechaVenta_dt").isNull(), F.lit("Fecha inválida o nula")),
    F.when(F.col("Cantidad").isNull(), F.lit("Cantidad nula")),
    F.when(F.col("Cantidad").isNotNull() & F.col("Cantidad_num").isNull(), F.lit("Cantidad no numérica")),
    F.when(F.col("Cantidad_num").isNotNull() &
           ((F.col("Cantidad_num") <= 0) | (F.col("Cantidad_num") != F.floor("Cantidad_num"))),
           F.lit("Cantidad no entera o <= 0")),
    F.when(F.col("Precio_num").isNull() | (F.col("Precio_num") <= 0), F.lit("PrecioUnitario inválido")),
    F.when(F.col("Cliente").isNull(), F.lit("Cliente nulo sin catálogo")),
    F.when(F.col("GeneroDescripcion").isNull(), F.lit("Genero no reconocido")),
    F.when(F.col("EstadoFacturaDescripcion").isNull(), F.lit("EstadoFactura no reconocido")),
    F.when(F.col("IdProducto").isNull() | F.col("IdCiudad").isNull() | F.col("IdCliente").isNull(),
           F.lit("Llave nula")),
))

df_validos    = df.filter(F.col("MotivoRechazo") == "")
df_rechazados = df.filter(F.col("MotivoRechazo") != "")
print("Válidos:", df_validos.count(), " | Rechazados:", df_rechazados.count())
display(df_rechazados.groupBy("MotivoRechazo").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Tipos finales y columna calculada `ValorVenta`

# CELL ********************

df_silver = (df_validos
    .withColumn("IdVenta",        F.col("IdVenta_num").cast("int"))
    .withColumn("FechaVenta",     F.col("FechaVenta_dt"))
    .withColumn("Cantidad",       F.col("Cantidad_num").cast("int"))
    .withColumn("PrecioUnitario", F.col("Precio_num").cast("decimal(18,2)"))
    .withColumn("ValorVenta",     (F.col("Cantidad") * F.col("PrecioUnitario")).cast("decimal(18,2)"))
    .withColumn("FechaCarga",     F.current_timestamp())
    .select("IdVenta", "FechaVenta", "IdCliente", "Cliente", "Genero", "GeneroDescripcion",
            "IdCiudad", "Ciudad", "EstadoFactura", "EstadoFacturaDescripcion", "CanalVenta",
            "IdProducto", "Producto", "Cantidad", "PrecioUnitario", "ValorVenta",
            "ClienteImputado", "FechaCarga")
)

df_cuarentena = (df_rechazados
    .withColumn("FechaCarga", F.current_timestamp())
    .select("IdVenta", "FechaVenta", "IdCliente", "Cliente", "Genero", "IdCiudad", "Ciudad",
            "EstadoFactura", "CanalVenta", "IdProducto", "Producto", "Cantidad", "PrecioUnitario",
            "MotivoRechazo", "FechaCarga")
)
display(df_silver.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Carga en las tablas Silver (sobrescritura completa)
# Se usa `INSERT OVERWRITE` sobre las tablas creadas en `Silver_ventas_DDL`, de modo que cada ejecución deja la capa Silver igual al último archivo de Bronze.

# CELL ********************

df_silver.createOrReplaceTempView("vw_silver_ventas")
df_cuarentena.createOrReplaceTempView("vw_silver_rechazados")

spark.sql("""
INSERT OVERWRITE TABLE Silver.ventas
SELECT IdVenta, FechaVenta, IdCliente, Cliente, Genero, GeneroDescripcion, IdCiudad, Ciudad,
       EstadoFactura, EstadoFacturaDescripcion, CanalVenta, IdProducto, Producto, Cantidad,
       PrecioUnitario, ValorVenta, ClienteImputado, FechaCarga
FROM vw_silver_ventas
""")

spark.sql("""
INSERT OVERWRITE TABLE Silver.ventas_rechazados
SELECT IdVenta, FechaVenta, IdCliente, Cliente, Genero, IdCiudad, Ciudad, EstadoFactura, CanalVenta,
       IdProducto, Producto, Cantidad, PrecioUnitario, MotivoRechazo, FechaCarga
FROM vw_silver_rechazados
""")
print("Carga Silver terminada")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7. Validación de la capa Silver

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT 'Silver.ventas' AS tabla, COUNT(*) AS filas FROM Silver.ventas
# MAGIC UNION ALL
# MAGIC SELECT 'Silver.ventas_rechazados', COUNT(*) FROM Silver.ventas_rechazados

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- Debe mostrar solo: Bogotá, Cali, Medellín
# MAGIC SELECT Ciudad, COUNT(*) AS filas FROM Silver.ventas GROUP BY Ciudad ORDER BY Ciudad

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT GeneroDescripcion, EstadoFacturaDescripcion, CanalVenta, COUNT(*) AS filas
# MAGIC FROM Silver.ventas
# MAGIC GROUP BY GeneroDescripcion, EstadoFacturaDescripcion, CanalVenta
# MAGIC ORDER BY 1, 2, 3

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT MIN(FechaVenta) AS fecha_min, MAX(FechaVenta) AS fecha_max,
# MAGIC        SUM(CASE WHEN ClienteImputado THEN 1 ELSE 0 END) AS clientes_imputados,
# MAGIC        SUM(ValorVenta) AS valor_total
# MAGIC FROM Silver.ventas

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
