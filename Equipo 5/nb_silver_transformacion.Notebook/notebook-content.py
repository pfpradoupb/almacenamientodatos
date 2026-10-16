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

# Welcome to your new notebook
# Type here in the cell editor to add code!

df = spark.read.parquet("Files/Parquet/Ventas.xlsx.parquet")
# df now is a Spark DataFrame containing parquet data from "Files/Parquet/Ventas.xlsx.parquet".
display(df)

from pyspark.sql import functions as F, Window
from pyspark.sql.types import StringType

# df = spark.read.parquet("Files/ventas.parquet")   # tu carga original

# ---------------------------------------------------------------
# 1. Eliminar espacios al inicio/final en TODAS las columnas de texto
# ---------------------------------------------------------------
for c in [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]:
    df = df.withColumn(c, F.trim(F.col(c)))

# ---------------------------------------------------------------
# 2. Estandarizar ciudades (una sola forma por IdCiudad, con tilde)
#    Esto resuelve BOGOTA / BOGOTÁ / bogota / Bogotá, etc.
# ---------------------------------------------------------------
df = df.withColumn(
    "Ciudad",
    F.when(F.col("IdCiudad") == "CIU001", "Bogotá")
     .when(F.col("IdCiudad") == "CIU002", "Medellín")
     .when(F.col("IdCiudad") == "CIU003", "Cali")
     .otherwise(F.initcap(F.col("Ciudad")))
)

# Estandarizar CanalVenta (venía WEB / web, TIENDA / tienda...)
df = df.withColumn("CanalVenta", F.initcap(F.col("CanalVenta")))

# ---------------------------------------------------------------
# 3. Convertir FechaVenta a Date (3 formatos mezclados)
#    Se elige el formato según el patrón de cada fila (no falla en modo ANSI)
# ---------------------------------------------------------------
f = F.col("FechaVenta")
df = df.withColumn(
    "FechaVenta",
    F.when(f.rlike(r"^\d{4}-\d{2}-\d{2}$"),        F.to_date(f, "yyyy-MM-dd"))
     .when(f.rlike(r"^\d{2}/\d{2}/\d{4}$"),        F.to_date(f, "dd/MM/yyyy"))
     .when(f.rlike(r"^\d{2}-[A-Za-z]{3}-\d{4}$"),  F.to_date(f, "dd-MMM-yyyy"))
     .otherwise(F.lit(None).cast("date"))
)

# ---------------------------------------------------------------
# 4. Nulos
# ---------------------------------------------------------------
# 4a. Cliente: se recupera desde otras filas del mismo IdCliente
w = Window.partitionBy("IdCliente")
df = df.withColumn(
    "Cliente",
    F.coalesce(F.col("Cliente"), F.first("Cliente", ignorenulls=True).over(w))
)
# Si algún cliente nunca tuvo nombre, queda marcado
df = df.fillna({"Cliente": "Sin Información"})

# ---------------------------------------------------------------
# 5. Validar que Cantidad sea numérica (try_cast: lo no numérico -> null)
# ---------------------------------------------------------------
df = df.withColumn("Cantidad", F.expr("try_cast(Cantidad as double)"))
df = df.withColumn("PrecioUnitario", F.expr("try_cast(PrecioUnitario as double)"))

# 4b. Cantidad nula: no se puede inferir -> se marca y se separa para revisión
df = df.withColumn(
    "RegistroInvalido",
    F.col("Cantidad").isNull() | F.col("PrecioUnitario").isNull() | F.col("FechaVenta").isNull()
)

# ---------------------------------------------------------------
# 6. Valores descriptivos (se normaliza a mayúscula primero: m, f, a, r, c)
# ---------------------------------------------------------------
df = df.withColumn(
    "Genero",
    F.when(F.upper(F.col("Genero")) == "F", "Femenino")
     .when(F.upper(F.col("Genero")) == "M", "Masculino")
     .otherwise("Desconocido")
)

df = df.withColumn(
    "EstadoFactura",
    F.when(F.upper(F.col("EstadoFactura")) == "C", "Cancelado")
     .when(F.upper(F.col("EstadoFactura")) == "R", "Rechazado")
     .when(F.upper(F.col("EstadoFactura")) == "A", "Anulado")
     .otherwise("Desconocido")
)

# ---------------------------------------------------------------
# 7. ValorVenta = Cantidad × PrecioUnitario
# ---------------------------------------------------------------
df = df.withColumn("ValorVenta", F.col("Cantidad") * F.col("PrecioUnitario"))

# ---------------------------------------------------------------
# Resultado: datos limpios vs. registros con problemas
# ---------------------------------------------------------------
df_invalidos = df.filter(F.col("RegistroInvalido"))
df_limpio    = df.filter(~F.col("RegistroInvalido")).drop("RegistroInvalido")

print(f"Total: {df.count()} | Válidos: {df_limpio.count()} | Inválidos (nulos): {df_invalidos.count()}")

from pyspark.sql import functions as F

columnas = ["IdVenta","FechaVenta","IdCliente","Cliente","Genero","IdCiudad","Ciudad",
            "EstadoFactura","CanalVenta","IdProducto","Producto",
            "Cantidad","PrecioUnitario","ValorVenta"]

def a_silver(df):
    # Mismo orden y tipos que el CREATE TABLE
    return df.select(
        F.col("IdVenta").cast("bigint"), F.col("FechaVenta").cast("date"),
        "IdCliente", "Cliente", "Genero", "IdCiudad", "Ciudad", "EstadoFactura",
        "CanalVenta", "IdProducto", "Producto",
        F.col("Cantidad").cast("double"), F.col("PrecioUnitario").cast("double"),
        F.col("ValorVenta").cast("double"))

a_silver(df_limpio).write.mode("overwrite").insertInto("silver.ventas", overwrite=True)
a_silver(df_invalidos).write.mode("overwrite").insertInto("silver.ventas_rechazadas", overwrite=True)

print("silver.ventas:", spark.table("silver.ventas").count())
print("silver.ventas_rechazadas:", spark.table("silver.ventas_rechazadas").count())
display(spark.table("silver.ventas").limit(20))



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
