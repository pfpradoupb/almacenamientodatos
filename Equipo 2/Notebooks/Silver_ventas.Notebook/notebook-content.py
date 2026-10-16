# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e44eee3b-bc2e-4a39-bba7-95e9df346ff1",
# META       "default_lakehouse_name": "Lakehouse_Total",
# META       "default_lakehouse_workspace_id": "0d6975e1-78ba-484d-85c5-68df20329d80",
# META       "known_lakehouses": [
# META         {
# META           "id": "e44eee3b-bc2e-4a39-bba7-95e9df346ff1"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F 

# Welcome to your new notebook
# Type here in the cell editor to add code!

df = spark.read.parquet("Files/Parquet/Ventas.parquet")
# df now is a Spark DataFrame containing parquet data from "Files/Parquet/Ventas.parquet".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Cambiar tipo de datos

# CELL ********************

df=df.withColumn("IdVenta",F.col("IdVenta").cast("int"))
df=df.withColumn("Cantidad",F.col("Cantidad").cast("int"))
df=df.withColumn("PrecioUnitario",F.col("PrecioUnitario").cast("float"))
formats=["yyyy-MM-dd","dd-MM-yyyy","dd-MMM-yyyy","dd/MM/yyyy"]
expresiones_fecha = [F.to_date(F.col("FechaVenta"), fmt) for fmt in formats]
df=df.withColumn("FechaVenta",F.coalesce(*expresiones_fecha))
df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Vamos a pasar a Silver todas las columnas

# MARKDOWN ********************

# ## Eliminar espacios en blanco al inicio y final de los textos

# MARKDOWN ********************

# ### Identifico las columnas string

# CELL ********************

string_cols=[f.name for f in df.schema.fields if f.dataType.simpleString()== "string"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def limpiar_espacios(columna): 
    columna_limpia=F.trim(columna)
    return columna_limpia

for c in string_cols:
    df=df.withColumn(c,limpiar_espacios(F.col(c)))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Estandarizar nombres de ciudades

# CELL ********************

df=df.withColumn("Ciudad",
F.when(F.lower(F.col("Ciudad")).isin("medellin","medellín"),F.lit("Medellín"))
 .when(F.lower(F.col("Ciudad")).isin("cali"),F.lit("Cali"))
 .when(F.lower(F.col("Ciudad")).isin("bogota","bogotá"),F.lit("Bogotá"))
 .otherwise(F.initcap(F.col("Ciudad")))
)

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Identificar registros con valores nulos

# CELL ********************

columnas = ""
cond = [c for c in df.columns if c not in ["Cliente", "Ciudad", "Producto"]]

for c in cond :
    if columnas == "":
        columnas = f"{c} is not null " 
    else:
        columnas = columnas + f" and {c} is not null"

df = df.filter(columnas)
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df=df.withColumn("Genero",
F.when(F.lower(F.col("Genero")).isin("f"),F.lit("Femenino"))
 .when(F.lower(F.col("Genero")).isin("m",),F.lit("Masculino"))
 .otherwise(F.col("Genero"))
)

df=df.withColumn("EstadoFactura",
F.when(F.lower(F.col("EstadoFactura")).isin("c"),F.lit("Cancelado"))
 .when(F.lower(F.col("EstadoFactura")).isin("r"),F.lit("Rechazado"))
 .when(F.lower(F.col("EstadoFactura")).isin("a"),F.lit("Anulado"))
 .otherwise(F.col("EstadoFactura"))
)

df = df.withColumn("CanalVenta", F.upper(F.col("CanalVenta")))
df = df.withColumn("ValorVenta", F.col("Cantidad") * F.col("PrecioUnitario"))
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver.ventas")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
