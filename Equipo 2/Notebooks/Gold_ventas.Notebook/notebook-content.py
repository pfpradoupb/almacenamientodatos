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

table_name = "silver.ventas"
df = spark.read.table(table_name)

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

# CELL ********************

# Dimensiones: cliente, ciudad, producto

dim_cliente = df.select(
    F.col("IdCliente").alias("id_cliente"), 
    F.col("Cliente").alias("nombre"), 
    F.col("Genero").alias("genero")
).distinct().filter("nombre is not null")

dim_cliente.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold.dim_cliente")

dim_ciudad = df.select(
    F.col("IdCiudad").alias("id_ciudad"), 
    F.col("Ciudad").alias("nombre"), 
).distinct()

dim_ciudad.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold.dim_ciudad")

dim_producto = df.select(
    F.col("IdProducto").alias("id_producto"), 
    F.col("Producto").alias("nombre")
).distinct()

dim_producto.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold.dim_producto")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Hecho: venta

hec_venta = df.select(
    F.col("IdVenta").alias("id_venta"),
    F.col("FechaVenta").alias("fecha"),
    F.col("IdCliente").alias("id_cliente"),
    F.col("IdCiudad").alias("id_ciudad"),
    F.col("IdProducto").alias("id_producto"),
    F.col("EstadoFactura").alias("estado_factura"),
    F.col("CanalVenta").alias("canal_venta"),
    F.col("Cantidad").alias("cantidad"),
    F.col("PrecioUnitario").alias("precio_unitario"),
    F.col("ValorVenta").alias("valor_venta")
)

hec_venta.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold.hec_venta")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
