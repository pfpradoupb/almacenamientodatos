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
# META     },
# META     "warehouse": {
# META       "default_warehouse": "23b6bfe4-c29a-435e-a02b-cb0668f2ba71",
# META       "known_warehouses": [
# META         {
# META           "id": "23b6bfe4-c29a-435e-a02b-cb0668f2ba71",
# META           "type": "Lakewarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F, Window

s = spark.table("silver.ventas")
print("Filas en silver.ventas:", s.count())

def con_key(df, key_name, order_col):
    return df.withColumn(key_name, F.row_number().over(Window.orderBy(order_col)).cast("int"))

def cargar(df, tabla):
    df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable(tabla)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_fecha = (s.select("FechaVenta").distinct()
    .withColumnRenamed("FechaVenta", "Fecha")
    .withColumn("FechaKey", F.date_format("Fecha", "yyyyMMdd").cast("int"))
    .withColumn("Anio", F.year("Fecha"))
    .withColumn("Trimestre", F.quarter("Fecha"))
    .withColumn("Mes", F.month("Fecha"))
    .withColumn("NombreMes", F.date_format("Fecha", "MMMM"))
    .withColumn("Dia", F.dayofmonth("Fecha"))
    .withColumn("DiaSemana", F.date_format("Fecha", "EEEE"))
    .select("FechaKey","Fecha","Anio","Trimestre","Mes","NombreMes","Dia","DiaSemana"))
cargar(dim_fecha, "gold.dim_fecha")
print("gold.dim_fecha:", dim_fecha.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_cliente = con_key(s.select("IdCliente","Cliente","Genero").distinct(), "ClienteKey", "IdCliente") \
    .select("ClienteKey","IdCliente","Cliente","Genero")
cargar(dim_cliente, "gold.dim_cliente")
print("gold.dim_cliente:", dim_cliente.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_ciudad = con_key(s.select("IdCiudad","Ciudad").distinct(), "CiudadKey", "IdCiudad") \
    .select("CiudadKey","IdCiudad","Ciudad")
cargar(dim_ciudad, "gold.dim_ciudad")
print("gold.dim_ciudad:", dim_ciudad.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_producto = con_key(s.select("IdProducto","Producto").distinct(), "ProductoKey", "IdProducto") \
    .select("ProductoKey","IdProducto","Producto")
cargar(dim_producto, "gold.dim_producto")
print("gold.dim_producto:", dim_producto.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_canal = con_key(s.select("CanalVenta").distinct(), "CanalKey", "CanalVenta") \
    .select("CanalKey","CanalVenta")
cargar(dim_canal, "gold.dim_canal")
print("gold.dim_canal:", dim_canal.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_estado_factura = con_key(s.select("EstadoFactura").distinct(), "EstadoKey", "EstadoFactura") \
    .select("EstadoKey","EstadoFactura")
cargar(dim_estado_factura, "gold.dim_estado_factura")
print("gold.dim_estado_factura:", dim_estado_factura.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

d_cli  = spark.table("gold.dim_cliente").select("ClienteKey","IdCliente")
d_ciu  = spark.table("gold.dim_ciudad").select("CiudadKey","IdCiudad")
d_prod = spark.table("gold.dim_producto").select("ProductoKey","IdProducto")
d_can  = spark.table("gold.dim_canal").select("CanalKey","CanalVenta")
d_est  = spark.table("gold.dim_estado_factura").select("EstadoKey","EstadoFactura")

fact = (s
    .withColumn("FechaKey", F.date_format("FechaVenta", "yyyyMMdd").cast("int"))
    .join(d_cli,  "IdCliente")
    .join(d_ciu,  "IdCiudad")
    .join(d_prod, "IdProducto")
    .join(d_can,  "CanalVenta")
    .join(d_est,  "EstadoFactura")
    .select(F.col("IdVenta").cast("int"), "FechaKey", "ClienteKey", "CiudadKey",
            "ProductoKey", "CanalKey", "EstadoKey",
            "Cantidad", "PrecioUnitario", "ValorVenta"))

fact.write.mode("overwrite").format("delta").saveAsTable("gold.fact_ventas")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Silver:", s.count(), "| Fact:", spark.table("gold.fact_ventas").count())
display(spark.sql("""
    SELECT c.Ciudad, f.Anio, SUM(v.ValorVenta) AS TotalVentas
    FROM gold.fact_ventas v
    JOIN gold.dim_ciudad c ON v.CiudadKey = c.CiudadKey
    JOIN gold.dim_fecha  f ON v.FechaKey  = f.FechaKey
    GROUP BY c.Ciudad, f.Anio
    ORDER BY f.Anio, TotalVentas DESC
"""))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
