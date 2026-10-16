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

# # Bronze_ventas_alternativo · Ingesta alternativa (respaldo del pipeline)
# 
# **Úsalo solo si la actividad *Copy data* del pipeline te da problemas con el Excel.**
# El entregable oficial de Bronze es el pipeline `PL_Bronze_ventas`; este notebook hace exactamente lo mismo con Python.
# 
# * **Lakehouse:** el lakehouse único del equipo (p. ej. `Lakehouse_Total`), como predeterminado.
# * Lee `Files/Excel/Ventas.xlsx` y escribe `Files/Parquet/Ventas.parquet` (ajusta las rutas si subiste el Excel a otra carpeta)
# * Todas las columnas se leen como texto (`dtype=str`) para **conservar la estructura original** sin transformar nada.

# CELL ********************

import pandas as pd
import os

RUTA_EXCEL   = "/lakehouse/default/Files/Excel/Ventas.xlsx"
RUTA_BRONZE  = "/lakehouse/default/Files/Parquet/Ventas.parquet"
HOJA         = "Ventas_Deportes"

df_raw = pd.read_excel(RUTA_EXCEL, sheet_name=HOJA, dtype=str)   # sin transformaciones
print(df_raw.shape)
df_raw.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

os.makedirs(os.path.dirname(RUTA_BRONZE), exist_ok=True)
df_raw.to_parquet(RUTA_BRONZE, index=False)

# Verificación: leer el parquet con Spark
df_check = spark.read.parquet("Files/Parquet/Ventas.parquet")
print("Filas en Bronze:", df_check.count())
df_check.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
