# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "fa7096ed-6fb9-4441-9e9d-3799d6810b8f",
# META       "default_lakehouse_name": "LS_UPB",
# META       "default_lakehouse_workspace_id": "0293bd32-c12e-403e-9acb-f6f442c66de3",
# META       "known_lakehouses": [
# META         {
# META           "id": "fa7096ed-6fb9-4441-9e9d-3799d6810b8f"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

df = spark.read.parquet("abfss://Clase@onelake.dfs.fabric.microsoft.com/LS_UPB.Lakehouse/Files/Parquet/ventas.parquet")
# df now is a Spark DataFrame containing parquet data from "abfss://Clase@onelake.dfs.fabric.microsoft.com/LS_UPB.Lakehouse/Files/Parquet/ventas.parquet".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
