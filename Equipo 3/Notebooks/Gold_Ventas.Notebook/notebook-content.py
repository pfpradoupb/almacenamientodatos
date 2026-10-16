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


# VERIFICACION DEL ORIGEN GOLD

print("=== VERIFICACION ORIGEN GOLD ===")

# Leer tabla Silver
df_origen = spark.table("Silver.Ventas")

print("Total registros Silver:", df_origen.count())

# Crear esquema Gold si no existe
spark.sql("CREATE DATABASE IF NOT EXISTS Gold")

print("Esquema Gold disponible")

# Mostrar algunos registros
spark.sql("""
SELECT
    IdVenta,
    FechaVenta,
    IdCliente,
    Cliente,
    IdProducto,
    Producto,
    Ciudad,
    CanalVenta,
    Cantidad,
    PrecioUnitario,
    ValorVenta
FROM Silver.Ventas
LIMIT 10
""").show(truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# CREAR DIMENSION CLIENTE
# Modelo dimensional Gold

spark.sql("""
CREATE OR REPLACE TABLE Gold.DimCliente
USING DELTA
AS
SELECT
    IdCliente,
    MAX(Cliente) AS NombreCliente,
    MAX(Genero) AS Genero,
    MAX(GeneroDescripcion) AS GeneroDescripcion
FROM Silver.Ventas
WHERE IdCliente IS NOT NULL
GROUP BY IdCliente
""")

print("=== DIMENSION CLIENTE CREADA ===")

spark.sql("""
SELECT *
FROM Gold.DimCliente
ORDER BY IdCliente
""").show(20, truncate=False)

print("TOTAL CLIENTES:")
spark.sql("""
SELECT COUNT(*) AS TotalClientes
FROM Gold.DimCliente
""").show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# CREAR DIMENSION PRODUCTO
# Modelo dimensional Gold

spark.sql("""
CREATE OR REPLACE TABLE Gold.DimProducto
USING DELTA
AS
SELECT
    IdProducto,
    MAX(Producto) AS NombreProducto
FROM Silver.Ventas
WHERE IdProducto IS NOT NULL
GROUP BY IdProducto
""")

print("=== DIMENSION PRODUCTO CREADA ===")

spark.sql("""
SELECT *
FROM Gold.DimProducto
ORDER BY IdProducto
""").show(20, truncate=False)

print("TOTAL PRODUCTOS:")
spark.sql("""
SELECT COUNT(*) AS TotalProductos
FROM Gold.DimProducto
""").show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# CREAR DIMENSION CIUDAD
# Modelo dimensional Gold - Spark SQL

spark.sql("""
CREATE OR REPLACE TABLE Gold.DimCiudad
USING DELTA
AS
SELECT
    IdCiudad,
    MAX(Ciudad) AS NombreCiudad
FROM Silver.Ventas
WHERE IdCiudad IS NOT NULL
GROUP BY IdCiudad
""")

print("=== DIMENSION CIUDAD CREADA ===")

spark.sql("""
SELECT *
FROM Gold.DimCiudad
ORDER BY IdCiudad
""").show(truncate=False)

print("TOTAL CIUDADES:")
spark.sql("""
SELECT COUNT(*) AS TotalCiudades
FROM Gold.DimCiudad
""").show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# CREAR DIMENSION FECHA
# Modelo dimensional Gold - Spark SQL

spark.sql("""
CREATE OR REPLACE TABLE Gold.DimFecha
USING DELTA
AS
SELECT DISTINCT
    CAST(date_format(FechaVenta, 'yyyyMMdd') AS INT)
        AS IdFecha,
    FechaVenta,
    YEAR(FechaVenta) AS `Año`,
    MONTH(FechaVenta) AS Mes,
    DAY(FechaVenta) AS Dia,
    QUARTER(FechaVenta) AS Trimestre,
    CASE MONTH(FechaVenta)
        WHEN 1 THEN 'Enero'
        WHEN 2 THEN 'Febrero'
        WHEN 3 THEN 'Marzo'
        WHEN 4 THEN 'Abril'
        WHEN 5 THEN 'Mayo'
        WHEN 6 THEN 'Junio'
        WHEN 7 THEN 'Julio'
        WHEN 8 THEN 'Agosto'
        WHEN 9 THEN 'Septiembre'
        WHEN 10 THEN 'Octubre'
        WHEN 11 THEN 'Noviembre'
        WHEN 12 THEN 'Diciembre'
    END AS NombreMes
FROM Silver.Ventas
WHERE FechaVenta IS NOT NULL
""")

print("=== DIMENSION FECHA CREADA ===")

spark.sql("""
SELECT *
FROM Gold.DimFecha
ORDER BY FechaVenta
LIMIT 15
""").show(truncate=False)

print("TOTAL FECHAS:")
spark.sql("""
SELECT COUNT(*) AS TotalFechas
FROM Gold.DimFecha
""").show()

print("FECHAS POR AÑO:")
spark.sql("""
SELECT
    `Año`,
    COUNT(*) AS FechasDistintas
FROM Gold.DimFecha
GROUP BY `Año`
ORDER BY `Año`
""").show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# CREAR DIMENSION CANAL DE VENTA
# Modelo dimensional Gold - Spark SQL

spark.sql("""
CREATE OR REPLACE TABLE Gold.DimCanalVenta
USING DELTA
AS
SELECT
    ROW_NUMBER() OVER (
        ORDER BY CanalVenta
    ) AS IdCanal,
    CanalVenta AS NombreCanal
FROM (
    SELECT DISTINCT CanalVenta
    FROM Silver.Ventas
    WHERE CanalVenta IS NOT NULL
) canales
""")

print("=== DIMENSION CANAL DE VENTA CREADA ===")

spark.sql("""
SELECT *
FROM Gold.DimCanalVenta
ORDER BY IdCanal
""").show(truncate=False)

print("TOTAL CANALES:")
spark.sql("""
SELECT COUNT(*) AS TotalCanales
FROM Gold.DimCanalVenta
""").show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# CREAR DIMENSION ESTADO FACTURA
# Modelo dimensional Gold - Spark SQL

spark.sql("""
CREATE OR REPLACE TABLE Gold.DimEstadoFactura
USING DELTA
AS
SELECT
    EstadoFactura AS IdEstadoFactura,
    MAX(EstadoFacturaDescripcion)
        AS NombreEstadoFactura
FROM Silver.Ventas
WHERE EstadoFactura IS NOT NULL
GROUP BY EstadoFactura
""")

print("=== DIMENSION ESTADO FACTURA CREADA ===")

spark.sql("""
SELECT *
FROM Gold.DimEstadoFactura
ORDER BY IdEstadoFactura
""").show(truncate=False)

print("TOTAL ESTADOS:")
spark.sql("""
SELECT COUNT(*) AS TotalEstados
FROM Gold.DimEstadoFactura
""").show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# CREAR TABLA DE HECHOS FACTVENTAS
# Modelo dimensional Gold - Spark SQL

spark.sql("""
CREATE OR REPLACE TABLE Gold.FactVentas
USING DELTA
AS
SELECT
    ROW_NUMBER() OVER (
        ORDER BY
            V.IdVenta,
            V.IdProducto,
            V.IdCliente,
            V.FechaVenta
    ) AS IdDetalleVenta,

    V.IdVenta,

    CAST(
        date_format(V.FechaVenta, 'yyyyMMdd')
        AS INT
    ) AS IdFecha,

    V.IdCliente,
    V.IdProducto,
    V.IdCiudad,

    C.IdCanal,

    V.EstadoFactura AS IdEstadoFactura,

    V.Cantidad,
    V.PrecioUnitario,
    V.ValorVenta,

    CASE
        WHEN V.Cantidad IS NULL
          OR V.ValorVenta IS NULL
        THEN 'Incompleto'
        ELSE 'Completo'
    END AS CalidadRegistro

FROM Silver.Ventas V

LEFT JOIN Gold.DimCanalVenta C
    ON V.CanalVenta = C.NombreCanal
""")

print("=== TABLA FACTVENTAS CREADA ===")

spark.sql("""
SELECT *
FROM Gold.FactVentas
ORDER BY IdDetalleVenta
LIMIT 15
""").show(truncate=False)

print("=== TOTAL REGISTROS FACTVENTAS ===")

spark.sql("""
SELECT COUNT(*) AS TotalRegistros
FROM Gold.FactVentas
""").show()

print("=== CALIDAD DE REGISTROS ===")

spark.sql("""
SELECT
    CalidadRegistro,
    COUNT(*) AS Registros
FROM Gold.FactVentas
GROUP BY CalidadRegistro
ORDER BY CalidadRegistro
""").show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# VALIDACION FINAL DEL MODELO GOLD
# Integridad, unicidad y calidad de datos

print("=== 1. COMPARACION SILVER VS GOLD ===")

spark.sql("""
SELECT
    (SELECT COUNT(*) FROM Silver.Ventas)
        AS RegistrosSilver,
    (SELECT COUNT(*) FROM Gold.FactVentas)
        AS RegistrosGold
""").show()


print("=== 2. VALIDACION CLAVE PRINCIPAL ===")

spark.sql("""
SELECT
    COUNT(*) AS TotalFilas,
    COUNT(DISTINCT IdDetalleVenta) AS ClavesUnicas,
    COUNT(*) - COUNT(DISTINCT IdDetalleVenta)
        AS Duplicados
FROM Gold.FactVentas
""").show()


print("=== 3. INTEGRIDAD REFERENCIAL ===")

spark.sql("""
SELECT
    COUNT(*) AS TotalFilas,
    SUM(CASE WHEN C.IdCliente IS NULL
        THEN 1 ELSE 0 END) AS ErrorCliente,
    SUM(CASE WHEN P.IdProducto IS NULL
        THEN 1 ELSE 0 END) AS ErrorProducto,
    SUM(CASE WHEN CI.IdCiudad IS NULL
        THEN 1 ELSE 0 END) AS ErrorCiudad,
    SUM(CASE WHEN F.IdFecha IS NULL
        THEN 1 ELSE 0 END) AS ErrorFecha,
    SUM(CASE WHEN CA.IdCanal IS NULL
        THEN 1 ELSE 0 END) AS ErrorCanal,
    SUM(CASE WHEN E.IdEstadoFactura IS NULL
        THEN 1 ELSE 0 END) AS ErrorEstado
FROM Gold.FactVentas V
LEFT JOIN Gold.DimCliente C
    ON V.IdCliente = C.IdCliente
LEFT JOIN Gold.DimProducto P
    ON V.IdProducto = P.IdProducto
LEFT JOIN Gold.DimCiudad CI
    ON V.IdCiudad = CI.IdCiudad
LEFT JOIN Gold.DimFecha F
    ON V.IdFecha = F.IdFecha
LEFT JOIN Gold.DimCanalVenta CA
    ON V.IdCanal = CA.IdCanal
LEFT JOIN Gold.DimEstadoFactura E
    ON V.IdEstadoFactura = E.IdEstadoFactura
""").show()


print("=== 4. CALIDAD DE LOS REGISTROS ===")

spark.sql("""
SELECT
    CalidadRegistro,
    COUNT(*) AS TotalRegistros
FROM Gold.FactVentas
GROUP BY CalidadRegistro
ORDER BY CalidadRegistro
""").show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
