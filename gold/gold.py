# Databricks notebook source
# MAGIC %md
# MAGIC #Capa Gold con métricas listas o con modelo dimension
# MAGIC
# MAGIC     Implementa el modelo dimensional Gold (hechos + dimensiones).
# MAGIC
# MAGIC     Crea unas pocas vistas con métricas clave (ej. viajes por línea, ranking de usuarios, top paradas).

# COMMAND ----------

from pyspark.sql import functions as F

# Rutas base
base_path = "/FileStore/rawdata"
bronze_path = f"{base_path}/bronze"
silver_path = f"{base_path}/silver"
gold_path = f"{base_path}/gold"

# COMMAND ----------

# ---- 1. Leer datos de Bronze ----
df_silver = spark.read.format("delta").load(f"{silver_path}/viajes_usuarios")

# ---- Verificando que los datos se leen correctamente de la capa de Bronze
print(f"Registro df_viajes: {df_silver.count()}")


df_silver.display()

# COMMAND ----------

# Crear Dim_Usuarios
dim_usuarios = df_silver.select("id_usuario", "edad", "genero", "tipo_abono").dropDuplicates()
dim_usuarios.write.format("delta").mode("overwrite").save(f"{gold_path}/dim_usuarios")

# Crear Dim_Lineas
dim_lineas = df_silver.select("linea").dropDuplicates() \
                      .withColumnRenamed("linea", "nombre_linea") \
                      .withColumn("id_linea", F.monotonically_increasing_id())
dim_lineas.write.format("delta").mode("overwrite").save(f"{gold_path}/dim_lineas")

# Crear Dim_Paradas
dim_paradas = df_silver.select("parada_subida").dropDuplicates() \
                       .withColumnRenamed("parada_subida", "nombre_parada") \
                       .withColumn("id_parada", F.monotonically_increasing_id())
dim_paradas.write.format("delta").mode("overwrite").save(f"{gold_path}/dim_paradas")

# Crear Dim_Tiempo
dim_tiempo = df_silver.select(
    F.to_date("hora_subida").alias("fecha"),
    F.hour("hora_subida").alias("hora"),
    F.dayofweek("hora_subida").alias("dia_semana"),
    F.month("hora_subida").alias("mes")
).dropDuplicates().withColumn("id_fecha", F.monotonically_increasing_id())
dim_tiempo.write.format("delta").mode("overwrite").save(f"{gold_path}/dim_tiempo")

# Crear Hecho_Viajes
hecho_viajes = df_silver.select(
    "id_viaje",
    "id_usuario",
    "linea",
    "parada_subida",
    "parada_bajada",
    "hora_subida",
    "hora_bajada",
    "duracion_calculada",
    "transbordo_bin"
)
hecho_viajes.write.format("delta").mode("overwrite").save(f"{gold_path}/hecho_viajes")


# COMMAND ----------

# Registrar las tablas Gold como vistas temporales
hecho_viajes = spark.read.format("delta").load(f"{gold_path}/hecho_viajes")
dim_usuarios = spark.read.format("delta").load(f"{gold_path}/dim_usuarios")
dim_lineas = spark.read.format("delta").load(f"{gold_path}/dim_lineas")
dim_paradas = spark.read.format("delta").load(f"{gold_path}/dim_paradas")
dim_tiempo = spark.read.format("delta").load(f"{gold_path}/dim_tiempo")

hecho_viajes.createOrReplaceTempView("Hecho_Viajes")
dim_usuarios.createOrReplaceTempView("Dim_Usuarios")
dim_lineas.createOrReplaceTempView("Dim_Lineas")
dim_paradas.createOrReplaceTempView("Dim_Paradas")
dim_tiempo.createOrReplaceTempView("Dim_Tiempo")

hecho_viajes.printSchema()
dim_lineas.printSchema()


# COMMAND ----------

# ---- 1. Viajes por línea ----
spark.sql("""
CREATE OR REPLACE TEMP VIEW vw_total_viajes_por_linea AS
SELECT 
    l.nombre_linea,
    COUNT(*) AS total_viajes,
    AVG(v.duracion_calculada) AS duracion_media
FROM hecho_viajes v
JOIN dim_lineas l 
    ON v.linea = l.id_linea
GROUP BY l.nombre_linea
""")

df_visualizar = spark.sql("SELECT * FROM vw_total_viajes_por_linea")
display(df_visualizar)