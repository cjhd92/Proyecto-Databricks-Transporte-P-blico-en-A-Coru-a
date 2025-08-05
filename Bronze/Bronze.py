# Databricks notebook source

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType


# COMMAND ----------

base_path = "/FileStore/rawdata"
bronze_path = f"{base_path}/bronze"


# ---- 1. Esquema para viajes ----
viajes_schema = StructType([
    StructField("id_viaje", IntegerType(), True),
    StructField("linea", StringType(), True),
    StructField("parada_subida", StringType(), True),
    StructField("parada_bajada", StringType(), True),
    StructField("hora_subida", StringType(), True),
    StructField("hora_bajada", StringType(), True),
    StructField("duracion_min", IntegerType(), True),
    StructField("transbordo", StringType(), True)
])

df_viajes = spark.read.schema(viajes_schema) \
    .option('header',True) \
    .csv("dbfs:/FileStore/rawdata/Viajes_de_Transporte_P_blico_en_A_Coru_a.csv")
  
# Guardar en Delta
df_viajes.write.format("delta").mode("overwrite").save(f"{bronze_path}/viajes")



# COMMAND ----------

base_path = "/FileStore/rawdata"
bronze_path = f"{base_path}/bronze"

# ---- 2. Esquema para usuarios ----
usuarios_schema = StructType([
    StructField("id_usuario", IntegerType(), True),
    StructField("edad", IntegerType(), True),
    StructField("genero", StringType(), True),
    StructField("tipo_abono", StringType(), True)
])

df_usuarios = spark.read.schema(usuarios_schema) \
    .option("heaer",True) \
    .csv("dbfs:/FileStore/rawdata/Usuarios_de_Transporte_P_blico_en_A_Coru_a.csv")

# Guardar en Delta

df_usuarios.write.format("delta").mode("overwrite").save(f"{bronze_path}/usuarios")

# COMMAND ----------

# Confirmar
print("Datos cargados en la capa Bronze:")
print(f"Viajes: {df_viajes.count()} registros")
print(f"Usuarios: {df_usuarios.count()} registros")
