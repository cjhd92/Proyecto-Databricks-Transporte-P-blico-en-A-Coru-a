# Databricks notebook source
# MAGIC %md
# MAGIC #Capa Silver
# MAGIC - Conversión de horas a formato timestamp.
# MAGIC - Join con los usuarios.
# MAGIC - Limpieza de transbordos (Sí/No → 1/0).

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
import random

# Rutas base
base_path = "/FileStore/rawdata"
bronze_path = f"{base_path}/bronze"
silver_path = f"{base_path}/silver"


# COMMAND ----------

# ---- 1. Leer datos de Bronze ----
df_viajes = spark.read.format("delta").load(f"{bronze_path}/viajes")
df_usuarios = spark.read.format("delta").load(f"{bronze_path}/usuarios")

# ---- Verificando que los datos se leen correctamente de la capa de Bronze
print(f"Registro df_viajes: {df_viajes.count()}")
print(f"Registro df_usuarios: {df_usuarios.count()}")

df_viajes.show()

# COMMAND ----------

# ---- 2. Convertir horas a Timestamp ----

df_viajes  = df_viajes.withColumn("hora_subida",F.to_timestamp("hora_subida","yyyy-MM-dd HH:mm")) \
                      .withColumn("hora_bajada",F.to_timestamp("hora_subida","yyyy-MM-dd HH:mm")) \
                      .withColumn("duracion_viaje",F.col("hora_bajada").cast("long") - F.col("hora_subida").cast("long"))

df_viajes.display()

# COMMAND ----------

# ---- 4. Normalizar transbordo ----

df_viajes = df_viajes.withColumn("transbordo_bin",
                                 F.when (F.col("transbordo") == "Sí",1).otherwise(0) )
df_viajes.display()



# COMMAND ----------

# ---- 5. Asignar id_usuario aleatorio a cada viaje ----
usuarios_list = [row.id_usuario for row in df_usuarios.collect()]
assign_udf = F.udf(lambda: random.choice(usuarios_list), IntegerType())

df_viajes = df_viajes.withColumn("id_usuario", assign_udf())



# COMMAND ----------

df_silver = df_viajes.join(df_usuarios, df_viajes.id_usuario == df_usuarios.id_usuario,"inner") \
    .drop(df_usuarios.id_usuario)

# ---- 7. Guardar en Delta ----
df_silver.write.format("delta").mode("overwrite").save(f"{silver_path}/viajes_usuarios")
#print(f"Capa Silver creada con {df_silver.count()} registros.")
df_silver.display()