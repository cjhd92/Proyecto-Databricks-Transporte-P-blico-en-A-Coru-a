# 🚍 Proyecto Databricks: Transporte Público en A Coruña

Este proyecto implementa un pipeline de datos usando la arquitectura de medallón (Bronze–Silver–Gold) 
en Databricks con PySpark y Delta Lake.

## 📂 Estructura
- **Bronze**: datos crudos en Delta.
- **Silver**: datos limpios y normalizados.
- **Gold**: modelo dimensional (Hecho_Viajes + Dimensiones) y vistas con métricas clave.

## 📊 Métricas Clave
- Total de viajes por línea.
- Viajes por tipo de abono.
- Top 3 paradas más utilizadas.
- Ranking de usuarios.
- Hora pico de mayor demanda.

## 📌 Cómo usar
1. Subir los CSV de `data/` a tu entorno Databricks.
2. Ejecutar los notebooks de `bronze/`, `silver/` y `gold/`.
3. Consultar las vistas de métricas creadas (`vw_total_viajes_por_linea`, etc.).

📄 Ver la documentación completa en: [proyecto_transporte_acoruna.pdf](./proyecto_transporte_acoruna.pdf)
