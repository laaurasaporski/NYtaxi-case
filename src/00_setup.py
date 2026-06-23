# Databricks notebook source
CATALOG = "workspace"    
SCHEMA  = "ifood_case"
VOLUME  = "landing"

MONTHS = ["2023-01", "2023-02", "2023-03", "2023-04", "2023-05"]

print(f"Vou trabalhar em: {CATALOG}.{SCHEMA}")
print(f"Landing zone:     /Volumes/{CATALOG}/{SCHEMA}/{VOLUME}")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")

print("Schema e volume prontos.")

# COMMAND ----------

display(spark.sql(f"SHOW VOLUMES IN {CATALOG}.{SCHEMA}"))