# Databricks notebook source
CATALOG = "workspace"
SCHEMA  = "ifood_case"

from pyspark.sql import functions as F

silver = spark.table(f"{CATALOG}.{SCHEMA}.silver_yellow_taxi")


gold = silver.select(
    "VendorID",
    "passenger_count",
    "total_amount",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "pickup_month",
    "pickup_hour",
    "trip_duration_min",
)

(gold.write
     .format("delta")
     .mode("overwrite")
     .option("overwriteSchema", "true")
     .saveAsTable(f"{CATALOG}.{SCHEMA}.gold_yellow_taxi"))

print(f"Gold gravada: {gold.count():,} linhas")

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.vw_yellow_taxi AS
    SELECT
        VendorID,
        passenger_count,
        total_amount,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        pickup_month,
        pickup_hour,
        trip_duration_min
    FROM {CATALOG}.{SCHEMA}.gold_yellow_taxi
""")

print("View criada: vw_yellow_taxi")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     pickup_month,
# MAGIC     COUNT(*)                       AS corridas,
# MAGIC     ROUND(AVG(total_amount), 2)    AS ticket_medio
# MAGIC FROM workspace.ifood_case.vw_yellow_taxi
# MAGIC GROUP BY pickup_month
# MAGIC ORDER BY pickup_month