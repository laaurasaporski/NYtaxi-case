# Databricks notebook source
CATALOG = "workspace"
SCHEMA  = "ifood_case"
VOLUME  = "landing"
MONTHS  = ["2023-01", "2023-02", "2023-03", "2023-04", "2023-05"]

LANDING  = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

print(f"Vou baixar para: {LANDING}")

# COMMAND ----------

import os
import shutil
import urllib.request

for m in MONTHS:
    fname = f"yellow_tripdata_{m}.parquet"
    dest  = f"{LANDING}/{fname}"
    if os.path.exists(dest):
        print(f"• já existe, pulando: {fname}")
        continue
    url = f"{BASE_URL}/{fname}"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req) as resp, open(dest, "wb") as out:
        shutil.copyfileobj(resp, out)
    print(f"✓ baixado: {fname} ({os.path.getsize(dest)/1e6:.1f} MB)")

display(dbutils.fs.ls(LANDING))

# COMMAND ----------

from pyspark.sql import functions as F
from functools import reduce


# Colunas que interessam, já com o tipo padronizado (cast garante consistência entre meses)
def padroniza(df, source_path):
    return (df.select(
                F.col("VendorID").cast("long").alias("VendorID"),
                F.col("passenger_count").cast("double").alias("passenger_count"),
                F.col("total_amount").cast("double").alias("total_amount"),
                F.col("tpep_pickup_datetime").cast("timestamp").alias("tpep_pickup_datetime"),
                F.col("tpep_dropoff_datetime").cast("timestamp").alias("tpep_dropoff_datetime"),
            )
            .withColumn("_source_file", F.lit(source_path))
            .withColumn("_ingestion_ts", F.current_timestamp()))

# Lê cada mês separadamente, padroniza, e só então empilha (union) (elimina o conflito na raiz)
dfs = []
for m in MONTHS:
    path = f"{LANDING}/yellow_tripdata_{m}.parquet"
    df_mes = spark.read.parquet(path)
    dfs.append(padroniza(df_mes, path))

bronze_df = reduce(lambda a, b: a.unionByName(b), dfs)

print(f"Linhas brutas ingeridas: {bronze_df.count():,}")
bronze_df.printSchema()    #  na linha do df_mes, o tipo original era timestamp_ntz e o VendorID era integer. Padronizei isso pra timestamp e long no cast, o conflito que quebrou o mergeSchema foi resolvido na raiz.

# COMMAND ----------

(bronze_df.write
          .format("delta")
          .mode("overwrite")
          .option("overwriteSchema", "true")
          .saveAsTable(f"{CATALOG}.{SCHEMA}.bronze_yellow_taxi"))

print("Bronze gravada!")

display(spark.sql(f"""
    SELECT _source_file, COUNT(*) AS linhas
    FROM {CATALOG}.{SCHEMA}.bronze_yellow_taxi
    GROUP BY _source_file
    ORDER BY _source_file
"""))

# COMMAND ----------

