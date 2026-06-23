# Databricks notebook source
CATALOG = "workspace"
SCHEMA  = "ifood_case"

from pyspark.sql import functions as F

bronze = spark.table(f"{CATALOG}.{SCHEMA}.bronze_yellow_taxi")   # dado CRU
gold   = spark.table(f"{CATALOG}.{SCHEMA}.gold_yellow_taxi")     # dado CONFIÁVEL

print("Bronze (cru):", f"{bronze.count():,}")
print("Gold (confiável):", f"{gold.count():,}")

# COMMAND ----------

# INGÊNUA: no Bronze cru — inclui negativos, outliers e meses vazados
q1_naive = (bronze
            .withColumn("pickup_month", F.date_trunc("month", "tpep_pickup_datetime"))
            .groupBy("pickup_month")
            .agg(F.round(F.avg("total_amount"), 2).alias("avg_naive"),
                 F.count("*").alias("corridas_naive"))
            .orderBy("pickup_month"))

# CONFIÁVEL: na Gold validada
q1_trust = (gold
            .groupBy("pickup_month")
            .agg(F.round(F.avg("total_amount"), 2).alias("avg_confiavel"),
                 F.count("*").alias("corridas_confiavel"))
            .orderBy("pickup_month"))

# Comparação lado a lado, só nos 5 meses válidos
q1 = (q1_trust.join(q1_naive, "pickup_month", "left")
      .withColumn("delta_pct",
                  F.round(100 * (F.col("avg_confiavel") - F.col("avg_naive")) / F.col("avg_naive"), 2))
      .select("pickup_month", "avg_naive", "avg_confiavel", "delta_pct",
              "corridas_naive", "corridas_confiavel")
      .orderBy("pickup_month"))

display(q1)

# COMMAND ----------

# INGÊNUA: maio no Bronze — inclui passenger_count nulo e zero
q2_naive = (bronze
            .filter((F.col("tpep_pickup_datetime") >= "2023-05-01") &
                    (F.col("tpep_pickup_datetime") < "2023-06-01"))
            .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
            .groupBy("pickup_hour")
            .agg(F.round(F.avg("passenger_count"), 3).alias("avg_pass_naive"))
            .orderBy("pickup_hour"))

# CONFIÁVEL: maio na Gold (passenger_count entre 1 e 6)
q2_trust = (gold
            .filter(F.col("pickup_month") == "2023-05-01")
            .groupBy("pickup_hour")
            .agg(F.round(F.avg("passenger_count"), 3).alias("avg_pass_confiavel"))
            .orderBy("pickup_hour"))

q2 = (q2_trust.join(q2_naive, "pickup_hour", "left")
      .select("pickup_hour", "avg_pass_naive", "avg_pass_confiavel")
      .orderBy("pickup_hour"))

display(q2)

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ## Resumo dos resultados
# MAGIC
# MAGIC - **Ingestão:** 16.186.386 corridas (jan–mai/2023), schema padronizado na entrada.
# MAGIC - **Qualidade:** 11 regras / 6 dimensões. **850.704 registros (5,26%)** reprovados e isolados em quarentena. **Silver confiável: 15.335.554**.
# MAGIC - **Reconciliação:** 16.186.386 − 850.704 − 128 duplicatas = 15.335.554 ✅
# MAGIC - **P1 — média do valor por mês:** corrigida pela remoção de valores ≤ 0 e meses vazados (ver `delta_pct`).
# MAGIC - **P2 — passageiros por hora (maio):** corrigida pela remoção de `passenger_count` nulo/zero.
# MAGIC