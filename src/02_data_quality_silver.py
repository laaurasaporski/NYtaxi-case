# Databricks notebook source
CATALOG = "workspace"
SCHEMA  = "ifood_case"

# Parâmetros das regras de negócio — deixados explícitos e ajustáveis de propósito
PERIOD_START   = "2023-01-01"   # janela pedida no enunciado
PERIOD_END     = "2023-06-01"   # exclusivo (pega até 31/maio)
MAX_TOTAL_AMT  = 1000.0         # corrida de táxi acima disso ≈ erro de registro
MIN_PASSENGERS = 1
MAX_PASSENGERS = 6              # limite operacional do táxi amarelo
MAX_TRIP_HOURS = 24             # corrida acima de 24h ≈ erro

print("Parâmetros de qualidade definidos.")

# COMMAND ----------

from pyspark.sql import functions as F

bronze = spark.table(f"{CATALOG}.{SCHEMA}.bronze_yellow_taxi")

# Duração da corrida em segundos — reusar isso em algumas regras
bronze = bronze.withColumn(
    "_trip_duration_sec",
    F.col("tpep_dropoff_datetime").cast("long") - F.col("tpep_pickup_datetime").cast("long"),
)

print(f"Bronze carregada: {bronze.count():,} linhas")

# COMMAND ----------

rules = [
    # (nome,                      dimensão,        condição que vale True quando o registro PASSA)
    ("pickup_not_null",           "completeness",  F.col("tpep_pickup_datetime").isNotNull()),
    ("dropoff_not_null",          "completeness",  F.col("tpep_dropoff_datetime").isNotNull()),
    ("total_amount_not_null",     "completeness",  F.col("total_amount").isNotNull()),
    ("passenger_count_not_null",  "completeness",  F.col("passenger_count").isNotNull()),
    ("total_amount_positive",     "validity",      F.col("total_amount") > 0),
    ("total_amount_within_cap",   "accuracy",      F.col("total_amount") <= MAX_TOTAL_AMT),
    ("passenger_count_in_range",  "validity",      F.col("passenger_count").between(MIN_PASSENGERS, MAX_PASSENGERS)),
    ("dropoff_after_pickup",      "consistency",   F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime")),
    ("trip_duration_within_24h",  "validity",      (F.col("_trip_duration_sec") > 0) & (F.col("_trip_duration_sec") <= MAX_TRIP_HOURS * 3600)),
    ("pickup_within_period",      "timeliness",    (F.col("tpep_pickup_datetime") >= F.lit(PERIOD_START)) & (F.col("tpep_pickup_datetime") < F.lit(PERIOD_END))),
    ("vendor_id_valid",           "validity",      F.col("VendorID").isin(1, 2, 6, 7)),
]

print(f"{len(rules)} regras definidas.")
print("Dimensões cobertas:", sorted(set(dim for _, dim, _ in rules)))

# COMMAND ----------

# 1. Para cada regra, cria uma coluna pass__<regra> (True = passou)
flagged = bronze
for name, dim, cond in rules:
    flagged = flagged.withColumn(f"pass__{name}", cond)

# 2. Junta os nomes das regras REPROVADAS em um array, por registro
fail_array = F.array(*[
    F.when(~F.col(f"pass__{name}"), F.lit(name)) for name, dim, cond in rules
])
flagged = (flagged
           .withColumn("dq_failed_rules", F.array_compact(fail_array))
           .withColumn("is_valid", F.size("dq_failed_rules") == 0))

# 3. Persiste como tabela Delta (substitui o cache — compatível com serverless)
(flagged.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.{SCHEMA}._flagged_yellow_taxi"))

# Relê a tabela materializada para usar nas próximas células
flagged = spark.table(f"{CATALOG}.{SCHEMA}._flagged_yellow_taxi")
total_records = flagged.count()

print(f"Registros avaliados: {total_records:,}")

# COMMAND ----------

# Conta as reprovações de TODAS as regras numa única passada (1 agregação, não 11)
agg_exprs = [
    F.sum(F.when(~F.col(f"pass__{name}"), 1).otherwise(0)).alias(name)
    for name, dim, cond in rules
]
agg_exprs.append(F.sum(F.when(~F.col("is_valid"), 1).otherwise(0)).alias("__any_failure"))

row = flagged.agg(*agg_exprs).collect()[0]

# Monta o scorecard: regra, dimensão, qtd reprovada, % reprovada
dim_by_rule = {name: dim for name, dim, cond in rules}
scorecard_rows = [
    (name, dim_by_rule[name], int(row[name]), total_records, round(100 * row[name] / total_records, 4))
    for name, dim, cond in rules
]

scorecard = (spark.createDataFrame(
                 scorecard_rows,
                 ["rule", "dimension", "failed_records", "total_records", "failed_pct"])
             .orderBy(F.desc("failed_pct")))

any_fail = row["__any_failure"]
print(f"Registros com ao menos 1 falha: {any_fail:,} ({100*any_fail/total_records:.2f}%)")
display(scorecard)

# COMMAND ----------

# MAGIC %md
# MAGIC 850.704 registros (5,26%) tinham ao menos um problema de qualidade. (1 em cada 19 registros)

# COMMAND ----------


scorecard_hist = scorecard.withColumn("run_ts", F.current_timestamp())

(scorecard_hist.write
               .format("delta")
               .mode("append")                      
               .option("mergeSchema", "true")
               .saveAsTable(f"{CATALOG}.{SCHEMA}.dq_results"))

print("Scorecard salvo no histórico: dq_results")
display(spark.table(f"{CATALOG}.{SCHEMA}.dq_results").orderBy(F.desc("run_ts"), F.desc("failed_pct")))

# COMMAND ----------

# Isola os registros reprovados — SEM apagar.
quarantine = flagged.filter(~F.col("is_valid")).select(
    "VendorID",
    "passenger_count",
    "total_amount",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "dq_failed_rules",          # o "porquê" de cada rejeição
    "_source_file",             # rastreabilidade até o arquivo de origem
    "_ingestion_ts",
)

(quarantine.write
           .format("delta")
           .mode("overwrite")
           .option("overwriteSchema", "true")
           .saveAsTable(f"{CATALOG}.{SCHEMA}.silver_yellow_taxi_quarantine"))

q_count = quarantine.count()
print(f"Quarentena: {q_count:,} registros isolados.")


display(quarantine.select("total_amount", "passenger_count", "dq_failed_rules").limit(10))

# COMMAND ----------

# Mantém só os válidos, seleciona as colunas obrigatórias + derivações úteis para a análise
silver = (flagged.filter(F.col("is_valid"))
          .select(
              "VendorID",
              "passenger_count",
              "total_amount",
              "tpep_pickup_datetime",
              "tpep_dropoff_datetime",
              F.date_trunc("month", "tpep_pickup_datetime").alias("pickup_month"),
              F.hour("tpep_pickup_datetime").alias("pickup_hour"),
              F.round(F.col("_trip_duration_sec") / 60, 2).alias("trip_duration_min"),
          ))


dedup_keys = ["VendorID", "passenger_count", "total_amount",
              "tpep_pickup_datetime", "tpep_dropoff_datetime"]
antes = silver.count()
silver = silver.dropDuplicates(dedup_keys)
depois = silver.count()
print(f"Uniqueness: {antes - depois:,} duplicatas removidas ({antes:,} -> {depois:,})")

(silver.write
       .format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(f"{CATALOG}.{SCHEMA}.silver_yellow_taxi"))

print("Silver gravada!")
display(silver.limit(5))