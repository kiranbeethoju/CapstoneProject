# Databricks notebook source
# Define your configurations
storage_account_name = "your_storage_account_name"
container_name = "your_container_name"
account_key = "your_account_key"  # Set this via environment variable or secure configuration

# Set Spark config for ADLS Gen2 using Account Key
spark.conf.set(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", account_key)


# COMMAND ----------

df=spark.table("default.fhv_tripdata_2025_04_taxi")

# COMMAND ----------

df.coalesce(1).write \
    .option("header", "true") \
    .mode("overwrite") \
    .csv(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/format.csv")

# COMMAND ----------

df.coalesce(1).write \
    .mode("overwrite") \
    .parquet(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/parquet_format.parquet")

# COMMAND ----------

