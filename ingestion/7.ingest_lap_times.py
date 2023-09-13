# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("data_source", "")
data_source = dbutils.widgets.get("data_source")

# COMMAND ----------

dbutils.widgets.text("file_date", "2021-03-28")
file_date = dbutils.widgets.get("file_date")

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import concat, col, lit, current_timestamp

# COMMAND ----------

lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

lap_times_df = spark.read \
.schema(lap_times_schema) \
.csv(f"{raw_folder_path}/{file_date}/lap_times")

# COMMAND ----------

final_lap_df = lap_times_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("data_source", lit(data_source)) \
.withColumn("file_date", lit(file_date))

final_lap_df = add_ingestion_date(final_lap_df)

# COMMAND ----------

# final_lap_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.lap_times")
overwrite_partition(final_lap_df, "f1_processed", "lap_times", "race_id")   

# COMMAND ----------


