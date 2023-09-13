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
from pyspark.sql.functions import to_timestamp, concat, col, lit, current_timestamp

# COMMAND ----------

pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("stop", StringType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("duration", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

pit_stops_df = spark.read.schema(pit_stops_schema).option("multiLine", True).json(f"{raw_folder_path}/{file_date}/pit_stops.json")

# COMMAND ----------

pit_final_df = pit_stops_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("data_source", lit(data_source)) \
.withColumn("file_date", lit(file_date))

# COMMAND ----------

# pit_final_df.write.mode("overwrite").parquet("/mnt/formula1lakedata/processed/pit_stops")
#pit_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pit_stops")

overwrite_partition(pit_final_df, "f1_processed", "pit_stops", "race_id")   


# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/pit_stops"))
