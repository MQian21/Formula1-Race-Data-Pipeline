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

# DBTITLE 1,Import Libs
from pyspark.sql.types import *
from pyspark.sql.functions import to_timestamp, concat, col, lit, current_timestamp

# COMMAND ----------

# DBTITLE 1,Define Schema
results_schema = StructType(fields = [StructField("resultId", IntegerType(), False),
                                    StructField("raceId", IntegerType(), False),
                                    StructField("driverId", IntegerType(), False),
                                    StructField("constructorId", IntegerType(), False),
                                    StructField("number", IntegerType(), True),
                                    StructField("grid", IntegerType(), False),
                                    StructField("position", IntegerType(), True), 
                                    StructField("positionText", StringType(), False),
                                    StructField("positionOrder", IntegerType(), False),
                                    StructField("points", FloatType(), False),
                                    StructField("laps", IntegerType(), False),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True),
                                    StructField("fastestLap", IntegerType(), True),
                                    StructField("rank", IntegerType(), True),
                                    StructField("fastestLapTime", StringType(), True),
                                    StructField("fastestLapSpeed", StringType(), True),
                                    StructField("statusId", IntegerType(), False),
])

# COMMAND ----------

results_df = spark.read.schema(results_schema).json(f"{raw_folder_path}/{file_date}/results.json")

# COMMAND ----------

results_with_columns_df = results_df.withColumnRenamed("resultId", "result_id") \
                                    .withColumnRenamed("raceId", "race_id") \
                                    .withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("constructorId", "constructor_id") \
                                    .withColumnRenamed("positionText", "position_text") \
                                    .withColumnRenamed("positionOrder", "position_order") \
                                    .withColumnRenamed("fastestLap", "fastest_lap") \
                                    .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                                    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
                                    .withColumn("ingestion_date", current_timestamp()) \
                                    .withColumn("data_source", lit(data_source)) \
                                    .withColumn("file_date", lit(file_date))
                                    
results_final_df = results_with_columns_df.drop(col("statusId"))

# COMMAND ----------

# DBTITLE 1,Incremental Load Method 1
# for race_id_list in results_final_df.select("race_id").distinct().collect():
#     if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#         spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

# DBTITLE 1,Incremental Load Method 1
# results_final_df.write.mode("append").partitionBy('race_id').format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# DBTITLE 1,Incremental Load Method 2
overwrite_partition(results_final_df, "f1_processed", "results", "race_id")   

# COMMAND ----------


