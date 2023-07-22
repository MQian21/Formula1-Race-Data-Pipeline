# Databricks notebook source
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

results_df = spark.read.schema(results_schema).json("/mnt/formula1lakedata/raw/results.json")

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
                                    .withColumn("ingestion_date", current_timestamp())
                                    
results_final_df = results_with_columns_df.drop(col("statusId"))

# COMMAND ----------

results_final_df.write.mode("overwrite").partitionBy('race_id').parquet("/mnt/formula1lakedata/processed/results")

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1lakedata/processed/results"))

# COMMAND ----------


