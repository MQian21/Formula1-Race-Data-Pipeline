# Databricks notebook source
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

pit_stops_df = spark.read.schema(pit_stops_schema).option("multiLine", True).json("/mnt/formula1lakedata/raw/pit_stops.json")

# COMMAND ----------

pit_final_df = pit_stops_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("ingestion_date", current_timestamp()) 

# COMMAND ----------

pit_final_df.write.mode("overwrite").parquet("/mnt/formula1lakedata/processed/pit_stops")

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1lakedata/processed/pit_stops"))

# COMMAND ----------


