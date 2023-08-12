# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("data_source", "")
data_source = dbutils.widgets.get("data_source")

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql.types import *
from pyspark.sql.functions import to_timestamp, concat, col, lit, current_timestamp

# COMMAND ----------

# DBTITLE 1,Define Schema for races dataframe
races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                  StructField("year", IntegerType(), True),
                                  StructField("round", IntegerType(), True),
                                  StructField("circuitId", IntegerType(), True),
                                  StructField("name", StringType(), True),
                                  StructField("date", DateType(), True),
                                  StructField("time", StringType(), True),
                                  ])

# COMMAND ----------

# DBTITLE 1,Read CSV File
races_df = spark.read \
.option("header", True) \
.schema(races_schema) \
.csv(f"{raw_folder_path}/races.csv")

# COMMAND ----------

# DBTITLE 1,Rename Columns
races_renamed_df = races_df.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("year", "race_year") \
.withColumnRenamed("circuitId", "circuit_id") 


# COMMAND ----------

# DBTITLE 1,Transform date and time into race_timestamp column
races_timestamp_df = races_renamed_df.withColumn('race_timestamp', to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')) \
                                 .withColumn('ingestion_date', current_timestamp()).withColumn("data_source", lit(data_source))

# COMMAND ----------

# DBTITLE 1,Select Required Columns
races_selected_df = races_timestamp_df.select(col('race_id'), col('race_year'), col('round'), col('name'), col('ingestion_date'), col('race_timestamp'), col('circuit_id'))

# COMMAND ----------

# DBTITLE 1,Write to Processed Container in Datalake as parquet file
# races_selected_df.write.mode('overwrite').partitionBy('race_year').parquet(f"{processed_folder_path}/races")

races_selected_df.write.mode("overwrite").partitionBy('race_year').format("parquet").saveAsTable("f1_processed.races")

# COMMAND ----------


