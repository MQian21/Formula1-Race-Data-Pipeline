# Databricks notebook source
# DBTITLE 1,Import Libraries
from pyspark.sql.types import *
from pyspark.sql.functions import to_timestamp, concat, col, lit, current_timestamp

# COMMAND ----------

# DBTITLE 1,Define Schema for races dataframe
races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                  StructField("year", IntegerType(), False),
                                  StructField("round", IntegerType(), False),
                                  StructField("circuitId", IntegerType(), False),
                                  StructField("name", StringType(), False),
                                  StructField("date", DateType(), False),
                                  StructField("time", StringType(), False),
                                  ])

# COMMAND ----------

# DBTITLE 1,Read CSV File
races_df = spark.read \
.option("header", True) \
.schema(races_schema) \
.csv("/mnt/formula1lakedata/raw/races.csv")

# COMMAND ----------

# DBTITLE 1,Rename Columns
races_renamed_df = races_df.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("year", "race_year") \
.withColumnRenamed("circuitId", "circuit_id") 


# COMMAND ----------

# DBTITLE 1,Transform date and time into race_timestamp column
races_timestamp_df = races_renamed_df.withColumn('race_timestamp', to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')) \
                                 .withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

# DBTITLE 1,Select Required Columns
races_selected_df = races_timestamp_df.select(col('race_id'), col('race_year'), col('round'), col('name'), col('ingestion_date'), col('race_timestamp'))

# COMMAND ----------

# DBTITLE 1,Write to Processed Container in Datalake as parquet file
races_selected_df.write.mode('overwrite').partitionBy('race_year').parquet('/mnt/formula1lakedata/processed/races')

# COMMAND ----------

display(spark.read.parquet('/mnt/formula1lakedata/processed/races'))
