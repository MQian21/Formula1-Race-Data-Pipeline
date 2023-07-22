# Databricks notebook source
# DBTITLE 1,Import Libs
from pyspark.sql.types import *
from pyspark.sql.functions import to_timestamp, concat, col, lit, current_timestamp

# COMMAND ----------

# DBTITLE 1,Define Schema
constructors_schema = StructType(fields=[StructField("constructorId", IntegerType(), False),
                                  StructField("constructorRef", StringType(), True),
                                  StructField("name", StringType(), True),
                                  StructField("nationality", StringType(), True),
                                  StructField("url", StringType(), True),
                                  ])

# COMMAND ----------

# DBTITLE 1,Use dataframe reader to read json file
constructor_df = spark.read \
.schema(constructors_schema) \
.json("/mnt/formula1lakedata/raw/constructors.json")

display(constructor_df)

# COMMAND ----------

# DBTITLE 1,Drop Url Column
constructor_dropped_df = constructor_df.drop('url')

# COMMAND ----------

# DBTITLE 1,Add ingestion date and rename columns
constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
                                             .withColumnRenamed("constructorRef", "constructor_ref") \
                                             .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# DBTITLE 1,Write to processed container
constructor_final_df.write.mode("overwrite").parquet("/mnt/formula1lakedata.processed/constructors")

# COMMAND ----------

dbutils.fs.ls("/mnt/formula1lakedata.processed/constructors")
