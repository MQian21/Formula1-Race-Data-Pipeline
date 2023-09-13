# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("data_source", "")
data_source = dbutils.widgets.get("data_source")

# COMMAND ----------

dbutils.widgets.text("file_date", "2021-03-21")
file_date = dbutils.widgets.get("file_date")

# COMMAND ----------

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
.json(f"{raw_folder_path}/{file_date}/constructors.json")

display(constructor_df)

# COMMAND ----------

# DBTITLE 1,Drop Url Column
constructor_dropped_df = constructor_df.drop('url')

# COMMAND ----------

# DBTITLE 1,Add ingestion date and rename columns
constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
                                             .withColumnRenamed("constructorRef", "constructor_ref") \
                                             .withColumn("ingestion_date", current_timestamp()) \
                                             .withColumn("data_source", lit(data_source)) \
                                             .withColumn("file_date", lit(file_date))

# COMMAND ----------

# DBTITLE 1,Write to processed container

constructor_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructors")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/constructors"))
