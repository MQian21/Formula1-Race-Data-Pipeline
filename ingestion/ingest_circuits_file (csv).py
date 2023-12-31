# Databricks notebook source
# MAGIC %md
# MAGIC #### Injest circuits.csv file

# COMMAND ----------

# DBTITLE 1,Calling config notebooks
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("data_source", "")
data_source = dbutils.widgets.get("data_source")

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import col, lit


# COMMAND ----------

# DBTITLE 1,Define Schema for circuits dataframe
circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True)

])

# COMMAND ----------

# DBTITLE 1,Read the CSV file using the spark dataframe reader + (remove header from first row)
circuits_df = spark.read \
.option("header", True) \
.schema(circuits_schema) \
.csv(f"{raw_folder_path}/circuits.csv")

# COMMAND ----------

# DBTITLE 1,Select Only Required Columns
circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

# DBTITLE 1,Rename Columns
circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
.withColumnRenamed("circuitRef", "circuit_ref") \
.withColumnRenamed("lat", "latitude") \
.withColumnRenamed("lng", "longitude") \
.withColumnRenamed("alt", "altitude") \
.withColumn("data_source", lit(data_source))


# COMMAND ----------

# DBTITLE 1,Add new column for ingestion date
circuits_final_df = add_ingestion_date(circuits_renamed_df)


# COMMAND ----------

# DBTITLE 1,Write Data to Datalake as Parquet file format
circuits_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")

# circuits_final_df.write.mode("overwrite").parquet("/mnt/formula1lakedata/processed/circuits")


# COMMAND ----------


