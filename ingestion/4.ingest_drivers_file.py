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
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

# DBTITLE 1,Define Schema
name_schema = StructType(fields = [StructField("forename", StringType(), True),
                                   StructField("surname", StringType(), True)
])

drivers_schema = StructType(fields = [StructField("driverId", IntegerType(), False),
                                     StructField("driverRef", StringType(), True),
                                     StructField("number", IntegerType(), True),
                                     StructField("code", StringType(), True),
                                     StructField("name", name_schema),
                                     StructField("dob", DateType(), True),
                                     StructField("nationality", StringType(), True),
                                     StructField("url", StringType(), True),                                
])

# COMMAND ----------

# DBTITLE 1,Read File
drivers_df = spark.read.schema(drivers_schema).json(f"{raw_folder_path}/{file_date}/drivers.json")

# COMMAND ----------

# DBTITLE 1,Rename Columns
drivers_renamed_df = drivers_df.withColumnRenamed("driverId", "driver_id") \
                               .withColumnRenamed("driverRef", "driver_ref") \
                               .withColumn("ingestion_date", current_timestamp()) \
                               .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname"))) \
                               .withColumn("data_source", lit(data_source)) \
                               .withColumn("file_date", lit(file_date))
display(drivers_renamed_df)

# COMMAND ----------

# DBTITLE 1,Drop url column
drivers_final_df = drivers_renamed_df.drop('url')

# COMMAND ----------

# DBTITLE 1,Write to processed container as parquet file
# drivers_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/drivers")

drivers_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.drivers")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.drivers

# COMMAND ----------


