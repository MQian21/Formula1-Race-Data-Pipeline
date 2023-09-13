# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("file_date", "2021-03-28")
file_date = dbutils.widgets.get("file_date")

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col, desc, rank, col
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Find race years for which the data is to be reprocessed and store them as a list
race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results") \
.filter(f"file_date = '{file_date}'")

race_year_list = df_column_to_list(race_results_df, 'race_year')

# COMMAND ----------

driver_standings_df = race_results_df.groupBy("race_year", "driver_name", "driver_nationality", "team") \
                                     .agg(sum("points").alias("total_points"), count(when(col("position") == 1, True)).alias("wins"))
display(driver_standings_df)

# COMMAND ----------

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = driver_standings_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.driver_standings")

overwrite_partition(final_df, 'f1_presentation', 'driver_standings', 'race_year')

# COMMAND ----------


