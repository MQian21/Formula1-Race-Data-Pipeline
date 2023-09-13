# Databricks notebook source
# DBTITLE 1,Import Libs
from pyspark.sql.functions import sum, when, count, col, desc, rank
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Run configurations
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# DBTITLE 1,Run Configurations
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("file_date", "2021-03-28")
file_date = dbutils.widgets.get("file_date")

# COMMAND ----------

# DBTITLE 1,Find race years for which the data is to be reprocessed and store them as a list
race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results") \
    .filter(f"file_date = '{file_date}'")

race_year_list = df_column_to_list(race_results_df, 'race_year')

# COMMAND ----------

# DBTITLE 1,Load race results 
race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results") \
    .filter(col("race_year").isin(race_year_list))

# COMMAND ----------

race_year_list = df_to_column_to_list(race_results_df, 'race_year')

# COMMAND ----------

# DBTITLE 1,Determine constructor standings using groupBy
constructor_standings_df = race_results_df.groupBy("race_year", "team") \
                                          .agg(sum("points").alias("total_points"), \
                                               count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

# DBTITLE 1,Create Window to generate rank column for dataframe
constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = constructor_standings_df.withColumn("rank", rank().over(constructor_rank_spec))

display(final_df)

# COMMAND ----------

# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.constructor_standings")

overwrite_partition(final_df, 'f1_presentation', 'constructor_standings', 'race_year')

# COMMAND ----------


