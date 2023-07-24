# Databricks notebook source
# DBTITLE 1,Add ingestion column for DF
from pyspark.sql.functions import current_timestamp
def add_ingestion_date(input_df):
    output_df = input_df.withColumn("ingestion_date", current_timestamp())
    return output_df
