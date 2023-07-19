# Databricks notebook source
# MAGIC %md
# MAGIC #### Explore the capabilities of the dbutils.secrets.utility

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope = 'formulaOne-scope')

# COMMAND ----------

dbutils.secrets.get(scope = 'formulaOne-scope', key = 'formula1dl-account-key')
