# Databricks notebook source
# MAGIC %md
# MAGIC #### Mount Azure Data Lake Containers for the Project

# COMMAND ----------

def mount_adls(storage_account_name, container_name) :
    # Get Secrets from Key Vault
    client_id = dbutils.secrets.get(scope = 'formulaOne-scope', key = 'formula1-app-client-id')
    tenant_id = dbutils.secrets.get(scope = 'formulaOne-scope', key = 'formula1-app-tenant-id')
    client_secret = dbutils.secrets.get(scope = 'formulaOne-scope', key = 'formula1-app-client-secret')

    # Set Spark Configurations
    configs = {"fs.azure.account.auth.type": "OAuth",
                "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
                "fs.azure.account.oauth2.client.id": client_id,
                "fs.azure.account.oauth2.client.secret": client_secret,
                "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    # Unmount mountPoint if it already exists
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")
    
    # Mount the storage account container
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account_name}/{container_name}",
        extra_configs = configs)
    
    # List all the mounts
    display(dbutils.fs.mounts())


# COMMAND ----------

# DBTITLE 1,Mount Containers from formula1lakedata
mount_adls('formula1lakedata', 'raw')
mount_adls('formula1lakedata', 'processed')
mount_adls('formula1lakedata', 'presentation')
