# Databricks notebook source
def mount_adls(storage_account_name, container_name):
    #Get secrets
    client_id="6dfbd2be-3f18-4c3d-9a40-b4f631ce0a13"
    tenant_id="b53cc320-6953-49a4-ad32-9198d8acdda0"
    client_secret= "iun8Q~cdhdn1-V2FYaECW5NWbdria.133EXu0bAS"
    #Set Spark Config
    configs = {"fs.azure.account.auth.type": "OAuth",
        "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        "fs.azure.account.oauth2.client.id": client_id,
        "fs.azure.account.oauth2.client.secret": client_secret,
        "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    # Unmount the mount point if it already exists
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")
    #Mount Storage Container
    dbutils.fs.mount(
      source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
      mount_point = f"/mnt/{storage_account_name}/{container_name}",
      extra_configs = configs)
    display(dbutils.fs.mounts())

# COMMAND ----------

mount_adls('formula1dltriple', 'raw')

# COMMAND ----------

mount_adls('formula1dltriple', 'processed')

# COMMAND ----------

mount_adls('formula1dltriple', 'presentation')

# COMMAND ----------

mount_adls('formula1dltriple', 'demo')

# COMMAND ----------

dbutils.fs.ls("/mnt/formula1dltriple/demo")

# COMMAND ----------

