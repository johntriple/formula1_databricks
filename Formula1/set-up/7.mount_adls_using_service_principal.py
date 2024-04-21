# Databricks notebook source
client_id="6dfbd2be-3f18-4c3d-9a40-b4f631ce0a13"

# COMMAND ----------

tenant_id="b53cc320-6953-49a4-ad32-9198d8acdda0"

# COMMAND ----------

client_secret= "iun8Q~cdhdn1-V2FYaECW5NWbdria.133EXu0bAS"

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@formula1dltriple.dfs.core.windows.net/",
  mount_point = "/mnt/formula1dltriple/demo",
  extra_configs = configs)


# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1dltriple/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/formula1dltriple/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

