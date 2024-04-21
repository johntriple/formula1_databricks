# Databricks notebook source
client_id="6dfbd2be-3f18-4c3d-9a40-b4f631ce0a13"

# COMMAND ----------

tenant_id="b53cc320-6953-49a4-ad32-9198d8acdda0"

# COMMAND ----------

client_secret= "iun8Q~cdhdn1-V2FYaECW5NWbdria.133EXu0bAS"

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dltriple.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1dltriple.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1dltriple.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1dltriple.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1dltriple.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dltriple.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dltriple.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

