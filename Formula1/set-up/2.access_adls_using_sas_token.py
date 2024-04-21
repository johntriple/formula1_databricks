# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type.formula1dltriple.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1dltriple.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1dltriple.dfs.core.windows.net", "sp=rl&st=2024-03-13T13:23:55Z&se=2024-04-06T20:23:55Z&spr=https&sv=2022-11-02&sr=c&sig=9rERe9SZuL9w4%2BPgFAJNUgD1vfRRYcmVSfuk2bxr3o0%3D")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dltriple.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dltriple.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

