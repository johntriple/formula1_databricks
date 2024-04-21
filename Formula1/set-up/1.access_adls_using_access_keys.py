# Databricks notebook source
spark.conf.set(
    "fs.azure.account.key.formula1dltriple.dfs.core.windows.net",
    "8f+5vJkcDQnve8W3sZM5DwmlP/32AN8L4VL+dTNxI1+oSdGXwChhIhYP7IgMbmpBBpcqKTTQ1qfD+AStjULQ9A=="
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dltriple.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dltriple.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

