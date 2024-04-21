# Databricks notebook source
display(dbutils.fs.ls("abfss://demo@formula1dltriple.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dltriple.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

