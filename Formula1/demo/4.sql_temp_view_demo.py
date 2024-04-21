# Databricks notebook source
# MAGIC %md
# MAGIC ####access dataframes using sql

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_results_df.createOrReplaceTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM v_race_results
# MAGIC WHERE race_year=2020

# COMMAND ----------

race_results_2019_df=spark.sql("SELECT* FROM v_race_results WHERE race_year = 2019")

# COMMAND ----------

#display(race_results_2019_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####GlobalTempView

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("v_race_results_global")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN global_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM global_temp.v_race_results_global
# MAGIC WHERE race_year=2020

# COMMAND ----------

global_race_results_2019_df=spark.sql("SELECT* FROM global_temp.v_race_results_global WHERE race_year = 2019")

# COMMAND ----------

#display(global_race_results_2019_df)

# COMMAND ----------

