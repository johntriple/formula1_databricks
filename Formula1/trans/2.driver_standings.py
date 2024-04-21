# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

race_results_list = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
.filter(f"file_date = '{v_file_date}'") \
.select("race_year") \
.distinct() \
.collect()

# COMMAND ----------

# race_results_list

# COMMAND ----------

race_year_list = []
for race_year in race_results_list:
    race_year_list.append(race_year.race_year)

print(race_year_list)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
.filter(col("race_year").isin(race_year_list))

# COMMAND ----------

#display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import sum, when, col, count, desc, rank

# COMMAND ----------

driver_standings = race_results_df \
.groupBy("race_year", "driver_name", "driver_nationality",) \
.agg(sum("points").alias("total_points"),
count(when(col("position")==1,True)).alias("wins")
)

# COMMAND ----------

#display(driver_standings.filter("race_year = 2020"))

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))

# COMMAND ----------

final_df = driver_standings.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

#display(final_df.filter("race_year = 2020"))

# COMMAND ----------

#We use the one below in order to be able to query the data using sql commands as well as python
#final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/driver_standings")

# COMMAND ----------

# if (spark._jsparkSession.catalog().tableExists("f1_presentation.driver_standings")):
#     for race_year in race_year_list:
#         spark.sql(f"ALTER TABLE f1_presentation.driver_standings DROP IF EXISTS PARTITION (race_year = {race_year})")

# COMMAND ----------

# final_df.write.mode("append").partitionBy("race_year").format("parquet").saveAsTable("f1_presentation.driver_standings")

# COMMAND ----------

merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_year = src.race_year"
merge_delta_data(final_df, "f1_presentation", "driver_standings", presentation_folder_path, merge_condition, "race_year")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM f1_presentation.driver_standings
# MAGIC WHERE race_year = 2021

# COMMAND ----------

# %sql
# DROP TABLE f1_presentation.driver_standings

# COMMAND ----------

#final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.driver_standings")