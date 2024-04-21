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

from pyspark.sql.functions import sum, when, count, col, desc, rank

# COMMAND ----------

constructor_standings = race_results_df \
.groupBy("race_year", "team") \
.agg(sum("points").alias("total_points"),
count(when(col("position")==1,True)).alias("wins")
)

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))

# COMMAND ----------

final_df = constructor_standings.withColumn("rank", rank().over(constructor_rank_spec))

# COMMAND ----------

#display(final_df)

# COMMAND ----------

#we use the one below in order to be able to query data using sql commands
#final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/constructor_standings")

# COMMAND ----------

# if (spark._jsparkSession.catalog().tableExists("f1_presentation.constructor_standings")):
#     for race_year in race_year_list:
#         spark.sql(f"ALTER TABLE f1_presentation.constructor_standings DROP IF EXISTS PARTITION (race_year = {race_year})")

# COMMAND ----------

# final_df.write.mode("append").partitionBy("race_year").format("parquet").saveAsTable("f1_presentation.constructor_standings")

# COMMAND ----------

merge_condition = "tgt.team = src.team AND tgt.race_year = src.race_year"
merge_delta_data(final_df, "f1_presentation", "constructor_standings", presentation_folder_path, merge_condition, "race_year")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM f1_presentation.constructor_standings
# MAGIC WHERE race_year = 2021

# COMMAND ----------

# %sql
# DROP TABLE f1_presentation.constructor_standings

# COMMAND ----------

#final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.constructor_standings")