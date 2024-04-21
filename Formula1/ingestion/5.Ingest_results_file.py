# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType

# COMMAND ----------

results_schema=StructType(fields=[
    StructField("resultId", IntegerType(), False),
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("constructorId", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("grid", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("positionText", StringType(), True),
    StructField("positionOrder", IntegerType(), True),
    StructField("points", FloatType(), True),
    StructField("laps", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True),
    StructField("fastestLap", IntegerType(), True),
    StructField("rank", IntegerType(), True),
    StructField("fastestLapTime", StringType(), True),
    StructField("fastestLapSpeed", FloatType(), True),
    StructField("statusId", StringType(), True)
])

# COMMAND ----------

results_df = spark.read \
.schema(results_schema) \
.json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

#display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Rename and add columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,col, lit

# COMMAND ----------

results_with_columns_renamed_df = results_df.withColumnRenamed("resultId","result_id") \
                                            .withColumnRenamed("raceId","race_id") \
                                            .withColumnRenamed("driverId","driver_id") \
                                            .withColumnRenamed("constructorId","constructor_id") \
                                            .withColumnRenamed("positionText","position_text") \
                                            .withColumnRenamed("positionOrder","position_order") \
                                            .withColumnRenamed("fastestLap","fastest_lap") \
                                            .withColumnRenamed("fastestLapTime","fastest_lap_time") \
                                            .withColumnRenamed("fastestLapSpeed","fastest_lap_speed") \
                                            .withColumn("ingestion_date", current_timestamp()) \
                                            .withColumn("source", lit(v_data_source)) \
                                            .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

results_final_df = results_with_columns_renamed_df.drop("statusId")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Deduplicate the results dataframe

# COMMAND ----------

results_deduped_df = results_final_df.dropDuplicates(["race_id", "driver_id"])

# COMMAND ----------

#results_final_df.write.mode("overwrite").partitionBy("race_id").parquet(f"{processed_folder_path}/results")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Method 1

# COMMAND ----------

# for race_id_list in results_final_df.select("race_id").distinct().collect():
#     if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#         spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

# results_final_df.write.mode("append").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
merge_delta_data(results_deduped_df, "f1_processed", "results", processed_folder_path, merge_condition, "race_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Method 2

# COMMAND ----------

# spark.conf.set("spark.sql.sources.paritionOverwriteMode", "dynamic")

# COMMAND ----------

# #race_id(the column we partitionBy) needs to be the last column so this select is necessary to change the order
# results_final_df = results_final_df.select("result_id", "driver_id", "constructor_id", "number", "grid", "position", "position_text", "position_order", "points",
#                                            "laps", "time", "milliseconds", "fastest_lap", "rank", "fastest_lap_time", "fastest_lap_speed", "ingestion_date",
#                                            "source", "file_date", "race_id")

# COMMAND ----------

# if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#     results_final_df.write.mode("overwrite").insertInto("f1_processed.results")
# else:
#     results_final_df.write.mode("overwrite").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# %sql
# DROP TABLE f1_processed.results

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, count(1)
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id

# COMMAND ----------

# %sql
# SELECT race_id, driver_id,COUNT(1)
# FROM f1_processed.results
# GROUP BY race_id, driver_id
# HAVING COUNT(1) > 1
# ORDER BY race_id, driver_id DESC

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

