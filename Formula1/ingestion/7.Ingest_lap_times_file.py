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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

lap_times_schema=StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("lap", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True),
])

# COMMAND ----------

lap_times_df = spark.read \
.schema(lap_times_schema) \
.csv(f"{raw_folder_path}/{v_file_date}/lap_times")

# COMMAND ----------

#display(lap_times_df)

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit

# COMMAND ----------

lap_times_final_df = lap_times_df.withColumnRenamed("raceId","race_id") \
                                 .withColumnRenamed("driverId","driver_id") \
                                 .withColumn("ingestion_date", current_timestamp()) \
                                 .withColumn("source", lit(v_data_source)) \
                                 .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

#lap_times_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/lap_times")

# COMMAND ----------

#lap_times_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.lap_times")

# COMMAND ----------

# for race_id_list in lap_times_final_df.select("race_id").distinct().collect():
#     if (spark._jsparkSession.catalog().tableExists("f1_processed.lap_times")):
#         spark.sql(f"ALTER TABLE f1_processed.lap_times DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

# lap_times_final_df.write.mode("append").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.lap_times")

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.lap = src.lap AND tgt.race_id = src.race_id"
merge_delta_data(lap_times_final_df, "f1_processed", "lap_times", processed_folder_path, merge_condition, "race_id")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, count(1)
# MAGIC FROM f1_processed.lap_times
# MAGIC GROUP BY race_id

# COMMAND ----------

dbutils.notebook.exit("Success")