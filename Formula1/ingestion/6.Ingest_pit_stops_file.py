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

pit_stop_schema=StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("stop", StringType(), True),
    StructField("lap", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("duration", StringType(), True),
    StructField("milliseconds", IntegerType(), True),
])

# COMMAND ----------

pit_stops_df = spark.read \
.schema(pit_stop_schema) \
.option("multiLine",True) \
.json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit

# COMMAND ----------

pit_stops_final_df = pit_stops_df.withColumnRenamed("raceId","race_id") \
                                 .withColumnRenamed("driverId","driver_id") \
                                 .withColumn("ingestion_date", current_timestamp()) \
                                 .withColumn("source", lit(v_data_source)) \
                                 .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

#display(pit_stops_final_df)

# COMMAND ----------

#pit_stops_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/pit_stops")

# COMMAND ----------

#pit_stops_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pit_stops")

# COMMAND ----------

# for race_id_list in pit_stops_final_df.select("race_id").distinct().collect():
#     if (spark._jsparkSession.catalog().tableExists("f1_processed.pit_stops")):
#         spark.sql(f"ALTER TABLE f1_processed.pit_stops DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

# pit_stops_final_df.write.mode("append").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.pit_stops")

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop AND tgt.race_id = src.race_id"
merge_delta_data(pit_stops_final_df, "f1_processed", "pit_stops", processed_folder_path, merge_condition, "race_id")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, count(1)
# MAGIC FROM f1_processed.pit_stops
# MAGIC GROUP BY race_id

# COMMAND ----------

dbutils.notebook.exit("Success")