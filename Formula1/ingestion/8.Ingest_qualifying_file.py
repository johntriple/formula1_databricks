# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "testing")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

qualifying_schema=StructType(fields=[
    StructField("qualifyId", IntegerType(), False),
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("constructorId", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("q1", StringType(), True),
    StructField("q2", StringType(), True),
    StructField("q3", StringType(), True)
])

# COMMAND ----------

qualifying_df = spark.read \
.schema(qualifying_schema) \
.option("multiLine",True) \
.json(f"{raw_folder_path}/{v_file_date}/qualifying")

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit

# COMMAND ----------

qualifying_final_df = qualifying_df.withColumnRenamed("raceId","race_id") \
                                   .withColumnRenamed("driverId","driver_id") \
                                   .withColumnRenamed("qualifyId","qualify_id") \
                                   .withColumnRenamed("constructorId","constructor_id") \
                                   .withColumn("ingestion_date", current_timestamp()) \
                                   .withColumn("source", lit(v_data_source)) \
                                   .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

#display(qualifying_final_df)

# COMMAND ----------

#qualifying_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/qualifying")

# COMMAND ----------

#qualifying_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")

# COMMAND ----------

# for race_id_list in qualifying_final_df.select("race_id").distinct().collect():
#     if (spark._jsparkSession.catalog().tableExists("f1_processed.qualifying")):
#         spark.sql(f"ALTER TABLE f1_processed.qualifying DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

# qualifying_final_df.write.mode("append").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.qualifying")

# COMMAND ----------

merge_condition = "tgt.qualify_id = src.qualify_id AND tgt.race_id = src.race_id"
merge_delta_data(qualifying_final_df, "f1_processed", "qualifying", processed_folder_path, merge_condition, "race_id")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, count(1)
# MAGIC FROM f1_processed.qualifying
# MAGIC GROUP BY race_id

# COMMAND ----------

#display(spark.read.parquet(f"{processed_folder_path}/qualifying"))

# COMMAND ----------

dbutils.notebook.exit("Success")