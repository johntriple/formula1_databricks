# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

name_schema = StructType(fields=[
    StructField("forename", StringType(), True),
    StructField("surname", StringType(), True)
])

# COMMAND ----------

driver_schema = StructType(fields=[
    StructField("driverId", IntegerType(), False),
    StructField("driverRef", StringType(), True),
    StructField("number", IntegerType(), True),
    StructField("code", StringType(), True),
    StructField("name", name_schema),
    StructField("dob", DateType(), True),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

drivers_df = spark.read \
.schema(driver_schema) \
.json(f"{raw_folder_path}/{v_file_date}/drivers.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ####rename and drop unwanted columns

# COMMAND ----------

from pyspark.sql.functions import col, concat, lit, current_timestamp

# COMMAND ----------

drivers_with_columns = drivers_df.withColumnRenamed("driverId","driver_id") \
                                 .withColumnRenamed("driverRef","driver_ref") \
                                 .withColumn("ingestion_date", current_timestamp()) \
                                 .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname"))) \
                                 .withColumn("source", lit(v_data_source)) \
                                 .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Drop unwanted columns

# COMMAND ----------

drivers_final_df = drivers_with_columns.drop("url")

# COMMAND ----------

#display(drivers_final_df)

# COMMAND ----------

#drivers_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/drivers")

# COMMAND ----------

drivers_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.drivers

# COMMAND ----------

dbutils.notebook.exit("Success")