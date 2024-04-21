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

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read \
.schema(constructors_schema) \
.json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Drop unwanted column

# COMMAND ----------

from pyspark.sql.functions import col,current_timestamp, lit

# COMMAND ----------

constructor_dropped_df = constructor_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Rename columns and add ingestion date

# COMMAND ----------

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId","constructor_id") \
                                             .withColumnRenamed("constructorRef","constructor_ref") \
                                             .withColumn("ingestion_date",current_timestamp()) \
                                             .withColumn("source", lit(v_data_source)) \
                                             .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

#display(constructor_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####write data to datalake

# COMMAND ----------

#constructor_final_df = constructor_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/constructors")

# COMMAND ----------

constructor_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.constructors

# COMMAND ----------

dbutils.notebook.exit("Success")