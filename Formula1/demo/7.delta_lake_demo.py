# Databricks notebook source
# MAGIC %md
# MAGIC ####Write and read data from a data lake(external and managed tables)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_demo
# MAGIC LOCATION "/mnt/formula1dltriple/demo"

# COMMAND ----------

results_df = spark.read \
.option("inferSchema", True) \
.json("/mnt/formula1dltriple/raw/2021-03-28/results.json")

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").saveAsTable("demo.results_managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM demo.results_managed

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").save("/mnt/formula1dltriple/demo/results_external")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS demo.results_external
# MAGIC USING DELTA
# MAGIC LOCATION "/mnt/formula1dltriple/demo/results_external"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM demo.results_external

# COMMAND ----------

results_external_df = spark.read.format("delta").load("/mnt/formula1dltriple/demo/results_external")

# COMMAND ----------

display(results_external_df)

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").partitionBy("constructorId").saveAsTable("demo.results_partitioned")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW PARTITIONS demo.results_partitioned

# COMMAND ----------

# MAGIC %md
# MAGIC ####Update and deletes on a delta lake

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM demo.results_managed

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE demo.results_managed
# MAGIC SET points = 11 - position
# MAGIC WHERE position <= 10

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM demo.results_managed
# MAGIC WHERE points = 0

# COMMAND ----------

# MAGIC %md
# MAGIC ####Upsert using merge

# COMMAND ----------

# MAGIC %sql
# MAGIC use demo

# COMMAND ----------

drivers_day1_df = spark.read \
.option("inferSchema", True) \
.json("/mnt/formula1dltriple/raw/2021-03-28/drivers.json") \
.filter("driverId <=10") \
.select("driverId", "dob", "name.forename", "name.surname")

# COMMAND ----------

#display(drivers_day1_df)

# COMMAND ----------

from pyspark.sql.functions import upper

# COMMAND ----------

drivers_day2_df = spark.read \
.option("inferSchema", True) \
.json("/mnt/formula1dltriple/raw/2021-03-28/drivers.json") \
.filter("driverId BETWEEN 6 AND 15") \
.select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

#display(drivers_day2_df)

# COMMAND ----------

drivers_day3_df = spark.read \
.option("inferSchema", True) \
.json("/mnt/formula1dltriple/raw/2021-03-28/drivers.json") \
.filter("driverId BETWEEN 1 AND 5 OR driverId BETWEEN 16 AND 20") \
.select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

#display(drivers_day3_df)

# COMMAND ----------

drivers_day1_df.createOrReplaceTempView("drivers_day1")

# COMMAND ----------

drivers_day2_df.createOrReplaceTempView("drivers_day2")

# COMMAND ----------

drivers_day3_df.createOrReplaceTempView("drivers_day3")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS demo.drivers_merge(
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING,
# MAGIC surname STRING,
# MAGIC createdDate DATE,
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO demo.drivers_merge tgt
# MAGIC USING drivers_day1 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET tgt.dob=upd.dob,
# MAGIC            tgt.forename=upd.forename,
# MAGIC            tgt.surname=upd.surname,
# MAGIC            tgt.updatedDate=current_timestamp
# MAGIC WHEN NOT MATCHED 
# MAGIC THEN INSERT (driverId, dob, forename, surname, createdDate) VALUES (driverId, dob, forename, surname, current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO demo.drivers_merge tgt
# MAGIC USING drivers_day2 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET tgt.dob=upd.dob,
# MAGIC            tgt.forename=upd.forename,
# MAGIC            tgt.surname=upd.surname,
# MAGIC            tgt.updatedDate=current_timestamp
# MAGIC WHEN NOT MATCHED 
# MAGIC THEN INSERT (driverId, dob, forename, surname, createdDate) VALUES (driverId, dob, forename, surname, current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC ####Same thing using pyspark instead of sql for demonstration purposes

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# deltaTable = DeltaTable.forPath(spark, "dbfs:/user/hive/warehouse/demo.db/drivers_merge")

# deltaTable.alias("tgt").merge(
#     drivers_day3_df.alias("upd"),
#     "tgt.driverId = upd.driverId") \
# .whenMatchedUpdate(set = {"dob" : "upd.dob", "forename":"upd.forename", "surname" : "upd.surname", "updatedDate" : "current_timestamp()"}) \
# .whenNotMatchedInsert(
#     values={
#         "driverId" : "upd.driverId",
#         "dob":"upd.dob",
#         "forename":"upd.forename",
#         "surname":"upd.surname",
#         "createdDate":"current_timestamp()"
#     }
# ) \
# .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO demo.drivers_merge tgt
# MAGIC USING drivers_day3 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET tgt.dob=upd.dob,
# MAGIC            tgt.forename=upd.forename,
# MAGIC            tgt.surname=upd.surname,
# MAGIC            tgt.updatedDate=current_timestamp
# MAGIC WHEN NOT MATCHED 
# MAGIC THEN INSERT (driverId, dob, forename, surname, createdDate) VALUES (driverId, dob, forename, surname, current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC ####History and time travel

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM demo.drivers_merge VERSION AS OF 1

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO  demo.drivers_merge tgt
# MAGIC USING demo.drivers_merge VERSION AS OF 3 src
# MAGIC ON (tgt.driverId=src.driverId)
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC ####Transaction logs

# COMMAND ----------

# MAGIC %sql
# MAGIC USE demo

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS demo.drivers_txn(
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING,
# MAGIC surname STRING,
# MAGIC createdDate DATE,
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY demo.drivers_txn

# COMMAND ----------

# MAGIC %md
# MAGIC ####Convert parquet to delta

# COMMAND ----------

# MAGIC %sql
# MAGIC USE demo;
# MAGIC CREATE TABLE IF NOT EXISTS demo.convert_to_delta(
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING,
# MAGIC surname STRING,
# MAGIC createdDate DATE,
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING PARQUET

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO demo.convert_to_delta
# MAGIC SELECT * FROM demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA demo.convert_to_delta

# COMMAND ----------

# %sql
# VACUUM demo.convert_to_delta

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY demo.convert_to_delta

# COMMAND ----------

df = spark.table("demo.convert_to_delta")

# COMMAND ----------

df.write.format("parquet").save("/mnt/formula1dltriple/demo/drivers_convert_delta_new")

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA parquet.`/mnt/formula1dltriple/demo/drivers_convert_delta_new`

# COMMAND ----------

