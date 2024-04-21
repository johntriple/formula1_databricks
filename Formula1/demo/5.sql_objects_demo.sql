-- Databricks notebook source
-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

--CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

--SHOW DATABASES;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

SELECT Current_database()

-- COMMAND ----------

use demo;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

DESCRIBE EXTENDED race_results_python;

-- COMMAND ----------

SELECT *
FROM demo.race_results_python
WHERE race_year=2020

-- COMMAND ----------

--CREATE TABLE demo.race_results_sql
--AS
--SELECT *
--FROM demo.race_results_python
--WHERE race_year=2020

-- COMMAND ----------

DESC EXTENDED demo.race_results_sql

-- COMMAND ----------

SHOW TABLES IN demo

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #race_results_df.write.format("parquet").option("path", f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

DESC EXTENDED demo.race_results_ext_py

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS demo.race_results_ext_sql
(
  race_year INT,
  race_name STRING,
  race_date TIMESTAMP,
  circuit_location STRING,
  driver_name STRING,
  driver_number INT,
  driver_nationality STRING,
  team STRING,
  grid INT,
  fastest_lap INT,
  race_time STRING,
  points FLOAT,
  position INT,
  created_date TIMESTAMP
)
USING PARQUET
LOCATION "/mnt/formula1dltriple/presentation/race_results_ext_sql"

-- COMMAND ----------

SHOW TABLES in demo

-- COMMAND ----------

INSERT INTO demo.race_results_ext_sql
SELECT * FROM demo.race_results_ext_py WHERE race_year = 2020;

-- COMMAND ----------

DROP TABLE demo.race_results_ext_sql

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_race_results
AS
SELECT *
FROM demo.race_results_python
WHERE race_year = 2018

-- COMMAND ----------

SELECT COUNT(1) FROM v_race_results

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW gv_race_results
AS
SELECT *
FROM demo.race_results_python
WHERE race_year = 2012

-- COMMAND ----------

SELECT * FROM global_temp.gv_race_results

-- COMMAND ----------

