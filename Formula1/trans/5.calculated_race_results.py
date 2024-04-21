# Databricks notebook source
# %sql
# USE f1_processed

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

spark.sql(f"""
          CREATE TABLE IF NOT EXISTS f1_presentation.calculated_race_results
          (
             race_year INT,
             team_name STRING,
             driver_id INT,
             driver_name STRING,
             race_id INT,
             position INT,
             points INT,
             calculated_points INT,
             created_date DATE,
             updated_date DATE
          )
          USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
                CREATE OR REPLACE TEMP VIEW race_results_updated
                AS
                SELECT races.race_year,
                constructors.name as team_name,
                drivers.driver_id,
                drivers.name as driver_name,
                races.race_id,
                results.position,
                results.points,
                11 - results.position as calculated_points
                FROM f1_processed.results
                INNER JOIN f1_processed.drivers ON results.driver_id = drivers.driver_id
                INNER JOIN f1_processed.constructors ON results.constructor_id = constructors.constructor_id
                INNER JOIN f1_processed.races ON results.race_id = races.race_id
                WHERE results.position <= 10 AND results.file_date= '{v_file_date}'
""")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_presentation.calculated_race_results tgt
# MAGIC USING race_results_updated upd
# MAGIC ON tgt.driver_id = upd.driver_id AND tgt.race_id = upd.race_id
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET tgt.position = upd.position,
# MAGIC            tgt.points = upd.points,
# MAGIC            tgt.calculated_points = upd.calculated_points,
# MAGIC            tgt.updated_date = current_timestamp
# MAGIC WHEN NOT MATCHED 
# MAGIC THEN INSERT (race_year, team_name, driver_id, driver_name, race_id, position, points, calculated_points, created_date) 
# MAGIC      VALUES (race_year, team_name, driver_id, driver_name, race_id, position, points, calculated_points, current_timestamp)

# COMMAND ----------

# %sql
# CREATE TABLE f1_presentation.calculated_race_results
# USING parquet
# AS
# SELECT races.race_year,
# constructors.name as team_name,
# drivers.name as driver_name,
# results.position, results.points,
# 11 - position as calculated_points
# FROM f1_processed.results
# INNER JOIN f1_processed.drivers ON results.driver_id = drivers.driver_id
# INNER JOIN f1_processed.constructors ON results.constructor_id = constructors.constructor_id
# INNER JOIN f1_processed.races ON results.race_id = races.race_id
# WHERE results.position <= 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.calculated_race_results

# COMMAND ----------

