-- Databricks notebook source
USE f1_processed

-- COMMAND ----------

SELECT *, concat(driver_ref, "-", code ) AS new_driver_ref
FROM drivers

-- COMMAND ----------

SELECT *, split(name, " ")[0] forename, split(name, " ")[1] surname
FROM drivers

-- COMMAND ----------

SELECT *, date_format(dob, "dd-MM-yyyy") as normal_format
FROM drivers

-- COMMAND ----------

SELECT nationality, count(*) as number_of_drivers
FROM drivers
GROUP BY nationality
HAVING count(*) > 50
ORDER BY nationality

-- COMMAND ----------

SELECT nationality, name, dob, rank() OVER (PARTITION BY nationality ORDER BY dob DESC) AS age_rank
FROM drivers
ORDER BY nationality, age_rank

-- COMMAND ----------

use f1_presentation

-- COMMAND ----------

desc driver_standings

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_driver_standings_2018
AS
SELECT race_year, driver_name, team, total_points, wins, rank
FROM driver_standings
WHERE race_year = 2018

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_driver_standings_2020
AS
SELECT race_year, driver_name, team, total_points, wins, rank
FROM driver_standings
WHERE race_year = 2020

-- COMMAND ----------

SELECT *
FROM v_driver_standings_2018

-- COMMAND ----------

SELECT *
FROM v_driver_standings_2020

-- COMMAND ----------

SELECT * 
FROM v_driver_standings_2018 d_2018
INNER JOIN v_driver_standings_2020 d_2020
ON d_2018.driver_name = d_2020.driver_name

-- COMMAND ----------

