-- Databricks notebook source
--SELECT * FROM f1_presentation.calculated_race_results

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_drivers
AS
SELECT driver_name,
SUM(calculated_points) as total_points,
COUNT(1) as total_races,
AVG(calculated_points) as avg_points,
RANK() OVER (ORDER BY AVG(calculated_points) DESC) as driver_rank
FROM f1_presentation.calculated_race_results
GROUP BY driver_name
HAVING total_races >= 50
ORDER BY  avg_points desc

-- COMMAND ----------

SELECT * FROM v_dominant_drivers

-- COMMAND ----------

SELECT driver_name,
race_year,
SUM(calculated_points) as total_points,
COUNT(1) as total_races,
AVG(calculated_points) as avg_points
FROM f1_presentation.calculated_race_results
WHERE driver_name IN (SELECT driver_name FROM v_dominant_drivers WHERE driver_rank <= 10)
GROUP BY driver_name, race_year
ORDER BY race_year desc, avg_points desc

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_teams
AS
SELECT team_name,
SUM(calculated_points) as total_points,
COUNT(1) as total_races,
AVG(calculated_points) as avg_points,
RANK() OVER (ORDER BY AVG(calculated_points) DESC) as team_rank
FROM f1_presentation.calculated_race_results
GROUP BY team_name
HAVING total_races >= 100
ORDER BY  avg_points desc

-- COMMAND ----------

SELECT * FROM v_dominant_teams

-- COMMAND ----------

SELECT team_name,
race_year,
SUM(calculated_points) as total_points,
COUNT(1) as total_races,
AVG(calculated_points) as avg_points
FROM f1_presentation.calculated_race_results
WHERE team_name IN (SELECT team_name FROM v_dominant_teams WHERE team_rank <= 5)
GROUP BY team_name, race_year
ORDER BY race_year desc, avg_points desc

-- COMMAND ----------

