-- Databricks notebook source
SELECT * FROM f1_presentation.calculated_race_results

-- COMMAND ----------

SELECT team_name,
SUM(calculated_points) as total_points,
COUNT(1) as total_races,
AVG(calculated_points) as avg_points
FROM f1_presentation.calculated_race_results
GROUP BY team_name
HAVING total_races >= 100
ORDER BY total_points desc

-- COMMAND ----------

SELECT team_name,
SUM(calculated_points) as total_points,
COUNT(1) as total_races,
AVG(calculated_points) as avg_points
FROM f1_presentation.calculated_race_results
WHERE race_year BETWEEN 2011 and 2020
GROUP BY team_name
HAVING total_races >= 100
ORDER BY total_points desc

-- COMMAND ----------

