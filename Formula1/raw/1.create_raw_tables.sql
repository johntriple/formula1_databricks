-- Databricks notebook source
-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_raw

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Create circuits table

-- COMMAND ----------

use f1_raw

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.circuits(
  circuitId INT,
  circuitRef STRING,
  name STRING,
  location STRING,
  country STRING,
  lat DOUBLE,
  lng DOUBLE,
  alt INT,
  url STRING
)
USING csv
OPTIONS (path "/mnt/formula1dltriple/raw/circuits.csv", header  TRUE)

-- COMMAND ----------

SELECT * FROM f1_raw.circuits

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Races table

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.races(
  raceId INT,
  year INT,
  round INT,
  circuitId INT,
  name STRING,
  date DATE,
  time STRING,
  url STRING
)
USING csv
OPTIONS (path "/mnt/formula1dltriple/raw/races.csv", header  TRUE)

-- COMMAND ----------

SELECT * FROM f1_raw.races

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Constructors table single line json simple structure

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.constructors(
constructorId INT,
constructorRef STRING,
name STRING,
nationality STRING,
url STRING
)
USING json
OPTIONS (path "/mnt/formula1dltriple/raw/constructors.json")

-- COMMAND ----------

SELECT * FROM f1_raw.constructors

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Drivers table single line json complex structure

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.drivers(
  driverId INT,
  driverRef STRING,
  number INT,
  code STRING,
  name STRUCT<forename: STRING, surname: STRING>,
  dob DATE,
  nationality STRING,
  url STRING
)
USING json
OPTIONS (path "/mnt/formula1dltriple/raw/drivers.json")

-- COMMAND ----------

SELECT * FROM f1_raw.drivers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Results table single line json simple structure

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.results(
resultId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,
grid INT,
position INT,
positionText STRING,
positionOrder INT,
points FLOAT,
laps INT,
time STRING,
milliseconds INT,
fastestLap INT,
rank INT,
fastestLapTime STRING,
fastestLapSpeed FLOAT,
statusId STRING
)
USING json
OPTIONS (path "/mnt/formula1dltriple/raw/results.json")

-- COMMAND ----------

SELECT * FROM f1_raw.results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Pit stops table multi line json simple structure

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
raceId INT,
driverId INT,
stop STRING,
lap INT,
time STRING,
duration STRING,
milliseconds INT
)
USING json
OPTIONS (path "/mnt/formula1dltriple/raw/pit_stops.json", multiLine TRUE)

-- COMMAND ----------

SELECT * FROM f1_raw.pit_stops

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Create tables for list of files
-- MAGIC #

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Multiple csv files to create table lap_times

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
raceId INT,
driverId INT,
lap INT,
position INT,
time STRING,
milliseconds INT
)
USING csv
OPTIONS (path "/mnt/formula1dltriple/raw/lap_times")

-- COMMAND ----------

SELECT * FROM f1_raw.lap_times

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Create qualifying table using multiple multiLine json files

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
qualifyId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,
position STRING,
q1 STRING,
q2 STRING,
q3 STRING
)
USING json
OPTIONS (path "/mnt/formula1dltriple/raw/qualifying", multiLine TRUE)

-- COMMAND ----------

SELECT * FROM f1_raw.qualifying

-- COMMAND ----------

DESC EXTENDED f1_raw.qualifying

-- COMMAND ----------

