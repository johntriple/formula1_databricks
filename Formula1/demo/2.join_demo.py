# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits") \
.filter("circuit_id < 70")

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races").filter("race_year = 2019")

# COMMAND ----------

#display(circuits_df)

# COMMAND ----------

#display(races_df)

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "inner") \
.select(circuits_df.name.alias("circuit_name"), circuits_df.location, circuits_df.race_country.alias("country"), races_df.name.alias("race_name"), races_df.round)

# COMMAND ----------

#display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Outer joins

# COMMAND ----------

#Left Outer join
race_circuits_left_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "left") \
.select(circuits_df.name.alias("circuit_name"), circuits_df.location, circuits_df.race_country.alias("country"), races_df.name.alias("race_name"), races_df.round)

# COMMAND ----------

#display(race_circuits_left_df)

# COMMAND ----------

#Right Outer join
race_circuits_right_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "right") \
.select(circuits_df.name.alias("circuit_name"), circuits_df.location, circuits_df.race_country.alias("country"), races_df.name.alias("race_name"), races_df.round)

# COMMAND ----------

#display(race_circuits_right_df)

# COMMAND ----------

#Full Outer join
race_circuits_full_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "full") \
.select(circuits_df.name.alias("circuit_name"), circuits_df.location, circuits_df.race_country.alias("country"), races_df.name.alias("race_name"), races_df.round)

# COMMAND ----------

#display(race_circuits_full_df)

# COMMAND ----------

#Anti join
#It returns the left df records that have no match on the right df
race_circuits_anti_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "anti")

# COMMAND ----------

#display(race_circuits_anti_df)

# COMMAND ----------

