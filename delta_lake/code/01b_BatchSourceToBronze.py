# Databricks notebook source
# MAGIC %md 
# MAGIC We will do a one time batch load of the bike sharing dataset

# COMMAND ----------

# sample data sources
dbutils.fs.ls('/databricks-datasets/bikeSharing/data-001')

# COMMAND ----------

# load hour data and display
hourly_df = (spark
.read
.option("header", True)
.option("inferSchema", True)
.csv("/databricks-datasets/bikeSharing/data-001/hour.csv"))

display(hourly_df)

# COMMAND ----------

# write hourly data to source
hourly_df.write.format("delta").mode("overwrite").save("/mnt/delta/bronze/bikeSharing/hourly")

# COMMAND ----------

# load daily data and display
daily_df = (spark
.read
.option("header", True)
.option("inferSchema", True)
.csv("/databricks-datasets/bikeSharing/data-001/day.csv"))

display(daily_df)

# COMMAND ----------

# write daily data to source
daily_df.write.format("delta").mode("overwrite").save("/mnt/delta/bronze/bikeSharing/daily")
