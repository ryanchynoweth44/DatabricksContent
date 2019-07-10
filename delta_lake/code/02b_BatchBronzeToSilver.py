# Databricks notebook source
# MAGIC %md
# MAGIC Load hourly and daily batch data from bronze tables

# COMMAND ----------

# Load data from Bronze
hourly_df = spark.read.format("delta").load("/mnt/delta/bronze/bikeSharing/hourly")

daily_df = spark.read.format("delta").load("/mnt/delta/bronze/bikeSharing/daily")

# COMMAND ----------

from pyspark.sql.functions import col, when

hourly_df = hourly_df.withColumn("Windy", when(col("windspeed") > 0, 1).otherwise(0))
daily_df = daily_df.withColumn("Windy", when(col("windspeed") > 0, 1).otherwise(0))

# COMMAND ----------

display(hourly_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Write data to delta

# COMMAND ----------

# write data to silver
hourly_df.write.format("delta").mode("overwrite").save("/mnt/delta/silver/bikeSharing/hourly")

daily_df.write.format("delta").mode("overwrite").save("/mnt/delta/silver/bikeSharing/daily")

# COMMAND ----------

hourly_df.write.format("delta").partitionBy("dteday").mode("append").save("/mnt/delta/silver/bikeSharing_partitioned/hourly")

# COMMAND ----------


