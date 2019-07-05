# Databricks notebook source
from pyspark.sql.functions import col
# Read a stream from delta table
streamDF = (spark.readStream.format('delta').load("/mnt/delta/bronze/iot/stream_data")
  .withColumnRenamed("timestamp", "raw_timestamp")
  .withColumn("Timestamp", (col("raw_timestamp")/1000).cast("timestamp")) )
display(streamDF)

# COMMAND ----------

streamDF = (spark.readStream.format('delta').load("/mnt/delta/bronze/iot/stream_data")
  .withColumnRenamed("timestamp", "raw_timestamp")
  .withColumn("Timestamp", (col("raw_timestamp")/1000).cast("timestamp")) )

# COMMAND ----------

(streamDF
 .writeStream
 .format("delta")
 .option("checkpointLocation", "/delta/checkpoints/iot/stream_data") #allows us to pick up where we left off if we lose connectivity
 .outputMode("append")
 .start("/mnt/delta/silver/iot/stream_data") )

# COMMAND ----------


