# Databricks notebook source
# MAGIC %md 
# MAGIC We will simulate a streaming data source by loading a dataframe and writing rows to our source file system using a sleep command. 
# MAGIC 
# MAGIC We will be using a sample IoT dataset. 

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.functions import monotonically_increasing_id
from time import sleep

df = (spark.read.format('json').load("/databricks-datasets/iot/iot_devices.json")
  .orderBy(col("timestamp"))
  .withColumn("id", monotonically_increasing_id()) )

# COMMAND ----------

display(df)

# COMMAND ----------

## for out simulation we will save our data frame to a table and collect batchs to write on a cadence
try:
  df = spark.sql("SELECT * FROM stream_data")
  print("Table Exists. Loaded Data.")
except:
  df.write.saveAsTable("stream_data")
  print("Table Created.")

# COMMAND ----------

current = 0
num_rows = df.count()
batch_size = 4000
frequency = 5

# COMMAND ----------

while current < num_rows:
  stream_df = spark.sql("select * from stream_data where id >= {} and id < {}".format(current, current+batch_size))
  stream_df.write.format("delta").mode("append").save("/mnt/delta/bronze/iot/stream_data")
  print("Streamed IDs between: {} and {}".format(current, current+batch_size-1))
  current = current+batch_size

  sleep(frequency)

# COMMAND ----------


