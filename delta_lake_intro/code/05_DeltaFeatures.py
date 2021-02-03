# Databricks notebook source
# MAGIC %md
# MAGIC Upserting hourly batch bikesharing data
# MAGIC 1. Create an intial gold table that is missing the most recent four hours
# MAGIC 1. Upsert with the entire dataset 

# COMMAND ----------

# Read in our data
df = spark.read.format('delta').load("/mnt/delta/silver/bikeSharing/hourly")

# COMMAND ----------

# subtract a few hours from the max date so we can filter
from datetime import timedelta
max_datetime = df.agg({"dteday": "max"}).collect()[0][0]-timedelta(hours=4)
max_datetime

# COMMAND ----------

# Summarize data, filter to keep away the most recent 2 hours, write the data
from pyspark.sql.functions import col, unix_timestamp
from pyspark.sql.types import DateType
summary_df = (df 
              .withColumn("datetime", (unix_timestamp(col("dteday"))+col("hr")*3600).cast("timestamp")) # we create a datetime column to filter on using the hr column and dteday column
              .filter(col("datetime") < max_datetime)
              .select("dteday", "cnt")
              .groupBy("dteday")
              .sum("cnt")
              .select(col("dteday").alias("date"), col("sum(cnt)").alias("cnt")))

# COMMAND ----------

# display, and use the html table to sort and see '2012-12-30'
display(summary_df)

# COMMAND ----------

# Write the data
summary_df.write.format("delta").save("/mnt/delta/gold/bikeSharing/daily_summar_for_upsert")

# COMMAND ----------

# MAGIC %md
# MAGIC Quickly see how we are able to enforce a schema with Delta!
# MAGIC 
# MAGIC Notice the following:
# MAGIC - Line 11: Cast the date column to a date type (was a timestamp)
# MAGIC - Line 13: We are overwriting the delta table (as opposed to appending)

# COMMAND ----------

#### This should throw an Error
## See the schema enforcement
# Summarize data, filter to keep away the most recent 2 hours, write the data
from pyspark.sql.functions import col, unix_timestamp
from pyspark.sql.types import DateType
summary_df = (df 
              .withColumn("datetime", (unix_timestamp(col("dteday"))+col("hr")*3600).cast("timestamp"))
              .filter(col("datetime") < max_datetime)
              .select("dteday", "cnt")
              .groupBy("dteday")
              .sum("cnt")
              .select(col("dteday").alias("date").cast("date"), col("sum(cnt)").alias("cnt")))

summary_df.write.format("delta").mode("overwrite").save("/mnt/delta/gold/bikeSharing/daily_summar_for_upsert")

# COMMAND ----------

# MAGIC %md 
# MAGIC Lets do an upsert

# COMMAND ----------

# Reread the data, summarize and upsert
whole_df = spark.read.format('delta').load("/mnt/delta/silver/bikeSharing/hourly")

upsert_df = (whole_df
              .withColumn("date", col("dteday").cast(DateType()))
              .select("date", "cnt")
              .groupBy("date")
              .sum("cnt")
              .select(col("date"), col("sum(cnt)").alias("cnt")))

# register as a temp table
upsert_df.registerTempTable("bike_upsert")

# COMMAND ----------

# display to show diff, filter to see the '2012-12-31' row is added to the whole dataframe and the '2012-12-30' is a different number
display(upsert_df)

# COMMAND ----------

# register delta table in our database. Open the "Data" tab and you will see it in the default database. 
spark.sql("""
  DROP TABLE IF EXISTS bike_counts
""")

spark.sql("""
  CREATE TABLE bike_counts
  USING DELTA
  LOCATION '{}'
""".format("/mnt/delta/gold/bikeSharing/daily_summar_for_upsert"))

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC MERGE INTO bike_counts
# MAGIC USING bike_upsert
# MAGIC ON bike_counts.date = bike_upsert.date
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET cnt = bike_upsert.cnt
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *

# COMMAND ----------

# Load and display delta table to see that it was updated appropriately
display(spark.sql("select * from bike_counts"))

# COMMAND ----------

# MAGIC %md
# MAGIC Now lets check out the time travel feature! 
# MAGIC 
# MAGIC The delta table that we just created has two versions available. Lets check out both versions. 

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE HISTORY bike_counts

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM bike_counts
# MAGIC VERSION AS OF 0

# COMMAND ----------

# MAGIC %md 
# MAGIC You can even query and compare between the different versions of the table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) - (
# MAGIC   SELECT count(*)
# MAGIC   FROM bike_counts
# MAGIC   VERSION AS OF 0 ) AS new_entries
# MAGIC FROM bike_counts
