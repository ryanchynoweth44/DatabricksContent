# Databricks notebook source
import pandas as pd

# COMMAND ----------

# create a spark dataframe
df = pd.DataFrame([{"ColumnA": 1, "ColumnB": 3}, {"ColumnA": 11, "ColumnB": 31}])
df = spark.createDataFrame(df)
display(df)

# COMMAND ----------

# create delta table
df.write.format("delta").mode("overwrite").save("/mnt/deltalake/demo/demo_delta_table")

# COMMAND ----------

# create external hive table
spark.sql("CREATE TABLE default.demo_delta_table USING DELTA LOCATION '/mnt/deltalake/demo/demo_delta_table' ")

# COMMAND ----------

# create a second database and view using the previously created external table
spark.sql("CREATE DATABASE IF NOT EXISTS new_demo_database ")
spark.sql("CREATE VIEW new_demo_database.demo_view AS SELECT * FROM default.demo_delta_table ")

# COMMAND ----------

# Display the contents of our view
display( spark.sql("SELECT * FROM new_demo_database.demo_view ") )

# COMMAND ----------

# create a new dataframe
df = pd.DataFrame([{"ColumnA": 1123, "ColumnB": 3178}, {"ColumnA": -11, "ColumnB": 37891}])
df = spark.createDataFrame(df)
# append dataframe to existing delta table
df.write.format("delta").mode("append").save("/mnt/deltalake/demo/demo_delta_table")

# COMMAND ----------

# Display our delta table
display(spark.read.format("delta").load("/mnt/deltalake/demo/demo_delta_table"))

# COMMAND ----------

# Display the contents of our view
display( spark.sql("SELECT * FROM new_demo_database.demo_view ") )

# COMMAND ----------


