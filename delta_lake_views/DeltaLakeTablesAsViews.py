# Databricks notebook source
# import pandas for easy data creation
import pandas as pd
from pyspark.sql.functions import col
# create a spark dataframe
df = pd.DataFrame([{"ColumnA": 1, "ColumnB": 3}, {"ColumnA": 11, "ColumnB": 31}])
df = spark.createDataFrame(df)
display(df)

# COMMAND ----------

# create delta table
df.write.format("delta").mode("overwrite").save("/mnt/deltalake/gold/demosource/Tables/demo_delta_table")

# COMMAND ----------

# create our delta "view"
df.filter(col("ColumnA") >= 5).write.format("delta").mode("overwrite").save("/mnt/deltalake/gold/demosource/Views/demo_delta_view")

# COMMAND ----------

streamDF = spark.readStream.format('delta').load("/mnt/deltalake/gold/demosource/Tables/demo_delta_table")

# COMMAND ----------

display(streamDF)


# COMMAND ----------

(streamDF.filter(col("ColumnA") >= 5)
.writeStream
.format("delta")
.option("checkpointLocation", "/delta/checkpoints/view_demo/streaming_demo_view") #allows us to pick up where we left off if we lose connectivity
.outputMode("append") # appends data to our table
.start("/mnt/deltalake/gold/demosource/Views/demo_delta_view") ) 

# COMMAND ----------

display(spark.read.format("delta").load("/mnt/deltalake/gold/demosource/Views/demo_delta_view"))


# COMMAND ----------

df = pd.DataFrame([{"ColumnA": 1, "ColumnB": 30}, {"ColumnA": 55, "ColumnB": 80}])
df = spark.createDataFrame(df)
df.write.format("delta").mode("append").save("/mnt/deltalake/gold/demosource/Tables/demo_delta_table")
