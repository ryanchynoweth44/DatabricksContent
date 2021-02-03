# Databricks notebook source
from pyspark.ml import PipelineModel

# COMMAND ----------

# load model
lrModel = PipelineModel.load("/mnt/mlflow/bikesharing/latest/bike_sharing_model.model")

# COMMAND ----------

# load data
df = spark.read.format('delta').load("/mnt/delta/silver/bikeSharing/hourly")

# COMMAND ----------

# score data
score = lrModel.transform(df)

# COMMAND ----------

# write data
score.write.format("delta").mode("overwrite").save("/mnt/delta/gold/bikeSharing/hourly")

# COMMAND ----------


