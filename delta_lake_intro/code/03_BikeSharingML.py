# Databricks notebook source
# MAGIC %md 
# MAGIC MLFlow Code Here. All the above is a step by step walk through, while the code below should be ran using MLFlow and the expirement we set up. 

# COMMAND ----------

import mlflow
from mlflow.tracking import MlflowClient
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
import datetime as dt
from pyspark.ml.feature import OneHotEncoder, VectorAssembler

# COMMAND ----------

# MAGIC %scala
# MAGIC val tags = com.databricks.logging.AttributionContext.current.tags
# MAGIC val username = tags.getOrElse(com.databricks.logging.BaseTagDefinitions.TAG_USER, java.util.UUID.randomUUID.toString.replace("-", ""))
# MAGIC spark.conf.set("com.databricks.demo.username", username)

# COMMAND ----------

client = MlflowClient()
exps = client.list_experiments()
exp = [s for s in exps if "/Users/{}/BikeSharing".format(spark.conf.get("com.databricks.demo.username")) in s.name][0]
exp_id = exp.experiment_id
artifact_location = exp.artifact_location
run = client.create_run(exp_id)
run_id = run.info.run_id 

# COMMAND ----------

mlflow.start_run(run_id)

# COMMAND ----------

try: 
  # load data
  df = spark.read.format('delta').load("/mnt/delta/silver/bikeSharing/hourly")
  
  # split data
  train_df, test_df = df.randomSplit([0.7, 0.3])

  # One Hot Encoding
  mnth_encoder = OneHotEncoder(inputCol="mnth", outputCol="encoded_mnth")
  hr_encoder = OneHotEncoder(inputCol="hr", outputCol="encoded_hr")
  weekday_encoder = OneHotEncoder(inputCol="weekday", outputCol="encoded_weekday")

  # set the training variables we want to use
  train_cols = ['encoded_mnth', 'encoded_hr', 'encoded_weekday', 'temp', 'hum']

  # convert cols to a single features col
  assembler = VectorAssembler(inputCols=train_cols, outputCol="features")

  # Set linear regression model
  lr = LinearRegression(featuresCol="features", labelCol="cnt")

  # Create pipeline
  pipeline = Pipeline(stages=[
    mnth_encoder,
    hr_encoder,
    weekday_encoder,
    assembler,
    lr
  ])

  # fit pipeline
  lrPipelineModel = pipeline.fit(train_df)

  # write model to datetime folder and latest folder
  lrPipelineModel.write().overwrite().save("{}/latest/bike_sharing_model.model".format(artifact_location))
  lrPipelineModel.write().overwrite().save("{}/year={}/month={}/day={}/bike_sharing_model.model".format(artifact_location, dt.datetime.utcnow().year, dt.datetime.utcnow().month, dt.datetime.utcnow().day))

  # write test predictions to datetime and lastest folder
  predictions = lrPipelineModel.transform(test_df)
  predictions.write.format("parquet").mode("overwrite").save("{}/latest/test_predictions.parquet".format(artifact_location))
  predictions.write.format("parquet").mode("overwrite").save("{}/year={}/month={}/day={}/test_predictions.parquet".format(artifact_location, dt.datetime.utcnow().year, dt.datetime.utcnow().month, dt.datetime.utcnow().day))

  # mlflow log evaluations
  evaluator = RegressionEvaluator(labelCol = "cnt", predictionCol = "prediction")

  mlflow.log_metric("mae", evaluator.evaluate(predictions, {evaluator.metricName: "mae"}))
  mlflow.log_metric("rmse", evaluator.evaluate(predictions, {evaluator.metricName: "rmse"}))
  mlflow.log_metric("r2", evaluator.evaluate(predictions, {evaluator.metricName: "r2"}))
  mlflow.set_tag("Model Path", "{}/year={}/month={}/day={}".format(artifact_location, dt.datetime.utcnow().year, dt.datetime.utcnow().month, dt.datetime.utcnow().day))
  
  mlflow.end_run(status="FINISHED")
  print("Model training finished successfully")
except Exception as e:
    mlflow.log_param("Error", str(e))
    mlflow.end_run(status="FAILED")
    print("Model training failed: {}".format(str(e)))

