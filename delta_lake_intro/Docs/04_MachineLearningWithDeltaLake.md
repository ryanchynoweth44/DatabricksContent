# Machine Learning with Delta Lake

So far we have created two data pipelines using the Bike Sharing dataset and an IoT dataset. In both cases, we do not have much data cleansing our joining to other datasets, therefore, we will quickly train a machine learning model that we will apply to our bike sharing datasets as we transform the data from silver to gold.  

Typically data scientists would base their solution off datasets that reside in our gold tables, however, I believe that at times gold data can be summarized at too high of a level and silver tables (which are typically more granular) should be open for predictive analytic consumption. 

Please note that we are leaving the streaming data in silver for this demo, since there is no other real data transformations we want to apply. This machine learning scenario should give you insight into the pattern that you apply delta lake and the flexible structure that it gives developers to query big data.  

## Bike Sharing Model
We will use our hourly bike sharing data to make demand predictions `cnt` using a very simple linear regression model. Databricks provides a great example of tracking and training [PySpark models with MLFlow on Databricks](https://docs.azuredatabricks.net/applications/mlflow/tracking-examples.html#train-a-pyspark-model-and-save-in-mleap-format), but we will provide a similar (less complex) example here.   

1. Create a library with Source PyPI and enter `mlflow`.

1. Next create and install a library with Source Maven Coordinate and Coordinates `ml.combust.mleap:mleap-spark_2.11:0.13.0`. Make sure both liraries are attached to your Databricks cluster.  

1. Create a new MLFlow Experiment, and provide a name ("BikeSharing") and an Artifact Location on DBFS ("dbfs:/mnt/mlflow/bikesharing").

1. Create a new Python notebook called [`03_BikeSharingML`](../code/03_BikeSharingML.py), and import the following libraries. 
    ```python
    import mlflow
    from mlflow.tracking import MlflowClient
    from pyspark.ml.regression import LinearRegression
    from pyspark.ml.evaluation import RegressionEvaluator
    from pyspark.ml import Pipeline
    import datetime as dt
    from pyspark.ml.feature import OneHotEncoder, VectorAssembler
    ``` 

1. By default when you create an MLFlow experiment it will be created in your User Root directory. Typically, I would recommend moving the experiment (and libraries) to their own organized folders, but for now we will leave it where it is. To programmatically get your username and format the path to the experiment run the following scala code.  
    ```scala
    %scala
    val tags = com.databricks.logging.AttributionContext.current.tags
    val username = tags.getOrElse(com.databricks.logging.BaseTagDefinitions.TAG_USER, java.util.UUID.randomUUID.toString.replace("-", ""))
    spark.conf.set("com.databricks.demo.username", username)
    ```

1. We will use a combination of the MLflowClient and the built in mlflow library we loaded previously. Create a run with the following code.  
    ```python
    client = MlflowClient() # client
    exps = client.list_experiments() # get all experiments
    exp = [s for s in exps if "/Users/{}/BikeSharing".format(spark.conf.get("com.databricks.demo.username")) in s.name][0] # get only the exp we want
    exp_id = exp.experiment_id # save exp id to variable
    artifact_location = exp.artifact_location # artifact location for storing
    run = client.create_run(exp_id) # create the run
    run_id = run.info.run_id # get the run id
    ```

1. Start the mlflow run.  
    ```python
    mlflow.start_run(run_id)
    ```

1. We will use the following code to train a machine learning model. See the inline comments for broad steps in what we are doing. The try/except allows us to end a run with a failure message.    
    ```python
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
    ```

Few things to note with the above code:
- We log all of our metrics so that we can easily track how our model is performing over time. 
- We set tags to our run to the location of our model. 
- We log an error message as a parameter if the run fails. 
- We write our model and test predictions to two folders: a datetime folder and a latest folder. The datetime folder allows us save a history of all our models and the latest folder allows us to easily pick up the model and use it in a batch prediction notebook which we will use in the [next portion of the demo](./05_SilverToGold.md).   
- We have a single mount location for all our mlflow experiments `/mnt/mlflow`. 
- The `.model` file extension when we save our models are actually not neccessary, I simply use them to mark the directory that I need to load when I want to use the model.  


Move onto the next portion of the demo where we move our [silver data to gold!](./05_SilverToGold.md)