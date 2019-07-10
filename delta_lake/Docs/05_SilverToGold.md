# Silver to Gold Notebooks 

So far we have our streaming and batch data going into a silver tables. We have a machine learning model that we can apply to our batch dataset, and for the sake of getting data into gold we will simply stream our silver data into gold as well. 


## Streaming Silver to Gold
Generally, data analysts, scientists, and engineers will have access to the gold tables,  restricted access to silver, and limited access to bronze. Because gold is open to the organization for analytics and reporting we need to promote our silver streaming data to gold even though we are not applying anymore transformations. Additionally, this is consistent in format so that if we do want to apply transformations in the future there is a notebook we can start to develop in. 

1. Create a python notebook called [`04a_StreamSilverToGold`](../code/04a_StreamSilverToGold.py). 

1. Load our stream data. 
    ```python
    # Read a stream from silver delta table
    streamDF = (spark
                .readStream
                .format('delta')
                .load("/mnt/delta/silver/iot/stream_data"))
    ```

1. Write our stream data. Make note of the checkpoint path and how we have similar table path structure to our silver tables.  
    ```python
    (streamDF
    .writeStream
    .format("delta")
    .option("checkpointLocation", "/delta/checkpoints/iot/gold/stream_data") #allows us to pick up where we left off if we lose connectivity
    .outputMode("append")
    .start("/mnt/delta/gold/iot/stream_data") )
    ```


## Batch Silver to Gold

For this demo we will just use our batch dataset that we used to train our model to make predictions as we move data from silver to gold. 

1. Create a python notebook called [`04b_BatchSilverToGold`](../code/04b_BatchSilverToGold.py), and import the `PipelineModel` function needed to load our previously trained model.  
    ```python
    from pyspark.ml import PipelineModel
    ```
1. Load our bike sharing dataset from the `latest` folder. 
    ```python
    lrModel = PipelineModel.load("/mnt/mlflow/bikesharing/latest/bike_sharing_model.model")
    ```
1. Load our silver dataset that we want to make predictions.  
    ```python
    df = spark.read.format('delta').load("/mnt/delta/silver/bikeSharing/hourly")
    ```

1. Apply our model to the dataset we loaded. Note, that we have not applied any transforms to this dataset yet, this one command formats and scores our data since it is a `PipelineModel`.  
    ```python
    score = lrModel.transform(df)
    ```

1. Finally, write our newly scored silver data to our gold table.  
    ```python
    score.write.format("delta").mode("overwrite").save("/mnt/delta/gold/bikeSharing/hourly")
    ```

Check out some of the [delta features](./06_DeltaFeatures.md) that I believe are worth checking out by adding a new silver to gold notebook on our bike sharing data. 