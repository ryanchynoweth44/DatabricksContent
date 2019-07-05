# Set Up Data Ingestion

Azure Databricks provides sample datasets in the Databricks file system. We will not be using these as our data sources, however, we will create a notebook that will simulate streaming data therefore, we will load the data from the sample datasets and "stream" the dataframe into a our source mount. Run the following code snippet in a Databricks notebook to view some of the sources.  

```python
# sample data sources
dbutils.fs.ls('/databricks-datasets')
```

In this section of the demo we will create a simulated streaming data source, and do a one time batch load of data into our source file system.  


## Batch Load 

We will be using the bike sharing dataset available in the datasets directory on our DBFS. 

1. Create a [`01b_BatchSourceToBronze`](../code/01b_BatchSourceToBronze.py) python notebook.

1. List the files in the directory with the following:
    ```python
    dbutils.fs.ls('/databricks-datasets/')
    ```

1. Read the hourly data into a data frame and write it to the source mount we created in step 1 of the demo.  
    ```python
    hourly_df = (spark
    .read
    .option("header", True)
    .option("inferSchema", True)
    .csv("/databricks-datasets/bikeSharing/data-001/hour.csv"))


    daily_df = (spark
    .read
    .option("header", True)
    .option("inferSchema", True)
    .csv("/databricks-datasets/bikeSharing/data-001/day.csv"))
    ```

1. Now that we have acquired our data from its source location (Databricks provided datasets), we will land it into our delta lake. In this case the only transformation we are applying is the transform from CSV to delta i.e. Apache Parquet. For example, if these were JSON files then depending on how complex the json was I would make the decision to tabularize and save it to delta or save it to delta as a single column of type array.  
    ```python
    # write hourly data to source
    hourly_df.write.format("delta").mode("overwrite").save("/mnt/delta/bronze/bikeSharing/hourly")

    # write daily data to source
    daily_df.write.format("delta").mode("overwrite").save("/mnt/delta/bronze/bikeSharing/daily")
    ```

We have completed the inital batch load of our dataset to our delta lake. Using this as an example, we would set up a scheduled job for hourly and daily that would load the newest data into our delta lake. 


## Simulated Streaming Data Source

To simulate a streaming data source we will load an IoT dataset from the sample Databricks datasets, save it as a Databricks table, and write data every 5 seconds to a delta table. 

1. Create a [`01a_StreamSourceToBronze`](../code/01a_StreamSourceToBronze.py) python notebook. 

1. First lets load in our data, and import a few functions that we will require.  
    ```python
    from pyspark.sql.functions import col
    from pyspark.sql.functions import monotonically_increasing_id
    from time import sleep

    df = (spark.read.format('json').load("/databricks-datasets/iot/iot_devices.json")
    .orderBy(col("timestamp"))
    .withColumn("id", monotonically_increasing_id()) )

    display(df)
    ```

1. Save our dataframe as a Databricks table.  
    ```python
    ## for out simulation we will save our data frame to a table and collect batchs to write on a cadence
    try:
    df = spark.sql("SELECT * FROM stream_data")
    print("Table Exists. Loaded Data.")
    except:
    df.write.saveAsTable("stream_data")
    print("Table Created.")
    ```

1. Initialize a few varibles, including the batch size and frequency in seconds you would like to stream. I would recommend a frequency of at least 1 second.  
    ```python
    current = 0
    num_rows = df.count()
    batch_size = 1000
    frequency = 5
    ```

1. Finally, execute this while loop to append data to our bronze Delta table.  
    ```python
    while current < num_rows:
        stream_df = spark.sql("select * from stream_data where id >= {} and id < {}".format(current, current+batch_size))
        stream_df.write.format("delta").mode("append").save("/mnt/delta/bronze/iot/stream_data")
        print("Streamed IDs between: {} and {}".format(current, current+batch_size-1))
        current = current+batch_size

        sleep(frequency)
    ```

Since we just executed the loop lets hurry up and process this data in the next portion of the demo, where we promote our [bronze data to silver](./03_BronzeToSilver.md).  