# Make Streaming Predictions Using the PipelineModel
In this walkthrough we will use our machine learning model and make predictions on a stream of data, then visualize that data in real-time. A copy of the script we implement can be found [here](../../code/StreamingTipPrediction.scala).

1. Import the libraries required for this notebook.
    ```scala
    // import required libraries
    import org.apache.spark.eventhubs.{ ConnectionStringBuilder, EventHubsConf, EventPosition }

    import org.apache.spark.sql.types._
    import org.apache.spark.sql.functions._
    import org.apache.spark.ml.Pipeline
    import org.apache.spark.ml.PipelineModel
    ```
1. Provide the names of your storage account and container. 
    ```scala
    val container_name = ""
    val account_name = ""
    ```

1. Read data from the event hub. Please provide the connection string to the Event Hub Namespace and Event Hub Name. 
    ```scala
    // Event hub configurations
    // Replace values below with yours        
    val eventHubName = ""
    val eventHubNSConnStr = ""
    val connStr = ConnectionStringBuilder(eventHubNSConnStr).setEventHubName(eventHubName).build 

    // set up streaming data
    val customEventhubParameters = EventHubsConf(connStr).setMaxEventsPerTrigger(5)
    val incomingStream = spark.readStream.format("eventhubs").options(customEventhubParameters.toMap).load()
    ```

1. Set data schema to read the message body
    ```scala
    val jsonSchema = new StructType()
            .add("passenger_count", StringType)
            .add("trip_time_in_secs", StringType)
            .add("trip_distance",StringType)
            .add("total_amount", StringType)
            .add("created_datetime", TimestampType)
    ```

1. Load the pipeline model we trained previously. 
    ```scala
    // set you account and container names
    val account_name = ""
    val container_name = ""
    // load pipeline model
    val pipelineModel = PipelineModel.load("/mnt/"+account_name+"/"+container_name+"/nycmodels/latest/nyctaximodel.model")
    ```

1. Connect to data stream and transform the data using the pipeline model. This code snippet will also display the data as it streams into the notebook.  
    ```scala
    // connect and transform the data stream
    val messages = pipelineModel.transform(incomingStream.select(from_json($"body".cast("string"), jsonSchema) as "data").select($"data.passenger_count".cast(IntegerType), $"data.trip_time_in_secs".cast(IntegerType), $"data.trip_distance".cast(DoubleType), $"data.total_amount".cast(IntegerType), $"data.created_datetime"))

    // display the message stream with predictions
    display(messages)
    ```

1. You have successfully made streaming machine learning predictions! 