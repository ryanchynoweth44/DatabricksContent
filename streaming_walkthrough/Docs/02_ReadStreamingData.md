# Read Data From Event Hub
In this walkthrough we will read our streaming dataset and visualize the data. A copy of the script we implement can be found [here](../code/ReadData.scala).

## Write code to Read Data
1. Create a new Scala notebook called "ReadData" inside your Azure Databricks workspace. 

1. Import the required libraries to read data. 
    ```scala
    // Databricks notebook source
    import org.apache.spark.eventhubs.{ ConnectionStringBuilder, EventHubsConf, EventPosition }

    import org.apache.spark.sql.types._
    import org.apache.spark.sql.functions._
    ```

1. Paste the following code and provide the Event Hub name, and the Event Hub Namespace connection string. 
    ``` scala 
    val eventHubName = ""
    val eventHubNSConnStr = ""
    val connStr = ConnectionStringBuilder(eventHubNSConnStr).setEventHubName(eventHubName).build 
    ```

1. Build the connection string and set up the data stream. 
    ```scala
    val customEventhubParameters = EventHubsConf(connStr).setMaxEventsPerTrigger(5)
    val incomingStream = spark.readStream.format("eventhubs").options(customEventhubParameters.toMap).load()
    ```

1. Read data from the Event Hub and display the messages. 
    ```scala
    // Event Hub message format is JSON and contains "body" field
    // Body is binary, so you cast it to string to see the actual content of the message
    val messages = incomingStream.withColumn("Offset", $"offset".cast(LongType)).withColumn("Time", $"enqueuedTime".cast(TimestampType)).withColumn("Timestamp", $"enqueuedTime".cast(LongType)).withColumn("Value", $"body".cast(StringType).cast(LongType)).select("Offset", "Time", "Timestamp", "Value")

    display(messages.select("Time", "Value"))
    ```

1. Please note that the data may be displayed as shown below. Alternative real-time visualizations can be created in databricks by clicking the plot button shown below.  
![](./imgs/05_databricks_visual.png)

1. I would then recommend clicking "Plot Options" and set the plot as shown below. Click "Apply" to see your visual upate in real-time!  
![](./imgs/06_customized_plot.png)

1. You have now read data from an Azure Event Hub. Please move on to "[03_TrainMachineLearningModel.md](03_TrainMachineLearningModel.md)" to train a machine learning model!

    **Please make sure shutdown the "SendData" Python notebook so that it is no longer sending data to the event hub**