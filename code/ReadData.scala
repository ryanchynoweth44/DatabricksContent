// Databricks notebook source
import org.apache.spark.eventhubs.{ ConnectionStringBuilder, EventHubsConf, EventPosition }

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


// Event hub configurations
// Replace values below with yours        
val eventHubName = ""
val eventHubNSConnStr = ""
val connStr = ConnectionStringBuilder(eventHubNSConnStr).setEventHubName(eventHubName).build 

val customEventhubParameters = EventHubsConf(connStr).setMaxEventsPerTrigger(5)
val incomingStream = spark.readStream.format("eventhubs").options(customEventhubParameters.toMap).load()


// Event Hub message format is JSON and contains "body" field
// Body is binary, so you cast it to string to see the actual content of the message
val messages = incomingStream.withColumn("Offset", $"offset".cast(LongType)).withColumn("Time", $"enqueuedTime".cast(TimestampType)).withColumn("Timestamp", $"enqueuedTime".cast(LongType)).withColumn("Value", $"body".cast(StringType).cast(LongType)).select("Offset", "Time", "Timestamp", "Value")


//messages.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()
display(messages.select("Time", "Value"))
