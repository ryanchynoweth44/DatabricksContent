// Databricks notebook source
import org.apache.spark.eventhubs.{ ConnectionStringBuilder, EventHubsConf, EventPosition }

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.PipelineModel

val container_name = ""
val account_name = ""

/// READ DATA
// Event hub configurations
// Replace values below with yours        
val eventHubName = ""
val eventHubNSConnStr = ""
val connStr = ConnectionStringBuilder(eventHubNSConnStr).setEventHubName(eventHubName).build 

val customEventhubParameters = EventHubsConf(connStr).setMaxEventsPerTrigger(5)
val incomingStream = spark.readStream.format("eventhubs").options(customEventhubParameters.toMap).load()
//incomingStream.printSchema    

val jsonSchema = new StructType()
        .add("passenger_count", StringType)
        .add("trip_time_in_secs", StringType)
        .add("trip_distance",StringType)
        .add("total_amount", StringType)
        .add("created_datetime", TimestampType)



val pipelineModel = PipelineModel.load("/mnt/" + account_name + "/" + container_name + "/nycmodels/latest/nyctaximodel.model")

val messages = pipelineModel.transform(incomingStream.select(from_json($"body".cast("string"), jsonSchema) as "data").select($"data.passenger_count".cast(IntegerType), $"data.trip_time_in_secs".cast(IntegerType), $"data.trip_distance".cast(DoubleType), $"data.total_amount".cast(IntegerType), $"data.created_datetime"))

display(messages)












