// Databricks notebook source
// MAGIC %md
// MAGIC Helpful link: https://docs.databricks.com/_static/notebooks/merge-in-streaming.html

// COMMAND ----------

import org.apache.spark.sql.streaming.Trigger
import io.delta.tables._

// COMMAND ----------

var silver_data = "/tmp/demo/silver_delta_table_cdc"
var gold_data = "/tmp/demo/gold_delta_table_cdc"
var silver_checkpoint = "/tmp/demo/silver_checkpoint"

// COMMAND ----------

dbutils.fs.rm(silver_data, true)
dbutils.fs.rm(gold_data, true)
dbutils.fs.rm(silver_checkpoint, true)

// COMMAND ----------

// write the intial dataset
val initDF = Seq((18, "bat"),
  (6, "mouse"),
  (-27, "horse"),
  (14, "dog"),
  (24, "cat")).toDF("number", "word")

initDF.write.format("delta").mode("overwrite").option("schemaOverwrite", "true").save(silver_data)

// COMMAND ----------

// function to upsert data from silver to gold
def upsertBatchData(microBatchDF: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row], batchId: scala.Long) = {
  
  if (DeltaTable.isDeltaTable(gold_data)){
    var deltaTable = DeltaTable.forPath(spark, gold_data) // set the delta table for upsert

    (deltaTable.alias("delta_table")
    .merge(microBatchDF.alias("updates"), "updates.word = delta_table.word") // join dataframe 'updates' with delta table 'delta_table' on the key
    .whenMatched().updateAll() // if we match a key then we update all columns
    .whenNotMatched().insertAll() // if we do not match a key then we insert all columns
    .execute() )
  } else {
    microBatchDF.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(gold_data)
  }
  
  
}

// COMMAND ----------

var silverRead = spark.readStream.format("delta").option("ignoreChanges", "true").load(silver_data) // Read the silver data as a stream

// COMMAND ----------

// write the silver data as a stream update
silverRead.writeStream
    .format("delta")
    .option("checkpointLocation", silver_checkpoint)
    .trigger(Trigger.Once())
    .foreachBatch(upsertBatchData _)
    .outputMode("update")
    .start()

// COMMAND ----------

// see the gold data 
display(spark.read.format("delta").load(gold_data))

// COMMAND ----------

// upsert into silver

// apply some updates - this should update the rows with bat and horse and add a row with john
// only present in the silver table too
var deltaTable = DeltaTable.forPath(spark, silver_data)

val updateDF = Seq((18, "bat"),
  (-20, "horse"),
  (40, "john")).toDF("number", "word")

(deltaTable.alias("delta_table")
.merge(updateDF.alias("updates"), "updates.word = delta_table.word") // join dataframe 'updates' with delta table 'delta_table' on the key
.whenMatched().updateAll() // if we match a key then we update all columns
.whenNotMatched().insertAll() // if we do not match a key then we insert all columns
.execute() )

// COMMAND ----------

// look at the silver data
display(spark.read.format("delta").load(silver_data))

// COMMAND ----------

// look at the gold data and see it is different
display(spark.read.format("delta").load(gold_data))

// COMMAND ----------

// write the silver data as a stream update
silverRead.writeStream
    .format("delta")
    .option("checkpointLocation", silver_checkpoint)
    .trigger(Trigger.Once())
    .foreachBatch(upsertBatchData _)
    .outputMode("update")
    .start()

// COMMAND ----------

// see that gold is updated
display(spark.read.format("delta").load(gold_data))

// COMMAND ----------


