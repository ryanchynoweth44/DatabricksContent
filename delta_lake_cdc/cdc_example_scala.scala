// Databricks notebook source
// MAGIC %md
// MAGIC First let's create our delta table with two different versions. 
// MAGIC 1. In the first version we will simply create and insert 4 rows of data
// MAGIC 1. In the second version we will delete 2 rows, insert 1 rows, and update 1 row. 

// COMMAND ----------

val initDF = Seq((18, "bat"),
  (6, "mouse"),
  (-27, "horse"),
  (14, "dog"),
  (24, "cat")).toDF("number", "word")

initDF.write.format("delta").mode("overwrite").option("schemaOverwrite", "true").save("/tmp/demo/delta_table_cdc")

// COMMAND ----------

val updateDF = Seq((18, "bat"),
  (6, "mouse"),
  (-20, "horse"),
  (40, "john")).toDF("number", "word")

updateDF.write.format("delta").mode("overwrite").option("schemaOverwrite", "true").save("/tmp/demo/delta_table_cdc")

// COMMAND ----------

// MAGIC %md
// MAGIC Check out the history of our delta table

// COMMAND ----------

// MAGIC %sql
// MAGIC DESCRIBE HISTORY delta.`/tmp/demo/delta_table_cdc`

// COMMAND ----------

// MAGIC %md
// MAGIC Read both versions of the dataset into a dataframe

// COMMAND ----------

var currentDeltaTableDF = spark.read.format("delta").load("/tmp/demo/delta_table_cdc")
var olderDeltaTableDF = spark.read.format("delta").option("versionAsOf", 0).load("/tmp/demo/delta_table_cdc") // timestampAsOf

// COMMAND ----------

// MAGIC %md
// MAGIC Get the insert and updates between the old version and the current version

// COMMAND ----------

display(currentDeltaTableDF.except(olderDeltaTableDF))

// COMMAND ----------

// MAGIC %md 
// MAGIC Get the deletes and updates

// COMMAND ----------

display(olderDeltaTableDF.except(currentDeltaTableDF))

// COMMAND ----------

// MAGIC %md
// MAGIC Get only the deletes

// COMMAND ----------

display(olderDeltaTableDF.as("old").join(currentDeltaTableDF.as("new"), $"old.word" === $"new.word", "leftanti"))
