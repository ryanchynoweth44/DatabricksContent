// Databricks notebook source
// MAGIC %md
// MAGIC In this notebook we will demonstrate differences in delta table merge commands using partitions and not using partitions. This can be used with some of the CDC stream operations we have seen in previous demos. 
// MAGIC 
// MAGIC We will be using the Databricks provided "flights" dataset as our data source to complete the following:
// MAGIC 1. Load the CSV into a dataframe, drop duplicate rows, and write to a delta table without altering the partitions.
// MAGIC 1. We will then merge the entire dataframe with the delta table and obtain the time it took to merge the data
// MAGIC 1. We will write the dataframe to a delta table with a hash partitions (using all columns)
// MAGIC 1. We will then merge the entire dataframe with the partitioned delta table and obtain the time it took to merge the data

// COMMAND ----------

import org.apache.spark.sql.streaming.Trigger._
import org.apache.spark.sql.functions._ // we will be using `hash` and `abs` functions
import io.delta.tables._

// COMMAND ----------

val deltaTablePath = "/tmp/demo/flights_delta"
val partitionedDeltaTablePath = "/tmp/demo/partitioned_flights_delta"
dbutils.fs.rm(deltaTablePath, true)
dbutils.fs.rm(partitionedDeltaTablePath, true)

// COMMAND ----------

// MAGIC %md
// MAGIC Create and merge a delta table without a partition

// COMMAND ----------

// read source data and drop dups
var df1 = spark.read.format("com.databricks.spark.csv").option("delimiter", ",").option("header", "true").load("/databricks-datasets/flights/departuredelays.csv")
df1 = df1.dropDuplicates()
var limitDF1 = df1.limit(50000)

// COMMAND ----------

// string used to match data
var mergeStatement = ""
for (i <- 0 to df1.columns.length-1){
  val colname = df1.columns(i)
  if (i == 0) {
    mergeStatement += s"target.$colname = source.$colname "
  } else {
    mergeStatement += s"and target.$colname = source.$colname "
  }
  
}

// COMMAND ----------

// write the csv to delta
var t1 = System.nanoTime
df1.write.format("delta").mode("overwrite").save(deltaTablePath)
var duration = (System.nanoTime - t1) / 1e9d

// COMMAND ----------

println(s"Time to Write Data: $duration seconds")

// COMMAND ----------

// merge the subset of dataset
t1 = System.nanoTime
var deltaTable = DeltaTable.forPath(spark, deltaTablePath)

(deltaTable.alias("target")
  .merge(limitDF1.alias("source"), mergeStatement)
  .whenMatched().updateAll()
  .whenNotMatched().insertAll()
  .execute()
)
duration = (System.nanoTime - t1) / 1e9d

// COMMAND ----------

println(s"Time to Merge Data: $duration seconds")

// COMMAND ----------

// MAGIC %md
// MAGIC Create and merge partitioned delta table

// COMMAND ----------

// determine the number of partitions we want
val numPartitions = 1000

// COMMAND ----------

// load the data into a new dataframe
var df2 = spark.read.format("com.databricks.spark.csv").option("delimiter", ",").option("header", "true").load("/databricks-datasets/flights/departuredelays.csv")
df2 = df2.dropDuplicates()

// COMMAND ----------

// string used to match data
var mergeStatement = ""
for (i <- 0 to df2.columns.length-1){
  val colname = df2.columns(i)
  if (i == 0) {
    mergeStatement += s"target.$colname = source.$colname "
  } else {
    mergeStatement += s"and target.$colname = source.$colname "
  }
  
}

// COMMAND ----------

var t1 = System.nanoTime
df2 = df2.withColumn("delta_partition", abs(hash(df2.columns.map(col(_)): _*))%numPartitions) // create the column partition
df2.write.format("delta").mode("overwrite").save(partitionedDeltaTablePath)
var duration = (System.nanoTime - t1) / 1e9d

// COMMAND ----------

var limitDF2 = df2.limit(50000)

// COMMAND ----------

println(s"Time to Write Data: $duration seconds")

// COMMAND ----------

val distinctPartitions = df2.select("delta_partition").distinct.collect.map(x => x.getInt(0)).mkString(",")

// COMMAND ----------

t1 = System.nanoTime
// merge the subset of dataset
var deltaTable = DeltaTable.forPath(spark, partitionedDeltaTablePath)

(deltaTable.alias("target")
  .merge(limitDF2.alias("source"), s"target.delta_partition in ($distinctPartitions) and $mergeStatement")
  .whenMatched().updateAll()
  .whenNotMatched().insertAll()
  .execute()
)
duration = (System.nanoTime - t1) / 1e9d

// COMMAND ----------

println(s"Time to Merge Data: $duration seconds")

// COMMAND ----------


