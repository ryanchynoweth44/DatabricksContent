# Processing Changes with Databricks Delta Streaming APIs


In this document I will walk through a demo example of processing the changes in a delta table using the `readStream` and `writeStream` functionality. 

To start we will create a scala notebooks in a Databricks Workspace, and import the following libraries. 
```scala
import org.apache.spark.sql.streaming.Trigger
import io.delta.tables._
```

Next we will set some variables we will use throughout the demo. 
```scala
var silver_data = "/tmp/demo/silver_delta_table_cdc" // this is the location of our source delta table
var gold_data = "/tmp/demo/gold_delta_table_cdc" // this is the location of our target delta table
var silver_checkpoint = "/tmp/demo/silver_checkpoint" // managed checkpoint location for the last read time from source
```

Let's make sure that these locations do not exist yet, this will allow us to rerun this notebook as needed as well.  
```scala
dbutils.fs.rm(silver_data, true)
dbutils.fs.rm(gold_data, true)
dbutils.fs.rm(silver_checkpoint, true)
```


We will initialize our dataset with a toy example. In this example we will consider `word` as the primary key of the table.  
```scala
// write the intial dataset
val initDF = Seq((18, "bat"),
  (6, "mouse"),
  (-27, "horse"),
  (14, "dog"),
  (24, "cat")).toDF("number", "word")

initDF.write.format("delta").mode("overwrite").option("schemaOverwrite", "true").save(silver_data)
```

To process our data we need a function that transforms and writes our dataset to the target table. In this case, we will simply upsert the table if it exists in target or we will create the target table. 
```scala
// function to upsert data from silver to gold
def upsertBatchData(microBatchDF: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row], batchId: scala.Long) = {
    // apply transformations as needed

    // write the data
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
```

We will utilize the `readStream` function to read our source data into a dataframe. If you look at the gold delta table you will notice that it matches the silver table.   
```scala
var silverRead = spark.readStream.format("delta").option("ignoreChanges", "true").load(silver_data) // Read the silver data as a stream
```

Then write the dataframe out to our gold location. Notice the `foreachBatch` argument executes our function we defined above, and we use `trigger` to execute the stream one time. This essentially makes our stream function a batch call by closing the stream after we execute a single micro-batch.  
```scala
// write the silver data as a stream update
silverRead.writeStream
    .format("delta")
    .option("checkpointLocation", silver_checkpoint)
    .trigger(Trigger.Once())
    .foreachBatch(upsertBatchData _)
    .outputMode("update")
    .start()

display(spark.read.format("delta").load(gold_data))
```


Next we will perform an upsert operation on our silver table. In this case we are simply updating one row and inserting another row. 
```scala
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
```


If you look at both the silver and gold tables you will notice that the silver table has been updated by the gold table has not been. 
```scala
// look at the silver data
display(spark.read.format("delta").load(silver_data))

// look at the gold data
display(spark.read.format("delta").load(gold_data))
```

After executing the same code that we used previously to update our gold data we will see that we have captured only the changes from silver and made the appropriate updates in gold.  
```scala
// write the silver data as a stream update
silverRead.writeStream
    .format("delta")
    .option("checkpointLocation", silver_checkpoint)
    .trigger(Trigger.Once())
    .foreachBatch(upsertBatchData _)
    .outputMode("update")
    .start()
```

Check out the gold data to see that it has been updated accordingly.  
```scala
display(spark.read.format("delta").load(gold_data))
```