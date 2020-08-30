# Databricks notebook source
## 
## This cluster parses silver job run data (completed) and inserts the data into our gold tables 
## 
## 
## 

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import explode, first

# COMMAND ----------

# get the storage account name for our delta table paths
scope_name = "AdminScope"
storage_account_name = dbutils.secrets.get(scope_name,"storageAccountName")

# COMMAND ----------

# Set table locations
trigger_once = True
silver_checkpoint_path = "/mnt/datalake/{}/silver/utility/checkpoints/completed_job_runs".format(storage_account_name)
silver_delta_path = "/mnt/datalake/{}/silver/utility/data_collection/completed_job_runs".format(storage_account_name)
completed_job_run_cluster_info_path = "/mnt/datalake/{}/gold/utility/data_collection/completed_job_run_cluster_info".format(storage_account_name)
completed_job_run_schedule_path = "/mnt/datalake/{}/gold/utility/data_collection/completed_job_run_schedule_info".format(storage_account_name)
completed_job_run_state_path = "/mnt/datalake/{}/gold/utility/data_collection/completed_job_run_state_info".format(storage_account_name)
completed_job_run_task_path = "/mnt/datalake/{}/gold/utility/data_collection/completed_job_run_task_info".format(storage_account_name)
completed_job_run_path = "/mnt/datalake/{}/gold/utility/data_collection/completed_job_run_task_info".format(storage_account_name)

# COMMAND ----------

df = spark.read.format("delta").load(silver_delta_path)

# COMMAND ----------

## function to parse the job run cluster data and save to the gold job_run_cluster_info_path
def saveCompletedJobRunClusterData(microBatchDF):
  
  clusterInsExplodeDF = microBatchDF.select(microBatchDF.job_id, microBatchDF.run_id, explode(microBatchDF.cluster_instance))
  clusterInsDF = clusterInsExplodeDF.groupBy("job_id", "run_id").pivot("key").agg(first("value"))
  clusterSpecExpode1 = microBatchDF.select(microBatchDF.job_id, microBatchDF.run_id, explode(microBatchDF.cluster_spec))
  clusterSpecExpode2 = clusterSpecExpode1.select(clusterSpecExpode1.job_id, clusterSpecExpode1.run_id, clusterSpecExpode1.key.alias("cluster_type"), explode(clusterSpecExpode1.value))
  clusterSpecDF = clusterSpecExpode2.groupBy("job_id", "run_id", "cluster_type").pivot("key").agg(first("value"))
  
  if DeltaTable.isDeltaTable(spark, completed_job_run_cluster_info_path):
    # merge data
    deltaTable = DeltaTable.forPath(spark, completed_job_run_cluster_info_path)
    sourceDF = clusterInsDF.join(clusterSpecDF, on=["job_id", "run_id"], how="inner")
    
    (deltaTable.alias("target")
     .merge(sourceDF.alias("source"), "source.job_id=target.job_id and source.run_id=target.run_id")
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute()    
    )
    
  else :
    (clusterInsDF.join(clusterSpecDF, on=["job_id", "run_id"], how="inner")
     .write.format("delta")
     .mode("overwrite")
     .option("mergeSchema", "true")
     .save(completed_job_run_cluster_info_path)
    )

# COMMAND ----------

# saves job run schedule data
def saveCompletedJobRunScheduleData(microBatchDF):
  scheduleExplodeDF = microBatchDF.select(microBatchDF.job_id, microBatchDF.run_id, explode(microBatchDF.schedule))
  scheduleDF = scheduleExplodeDF.groupBy("job_id", "run_id").pivot("key").agg(first("value"))
  
  if DeltaTable.isDeltaTable(spark, completed_job_run_schedule_path):
    # merge data
    deltaTable = DeltaTable.forPath(spark, completed_job_run_schedule_path)
    
    (deltaTable.alias("target")
     .merge(scheduleDF.alias("source"), "source.job_id=target.job_id and source.run_id=target.run_id")
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute()    
    )
    
  else :
    (scheduleDF.write
     .format("delta")
     .mode("overwrite")
     .option("mergeSchema", "true")
     .save(completed_job_run_schedule_path)
    )

# COMMAND ----------

# saves job run state data
def saveCompletedJobRunStateData(microBatchDF):
  stateExplodeDF = microBatchDF.select(microBatchDF.job_id, microBatchDF.run_id, explode(microBatchDF.state))
  stateDF = stateExplodeDF.groupBy("job_id", "run_id").pivot("key").agg(first("value"))
  
  if DeltaTable.isDeltaTable(spark, completed_job_run_state_path):
    # merge data
    deltaTable = DeltaTable.forPath(spark, completed_job_run_state_path)
    
    (deltaTable.alias("target")
     .merge(stateDF.alias("source"), "source.job_id=target.job_id and source.run_id=target.run_id")
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute()    
    )
    
  else :
    (stateDF.write
     .format("delta")
     .mode("overwrite")
     .option("mergeSchema", "true")
     .save(completed_job_run_state_path)
    )

# COMMAND ----------

## function to parse the job run task and save to the gold job_run_cluster_info_path
def saveCompletedJobRunTaskData(microBatchDF):
    
  taskExpode1 = microBatchDF.select(microBatchDF.job_id, microBatchDF.run_id, explode(microBatchDF.cluster_spec))
  taskExpode2 = taskExpode1.select(taskExpode1.job_id, taskExpode1.run_id, taskExpode1.key.alias("task_type"), explode(taskExpode1.value))
  taskDF = taskExpode2.groupBy("job_id", "run_id", "task_type").pivot("key").agg(first("value"))
  
  if DeltaTable.isDeltaTable(spark, completed_job_run_task_path):
    # merge data
    deltaTable = DeltaTable.forPath(spark, completed_job_run_task_path)
    
    (deltaTable.alias("target")
     .merge(taskDF.alias("source"), "source.job_id=target.job_id and source.run_id=target.run_id")
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute()    
    )
    
  else :
    (taskDF.write
     .format("delta")
     .mode("overwrite")
     .option("mergeSchema", "true")
     .save(completed_job_run_task_path)
    )

# COMMAND ----------

def parseCompletedJobRunDataBatch(microBatchDF, batchId):
  # save cluster_instance and cluster_spec data to the completed_job_run_cluster_info table
  saveCompletedJobRunClusterData(microBatchDF)
  # save schedule data to completed_job_run_schedule_path table
  saveCompletedJobRunScheduleData(microBatchDF)
#   # save state data to completed_job_run_state_path table
  saveCompletedJobRunStateData(microBatchDF)
#   # save task data to completed_job_run_task_path table
  saveCompletedJobRunTaskData(microBatchDF)

  # save the rest of the job data to a sing
  sourceDF = microBatchDF.drop('schedule', 'state', 'task', 'cluster_instance', 'cluster_spec')
  if DeltaTable.isDeltaTable(spark, completed_job_run_path):
    # merge data
    deltaTable = DeltaTable.forPath(spark, completed_job_run_path)
    
    (deltaTable.alias("target")
     .merge(sourceDF.alias("source"), "source.job_id=target.job_id and source.run_id=target.run_id")
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute()    
    )
    
  else :
    (sourceDF.write
     .format("delta")
     .mode("overwrite")
     .option("mergeSchema", "true")
     .save(completed_job_run_path)
    )


# COMMAND ----------

# read silver data as a stream
df = spark.readStream.format("delta").load(silver_delta_path)

# COMMAND ----------

if trigger_once:
  print("Trigger write once.")
  (df.writeStream
   .format("delta")
   .option("checkpointLocation", silver_checkpoint_path)
   .trigger(once=True)
   .foreachBatch(parseCompletedJobRunDataBatch)
   .outputMode("update")
   .start()
  )
else :
  print("Triggering stream.")
  (df.writeStream
   .format("delta")
   .option("checkpointLocation", silver_checkpoint_path)
   .foreachBatch(parseCompletedJobRunDataBatch)
   .outputMode("update")
   .start()
  )
  

# COMMAND ----------


