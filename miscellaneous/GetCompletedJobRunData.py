# Databricks notebook source
# MAGIC %md
# MAGIC # Get Completed Job Runs Data
# MAGIC 
# MAGIC **Author**: Ryan Chynoweth, [ryan.chynoweth@databricks.com](mailto:ryan.chynoweth@databricks.com)
# MAGIC 
# MAGIC This notebook collects only the completed runs of jobs from the [Runs List endpoint](https://docs.databricks.com/dev-tools/api/latest/jobs.html#runs-list) of a Databricks workspace and saves the data to a delta table. 
# MAGIC 
# MAGIC The notebook can be run in batch more or continuously. 
# MAGIC 
# MAGIC Please note that `run_id` is a unique ID for all runs of all jobs within a workspace. 
# MAGIC 
# MAGIC The values in command 3 must be configured per implementation, and the delta table will be registered in the `default` Hive Metastore so please change the database if you wish to store it in a different one. 

# COMMAND ----------

import requests
import json
import time
import pandas as pd
from delta.tables import *

# COMMAND ----------

# secret values
scope_name = "AdminScope"
storage_account_name = dbutils.secrets.get(scope_name,"storageAccountName")
databricks_token = dbutils.secrets.get(scope_name, "databricksToken")

# if set to false then this notebook will stop executing after it loads all completed runs
run_continuously = False # if True this notebook will run forever  
reset_table = False # to reset the delta table
delta_location = "/mnt/datalake/{}/delta/data_collection/completed_job_runs".format(storage_account_name) ## storage location for the table
databricks_instance = "" # i.e. "eastus2.azuredatabricks.net"
database = "default"

# COMMAND ----------

spark.sql("USE DATABASE {}".format(database))

# COMMAND ----------

# check to see if our table exists
last_run_id = 0
existing_table = DeltaTable.isDeltaTable(spark, delta_location)

# COMMAND ----------

# get the latest run_id saved to our table
if existing_table:
  last_run_id = spark.sql("select max(run_id) from completed_job_runs").collect()[0][0]

# COMMAND ----------

# headers for request
dbricks_auth = {"Authorization": "Bearer {}".format(databricks_token)}

# COMMAND ----------

def save_data(data):
  """ data is a dictionary object """
  ids = [ d.get("run_id") for d in data ] # get all of our run_ids for this batch

  df = spark.createDataFrame(pd.DataFrame(data))

  # merge into our delta table
  if DeltaTable.isDeltaTable(spark, DeltaLocation):
    print("Merging Delta Table")
    deltaTable = DeltaTable.forPath(spark, DeltaLocation)

    (deltaTable.alias("target")
      .merge(df.alias("source"), "target.run_id = source.run_id")
      .whenMatchedUpdateAll()
      .whenNotMatchedInsertAll()
      .execute() )

  else :
    print("Creating Delta Table")
    df.write.format("delta").save(DeltaLocation)
    spark.sql("CREATE TABLE IF NOT EXISTS completed_job_runs USING DELTA LOCATION '{}'".format(DeltaLocation))
  
  return ids # return the batch run_ids

# COMMAND ----------

run_offset = 0 
limit = 100
while True:
  # request completed runs data
  res = requests.get("https://{}/api/2.0/jobs/runs/list".format(databricks_instance), json={'completed_only': True, 'offset': run_offset, 'limit': limit}, headers=dbricks_auth)
  data = json.loads(res.content.decode("utf-8"))
  
  run_ids = save_data(data.get('runs'))
  run_offset += len(run_ids) # increment our offset
  
  # stop the loop if we have no more data
  if (data.get('has_more') == False and run_continuously == False) or (last_run_id in run_ids and run_continuously == False):
    break
  # if there is more data or we run forever then take a rest
  elif (data.get('has_more') == False and run_continuously) or (last_run_id in run_ids and run_continuously):
    last_run_id = spark.sql("select max(run_id) from completed_job_runs").collect()[0][0]
    run_offset = 0 # reset our offset
    print("Sleeping for 30 seconds. Setting last_run_id to: {}".format(last_run_id))
    time.sleep(30) ## sleep for 30 seconds if we have no data left  

