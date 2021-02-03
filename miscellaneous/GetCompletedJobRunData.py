# Databricks notebook source
## 
## This notebook collects completed job run data from the Databricks 2.0/jobs/runs/list API
## We save all completed runs to the utility__data_collection__silver.completed_job_runs table 
## NOTE - this notebook simply collects the data, therefore, parsing and analyzing data should be done in separate processes  
## 

# COMMAND ----------

import requests
import json
import time
import pandas as pd
from delta.tables import *

# COMMAND ----------

scope_name = "AdminScope"
storage_account_name = dbutils.secrets.get(scope_name,"storageAccountName")

# COMMAND ----------

# if set to false then this notebook will stop executing after it loads all completed runs
run_continuously = False # if True this notebook will run forever  
reset_table = False # to reset the delta table
DeltaLocation = "/mnt/datalake/{}/silver/utility/data_collection/completed_job_runs".format(storage_account_name)

# COMMAND ----------

if reset_table: 
  print("Deleting Table.")
  dbutils.fs.rm(DeltaLocation, True)
  spark.sql("DROP TABLE IF EXISTS utility__data_collection__silver.completed_job_runs")

# COMMAND ----------

# check to see if our table exists
table_list=spark.sql("""show tables in utility__data_collection__silver""")
table_name=table_list.filter(table_list.tableName=="completed_jobs").collect()
existing_table = True
last_run_id = 1
if len(table_name)==0:
  existing_table = False
  print("The Table 'utility__data_collection__silver.completed_job_runs' does not exist.")

# COMMAND ----------

# get the latest run_id saved to our table
if existing_table:
  last_run_id = spark.sql("select max(run_id) from utility__data_collection__silver.completed_job_runs").collect()[0][0]

# COMMAND ----------

# get workspace url for api
context = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
databricks_instance = context.get('tags').get('browserHostName')

# COMMAND ----------

# token for authentication
DatabricksToken = dbutils.secrets.get("DeploymentScope", "DatabricksToken")

# COMMAND ----------

# headers for request
dbricks_auth = {"Authorization": "Bearer {}".format(DatabricksToken)}

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
    spark.sql("CREATE TABLE IF NOT EXISTS utility__data_collection__silver.completed_job_runs USING DELTA LOCATION '{}'".format(DeltaLocation))
  
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
    last_run_id = spark.sql("select max(run_id) from utility__data_collection__silver.completed_job_runs").collect()[0][0]
    run_offset = 0 # reset our offset
    print("Sleeping for 30 seconds. Setting last_run_id to: {}".format(last_run_id))
    time.sleep(30) ## sleep for 30 seconds if we have no data left  

