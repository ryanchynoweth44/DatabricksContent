# Databricks notebook source
## 
## This notebook should be executed on a regular basis to continuously ensure that all Delta tables are registered in the hive store
## 
## We will always loop through all tables and create them if they do not exist. 
## 

# COMMAND ----------

from delta.tables import *
import multiprocessing
pool = multiprocessing.pool.ThreadPool(multiprocessing.cpu_count())

# COMMAND ----------

storageAccountName = dbutils.secrets.get("AdminScope", "storageAccountName")

# COMMAND ----------

table_paths = []

# get all the silver table paths 
for s in dbutils.fs.ls("/mnt/datalake/{}/silver".format(storageAccountName)):
  projects = [p.path for p in dbutils.fs.ls(s.path)]
  for p in projects:
    table_paths.extend([t.path for t in dbutils.fs.ls(p)])

# get all the gold table paths
for s in dbutils.fs.ls("/mnt/datalake/{}/gold".format(storageAccountName)):
  projects = [p.path for p in dbutils.fs.ls(s.path)]
  for p in projects:
    table_paths.extend([t.path for t in dbutils.fs.ls(p)])

# COMMAND ----------

# function to create tables
def create_table(table_path):
  if DeltaTable.isDeltaTable(spark, table_path): # Ensure that it is a delta table
    print("-----> Creating External Hive Table Using Delta: `{}` ".format(table_path))
    # parse the file path
    path_parts = table_path.split("/")
    database_name = "{}__{}__{}".format(path_parts[5], path_parts[6], path_parts[4])
    table_name = path_parts[7]
    
    # ensure the database is created
    spark.sql("CREATE DATABASE IF NOT EXISTS {}".format(database_name))
    # create the table
    spark.sql("CREATE TABLE IF NOT EXISTS {}.{} USING DELTA LOCATION '{}'".format(database_name, table_name, table_path))

# COMMAND ----------

pool.map(
  lambda tp: create_table(tp), 
  table_paths
)
