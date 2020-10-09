# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC CreateData.py creates demo json data and saves it to the `/dbfs/tmp/autoloader_demo/src` directory. This is the source data for the autoloader ingestion. 

# COMMAND ----------

import os
import json
import time
import datetime
import random
import shutil

# COMMAND ----------

mount_location = "<path to adls file system>"
shutil.rmtree("/dbfs/{}/autoloader_demo".format(mount_location))

# COMMAND ----------

num_files = 1000

# COMMAND ----------

os.makedirs("/dbfs/{}/autoloader_demo/src".format(mount_location), exist_ok=True)

# COMMAND ----------

dbutils.fs.ls(mount_location+"/autoloader_demo")

# COMMAND ----------

wait_time = 0

for i in range(0, num_files):
  data = {"datetime": str(datetime.datetime.utcnow()), "counter": i, "wait_time": wait_time}
  with open("/dbfs/{}/autoloader_demo/src/json-part-{}.json".format(mount_location, i), 'w') as f:
    print("----> Saving json file: {}".format(data))
    json.dump(data, f)
    
  wait_time = random.randint(0,10)
  print("----> Sleeping for {} seconds.".format(wait_time))
  time.sleep(wait_time)

# COMMAND ----------


