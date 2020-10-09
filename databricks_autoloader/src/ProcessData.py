# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
import json
import random
import os

# COMMAND ----------

connection_string = "<ADLS Connection String>"
rg_name = "<resource group name>"
account_name = "<storage account name>"
file_system = "<file system name>"
client_id = "<service principal id>"
client_secret = "<service principal secret>"
tenant_id = "<directory id>"
mount_location = "<mount location of adls file system>"
subscription_id = "<subscription id>"
delta_table = "{}/autoloader_demo/output/table".format(mount_location) 
checkpoint_path = "{}/autoloader_demo/output/checkpoint".format(mount_location)

# COMMAND ----------

os.makedirs('/dbfs/{}/autoloader_demo/output'.format(mount_location))

# COMMAND ----------

# MAGIC %scala
# MAGIC val connection_string = "<ADLS Connection String>"
# MAGIC val rg_name = "<resource group name>"
# MAGIC val account_name = "<storage account name>"
# MAGIC val file_system = "<file system name>"
# MAGIC val client_id = "<service principal id>"
# MAGIC val client_secret = "<service principal secret>"
# MAGIC val tenant_id = "<directory id>"
# MAGIC val mount_location = "<mount location of adls file system>"
# MAGIC val subscription_id = "<subscription id>"

# COMMAND ----------

sc = StructType([
  StructField("counter", LongType()),
  StructField("datetime", TimestampType()),
  StructField("wait_time", LongType())
])

# COMMAND ----------

# configs: https://docs.microsoft.com/en-us/azure/databricks/spark/latest/structured-streaming/auto-loader#configuration
df = (spark.readStream.format("cloudFiles")
  .option("cloudFiles.format", "json")
  .schema(sc)
  .load("{}/autoloader_demo/src/*.json".format(mount_location))
     )

# COMMAND ----------

# MAGIC %scala
# MAGIC import com.databricks.sql.CloudFilesAzureResourceManager
# MAGIC val manager = CloudFilesAzureResourceManager
# MAGIC   .newManager
# MAGIC   .option("cloudFiles.connectionString", connection_string)
# MAGIC   .option("cloudFiles.resourceGroup", rg_name)
# MAGIC   .option("cloudFiles.subscriptionId", subscription_id)
# MAGIC   .option("cloudFiles.tenantId", tenant_id)
# MAGIC   .option("cloudFiles.clientId", client_id)
# MAGIC   .option("cloudFiles.clientSecret", client_secret)
# MAGIC   .create()

# COMMAND ----------

data = {"fail": 0}
with open("/dbfs/{}/autoloader_demo/output/failure.json".format(mount_location), 'w') as f:
  print("----> Saving json file: {}".format(data))
  json.dump(data, f)

# COMMAND ----------

def process_data(microBatchDF, batchId):
  with open("/dbfs/{}/autoloader_demo/output/failure.json".format(mount_location), 'r') as f:
    cnt = json.load(f).get('fail')

  fail = True if cnt == 1 else False

  if fail and DeltaTable.isDeltaTable(spark, delta_table):
    microBatchDF = microBatchDF.withColumn("failCol", lit(1))
    microBatchDF = microBatchDF.withColumn("file_name", input_file_name())
    microBatchDF.write.format("delta").mode("append").save(delta_table)   
  else :
    microBatchDF = microBatchDF.withColumn("file_name", input_file_name())
    microBatchDF.write.format("delta").mode("append").option("mergeSchema", "true").save(delta_table)
  
  cnt+=1
  data = {"fail": cnt}
  with open("/dbfs/{}/autoloader_demo/output/failure.json".format(mount_location), 'w') as f:
    print("----> Saving json file: {}".format(data))
    json.dump(data, f)
    

# COMMAND ----------

(df.writeStream
  .format("delta")
  .option("checkpointLocation", checkpoint_path)
  .option("cloudFiles.useNotifications", True)
  .trigger(once=True)
  .foreachBatch(process_data)
  .outputMode("update")
  .start()
)

# COMMAND ----------

display(spark.read.format('delta').load(delta_table).orderBy("counter"))

# COMMAND ----------


