# Databricks notebook source
# MAGIC %md
# MAGIC Let's assume that we have an Azure Data Lake Gen 2 with three containers created: bronze (raw), silver (delta), and gold (delta). 
# MAGIC 
# MAGIC We will mount each container to the following locations: 
# MAGIC - `bronze` --> `/mnt/datalake/<storage account name>/bronze`
# MAGIC - `silver` --> `/mnt/datalake/<storage account name>/silver`
# MAGIC - `gold` --> `/mnt/datalake/<storage account name>/gold`
# MAGIC 
# MAGIC Our silver and gold tables will follow the following path structure using subject area and table name: `/mnt/datalake/<storage account name>/gold/<subject area>/<table name>`

# COMMAND ----------

# required variables
scope_name = "AdminScope"
storage_account_name = dbutils.secrets.get(scope_name,"storageAccountName")
containers = ['bronze', 'silver', 'gold']
client_id = dbutils.secrets.get(scope_name, "client_id")
client_secret = dbutils.secrets.get(scope_name, "client_secret")
tenant_id = dbutils.secrets.get(scope_name, "tenant_id")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": client_id,
           "fs.azure.account.oauth2.client.secret": client_secret,
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/{}/oauth2/token".format(tenant_id)}

# COMMAND ----------

for c in containers:
  try : 
      dbutils.fs.mount(
      source = "abfss://{}@{}.dfs.core.windows.net/".format(c, storage_account_name),
      mount_point = "/mnt/datalake/{}/{}".format(storage_account_name, c),
      extra_configs = configs)
      print("{} Container Mounted.".format(c))
  except Exception as e:
      if "Directory already mounted" in str(e):
          pass # Ignore error if already mounted.
      else:
          raise e

# COMMAND ----------


