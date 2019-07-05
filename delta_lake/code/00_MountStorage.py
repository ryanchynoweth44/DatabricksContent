# Databricks notebook source
# MAGIC %md 
# MAGIC Provide secrets and variables and configuration for mounts

# COMMAND ----------

account_name = dbutils.secrets.get("default", "source_storage_name")
file_system = dbutils.secrets.get("default", "source_file_system")
key = dbutils.secrets.get("default", "source_storage_key")
client_id = dbutils.secrets.get("default", "client_id")
client_secret = dbutils.secrets.get("default", "client_secret")
tenant_id = dbutils.secrets.get("default", "tenant_id")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": client_id,
           "fs.azure.account.oauth2.client.secret": client_secret,
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/{}/oauth2/token".format(tenant_id)}

# COMMAND ----------

# MAGIC %md
# MAGIC Mount your data sources for the demo

# COMMAND ----------

try : 
    dbutils.fs.mount(
      source = "abfss://{}@{}.dfs.core.windows.net/".format(file_system, account_name),
      mount_point = "/mnt/sources/{}".format(account_name+"_"+file_system),
      extra_configs = configs)
    print("Storage Mounted.")
except Exception as e:
    if "Directory already mounted" in str(e):
        pass # Ignore error if already mounted.
    else:
        raise e
print("Success.")

# COMMAND ----------

# MAGIC %md 
# MAGIC Mount your Delta Lake point

# COMMAND ----------

try : 
    dbutils.fs.mount(
    source = "abfss://{}@{}.dfs.core.windows.net/".format('delta', account_name),
    mount_point = "/mnt/delta",
    extra_configs = configs)
    print("Storage Mounted.")
except Exception as e:
    if "Directory already mounted" in str(e):
        pass # Ignore error if already mounted.
    else:
        raise e
print("Success.")

# COMMAND ----------


