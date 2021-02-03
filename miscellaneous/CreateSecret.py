# Databricks notebook source
import requests
import json

# COMMAND ----------

context = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
databricks_instance = context.get('tags').get('browserHostName')

# COMMAND ----------

SecretScope = ""
SecretName = ""
SecretValue = ""
DatabricksToken = dbutils.secrets.get("AdminScope", "DatabricksToken")

# COMMAND ----------

dbricks_auth = {"Authorization": "Bearer {}".format(DatabricksToken)}

# COMMAND ----------

# check if the secret scope exists
# if not then we will create it
scopes = dbutils.secrets.listScopes()
exists = False
for s in scopes:
  if s.name == SecretScope:
    exists = True

# COMMAND ----------

if exists == False:
  print("Creating scope: {}".format(SecretScope))
  res = requests.post("https://{}/api/2.0/secrets/scopes/create".format(databricks_instance), json={"scope": SecretScope}, headers=dbricks_auth)
  if res.status_code == 200:
    print("Scope Created. ")
  

# COMMAND ----------

## Add secret to secret scope 
# Note: this will overwrite values!
res = requests.post("https://{}/api/2.0/secrets/put".format(databricks_instance), json={"scope": SecretScope, "key": SecretName, "string_value": SecretValue}, headers=dbricks_auth)
if res.status_code == 200:
  print("Secret Created. ")

