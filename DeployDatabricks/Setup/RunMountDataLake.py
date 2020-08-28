import os
import json
import requests
import time
from base64 import b64decode, b64encode



# load config files
config_path = "DeployDatabricks/Setup/setup_config.json"

# create environment variables
with open(config_path) as f:
    environment_variables = json.load(f)

for k in environment_variables.keys():
    os.environ[k] = environment_variables.get(k)

dbricks_auth = {"Authorization": "Bearer {}".format(os.environ.get('databricksToken'))}



#### Create Setup directory
print("Creating Databricks Directory.")
res = requests.post("https://{}.azuredatabricks.net/api/2.0/workspace/mkdirs".format(os.environ.get('region')), json={'path': "/Setup"}, headers=dbricks_auth)
assert res.status_code == 200



#### Import Notebook
print("Importing Notebook.")

mountPath = "{}\\DeployDatabricks\\Setup\\MountDataLake.py".format(os.getcwd())

with open(mountPath, 'rb') as f:
    content = b64encode(f.read()).decode()

data = {'path': '/Setup/MountDataLake', 'format': "SOURCE", "language": "PYTHON", "overwrite": True, 'content': content}


res = requests.post("https://{}.azuredatabricks.net/api/2.0/workspace/import".format(os.environ.get('region')), json=data, headers=dbricks_auth)
assert res.status_code == 200


#### Creating Cluster
payload = {
    "autoscale": {
        "min_workers": 1,
        "max_workers": 2
    },
    "cluster_name": "AdminCluster",
    "spark_version": "6.6.x-scala2.11",
    "spark_conf": {
        "spark.databricks.repl.allowedLanguages": "sql,python,r",
        "spark.databricks.cluster.profile": "serverless",
        "spark.databricks.delta.preview.enabled": "true"
    },
    "node_type_id": "Standard_DS3_v2",
    "driver_node_type_id": "Standard_DS3_v2",
    "spark_env_vars": {
        "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
    },
    "autotermination_minutes": 15,
    "enable_elastic_disk": True,
    "init_scripts": []
}

res = requests.post("https://{}.azuredatabricks.net/api/2.0/clusters/create".format(os.environ.get('region')), json=payload, headers=dbricks_auth)
assert res.status_code == 200



print("Saving Cluster ID to config file. ")
environment_variables['cluster_id'] = json.loads(res.content.decode("utf-8")).get("cluster_id")
# Save Databricks Token to Config File
with open(config_path, 'w') as f:
    json.dump(environment_variables, f)



#### Running Mount Notebook
payload = {
    'existing_cluster_id': environment_variables.get('cluster_id'),
    'notebook_task': {
        'notebook_path': '/Setup/MountDataLake'
        }
    }

res = requests.post("https://{}.azuredatabricks.net/api/2.0/jobs/runs/submit".format(os.environ.get('region')), json=payload, headers=dbricks_auth)
assert res.status_code == 200
run_id = json.loads(res.content.decode('utf-8')).get('run_id')


while True:
    res = requests.get("https://{}.azuredatabricks.net/api/2.0/jobs/runs/get".format(os.environ.get('region')), json={'run_id': run_id}, headers=dbricks_auth)
    if json.loads(res.content.decode('utf-8')).get('state').get('life_cycle_state') == "TERMINATED":
        print("Mount notebook completed with status: {}".format(json.loads(res.content.decode('utf-8')).get('state').get('result_state')))
        break

    time.sleep(10)


