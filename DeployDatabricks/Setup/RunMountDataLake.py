import os
import json
import requests
import time



# load config files
config_path = "DeployDatabricks/Setup/setup_config.json"

# create environment variables
with open(config_path) as f:
    environment_variables = json.load(f)

for k in environment_variables.keys():
    os.environ[k] = environment_variables.get(k)

dbricks_auth = {"Authorization": "Bearer {}".format(os.environ.get('databricksToken'))}


mountPath = "{}\\DeployDatabricks\\Setup\\MountDataLake.py".format(os.getcwd())

with open(mountPath, 'rb') as f:
    content = f.readlines()

data = {'path': 'DeployDatabricks\\Setup\\MountDataLake', 'format': "SOURCE", "language": "PYTHON", "overwrite": True, 'content': content}


print("Mounting Data Lake")
res = requests.post("https://{}.azuredatabricks.net/api/2.0/workspace/import".format(os.environ.get('region')), json=data, headers=dbricks_auth)
res.status_code
res.content  
