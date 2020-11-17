import requests
import json


url = "2.0/instance-pools/create"
token = ""
databricks_instance = "" # region or workspace url i.e. "centralus" or "adb-xxxxxxxxxxxxxxxxx.x.azuredatabricks.net"

## can use this if executing in databricks notebook
# context = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
# databricks_instance = context.get('tags').get('browserHostName')


# token authentication
auth = {"Authorization": "Bearer {}".format(token)}


# pool details
payload = {
  "instance_pool_name": "my-test-pool",
  "node_type_id": "Standard_D3_v2",
  "min_idle_instances": 0,
  "max_capacity": 25, 
  "idle_instance_autotermination_minutes": 20
}


response = requests.post("https://{}/api/{}".format(databricks_instance, url), json=payload, headers=auth)
response.content
assert response.status_code == 200
