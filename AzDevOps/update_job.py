import requests
import json
import sys 
import os 
import json

# YOU CAN PASS PARAMETERS IN VIA THE COMMAND LINE
args = sys.argv

url = "2.1/jobs/reset"
token = args[1]
databricks_instance = args[0] # region or workspace url i.e.  "adb-xxxxxxxxxxxxxxxxx.x.azuredatabricks.net"


# token authentication
auth = {"Authorization": "Bearer {}".format(token)}


f = open("_DevOpsDemo/drop/s/Jobs/AzureDevOpsDemo_JobDefinition.json")
payload = json.load(f)
f.close()
payload




response = requests.post("https://{}/api/{}".format(databricks_instance, url), json=payload, headers=auth)
response.content
assert response.status_code == 200