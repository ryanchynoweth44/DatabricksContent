import requests
import json
import sys 

args = sys.argv


token = args[1]
databricks_instance = args[0] # region or workspace url i.e. "adb-xxxxxxxxxxxxxxxxx.x.azuredatabricks.net"
repo_id = args[2]
url = "2.0/repos/{}".format(repo_id)


# token authentication
auth = {"Authorization": "Bearer {}".format(token)}


# pool details
payload = {
  "branch": "main",
}


response = requests.patch("https://{}/api/{}".format(databricks_instance, url), json=payload, headers=auth)
response.content
assert response.status_code == 200
