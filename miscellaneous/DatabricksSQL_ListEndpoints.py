import requests
import json


token = "<PAT>"
url = "<DB URL>"
auth = {'Authorization': 'Bearer {}'.format(token)}

list_endpoint = "api/2.0/sql/endpoints/"

data = requests.get(url+list_endpoint, headers=auth)
json.loads(data.content.decode('utf-8')).get('endpoints')[0]
