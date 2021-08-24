import requests
import json

## https://docs.databricks.com/dev-tools/api/latest/repos.html 


token = ""
url = ""
auth = {'Authorization': 'Bearer {}'.format(token)}
repos_path = ""
repo_name = ""


## list and get repo id
list_endpoint = "api/2.0/repos"
list_params = {'path_prefix': repos_path}

data = requests.get(url+list_endpoint, headers=auth, json=list_params)
repo_data = json.loads(data.content.decode('utf-8')).get('repos')
repo_data


repo_id = None
repo_commit_id = None
for r in repo_data:
    if r.get('path') == '{}/{}'.format(repos_path, repo_name):
        repo_id = r.get('id')
        repo_commit_id = r.get('head_commit_id')
        break 


assert repo_id is not None
assert repo_commit_id is not None


update_endpoint = "api/2.0/repos/{}".format(repo_id)

update_params = {'branch': 'main'}

data = requests.patch(url+update_endpoint, headers=auth, json=update_params)
repo_data = json.loads(data.content.decode('utf-8'))
repo_data

print("Old commit id: {}".format(repo_commit_id))
print("New commit id: {}".format(repo_data.get('head_commit_id')))
print("** If commit ids are the same then: \n 1. The update did not work. \n 2. There could be no changes to the repo. ")
assert repo_commit_id != repo_data.get('head_commit_id') # this assertion will succeed if there were changes that we pulled

