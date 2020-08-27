import os
import json
import requests
import time
from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.storage import StorageManagementClient
from azure.mgmt.resource.resources.models import DeploymentMode
from azure.storage.filedatalake import DataLakeServiceClient
import adal


# load config files
config_path = "DeployDatabricks/Setup/setup_config.json"
databricks_arm_path = "DeployDatabricks/Setup/databricks_arm.json"

# create environment variables
with open(config_path) as f:
    environment_variables = json.load(f)

for k in environment_variables.keys():
    os.environ[k] = environment_variables.get(k)


## Set ARM Template values for databricks
with open(databricks_arm_path) as f:
    databricks_arm = json.load(f)

if os.environ.get("databricks_workspace_name") is not None:
    databricks_arm['variables']['managedResourceGroupName'] = "[concat('databricks-rg-', '{}', '-', uniqueString('{}', resourceGroup().id))]".format(os.environ.get("databricks_workspace_name"), os.environ.get("databricks_workspace_name") )

    databricks_arm['resources'][0]['name'] = "['{}']".format(os.environ.get("databricks_workspace_name") )

    databricks_arm['outputs']['workspace']['value'] = "[reference(resourceId('Microsoft.Databricks/workspaces', '{}'))]".format(os.environ.get("databricks_workspace_name"))
    

if os.environ.get("databricks_sku") is not None:
    databricks_arm['resources'][0]['sku']['name'] = "['{}']".format(os.environ.get("databricks_sku"))

if os.environ.get("region") is not None:
    databricks_arm['resources'][0]['location'] = "['{}']".format(os.environ.get("region"))
    databricks_arm['resources'][1]['location'] = "['{}']".format(os.environ.get("region"))

if os.environ.get("storageAccountName") is not None:
    databricks_arm['resources'][1]['name'] = "['{}']".format(os.environ.get("storageAccountName"))



with open(databricks_arm_path, 'w') as f:
    json.dump(databricks_arm, f)


region = os.environ.get("region")
print("Deploying to {} Azure region.".format(region))

#############################
## authentication
credentials = ServicePrincipalCredentials(
    client_id=os.environ.get("client_id"), # service principal
    secret=os.environ.get("client_secret"), # service principal secret
    tenant=os.environ.get("tenant_id") # tenant id
)
client = ResourceManagementClient(credentials, os.environ.get("subscription_id"))


#############################
### Resource Group Deployment
rgs = client.resource_groups.list()
existing_rg = False
for r in rgs:
    if r.name == os.environ.get("resource_group_name"):
        print("Warning: Resource Group already exists. We will deploy to this resource group. ")
        existing_rg = True


res = None
if not existing_rg:
    print("Creating resource group.")
    res = client.resource_groups.create_or_update(os.environ.get("resource_group_name"), {'location': region})

deployed = False
while not deployed:
    rgs = client.resource_groups.list()
    for r in rgs:
        if r.name == os.environ.get("resource_group_name"):
            deployed = True
            print("Resource Group Deployed. ")
    time.sleep(10)



#############################
### Databricks Deployment 
# Read template file
with open(databricks_arm_path) as template_file:
    template = json.load(template_file)


# Define template deployment properties
deployment_properties = {
'mode': DeploymentMode.incremental,
'template': template
}

# create workspace
deployment_async_operation = client.deployments.create_or_update(os.environ.get("resource_group_name"), os.environ.get("databricks_workspace_name"), deployment_properties)

# wait for it to be deployed
deployed = False
while not deployed:
    data = client.resources.list_by_resource_group(os.environ.get("resource_group_name"))
    for d in data:
        if d.name == os.environ.get("databricks_workspace_name"):
            deployed = True
    time.sleep(10)



#############################
### Connect to Azure storage 
## Get key and create file systems


storage_client = StorageManagementClient(credentials, os.environ.get("subscription_id"))

storage_keys = storage_client.storage_accounts.list_keys(os.environ.get("resource_group_name"), os.environ.get("storageAccountName"))


storage_keys = {v.key_name: v.value for v in storage_keys.keys}
print('\tKey 1: {}'.format(storage_keys['key1']))
print('\tKey 2: {}'.format(storage_keys['key2']))


datalake_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format("https", os.environ.get("storageAccountName")), credential=storage_keys['key1'])
print("Creating file systems")
datalake_client.create_file_system(file_system="bronze")
datalake_client.create_file_system(file_system="silver")
datalake_client.create_file_system(file_system="gold")



#############################
### Connect to Databricks  
## Get token then create our secrets and scope

# Acquire a token to authenticate against Azure management API
authority_url = 'https://login.microsoftonline.com/'+os.environ.get("tenant_id")
context = adal.AuthenticationContext(authority_url)
token = context.acquire_token_with_client_credentials(
    resource='https://management.core.windows.net/',
    client_id=os.environ.get("client_id"),
    client_secret=os.environ.get("client_secret")
)
azToken = token.get('accessToken')

# Acquire a token to authenticate against the Azure Databricks Resource
token = context.acquire_token_with_client_credentials(
    resource="2ff814a6-3304-4ab8-85cb-cd0e6f879c1d",
    client_id=os.environ.get("client_id"),
    client_secret=os.environ.get("client_secret")
)
adbToken = token.get('accessToken')


# Format Request API Url
dbricks_api = "https://{}.azuredatabricks.net/api/2.0".format(region)


# Request Authentication
dbricks_auth = {
    "Authorization": "Bearer {}".format(adbToken),
    "X-Databricks-Azure-SP-Management-Token": azToken,
    "X-Databricks-Azure-Workspace-Resource-Id": ("/subscriptions/{}/resourceGroups/{}/providers/Microsoft.Databricks/workspaces/{}".format(os.environ.get("subscription_id"), os.environ.get("resource_group_name"), os.environ.get("databricks_workspace_name")) )
    }


# Optional Paramters 
payload = {
    "comment": "This token is generated at time of deployment", # optional parameter
    # "lifetime_seconds": 3600 # optional parameter. If not passed then it is indefinte
}


# Request and Send Data to Create a Databricks Token
data = requests.post("{}/token/create".format(dbricks_api), headers= dbricks_auth, json=payload)

# display the response data
data.status_code
data.content

# Decode response, get token, and print token
dict_content = json.loads(data.content.decode('utf-8'))
token = dict_content.get('token_value')
print("This is the databricks token: {}".format(token))

print("Saving Token to config file. ")
environment_variables['databricksToken'] = token
# Save Databricks Token to Config File
with open(config_path, 'w') as f:
    json.dump(environment_variables, f)


os.environ['databricksToken'] = token

#### Creating our admin scope with secrets
auth_header = {'Authorization': 'Bearer {}'.format(token)}

# Create scope
payload = {"scope": os.environ.get("scopeName")}

d = requests.post("https://{}.azuredatabricks.net/api/2.0/secrets/scopes/create".format(region), headers=auth_header, json=payload)
if d.status_code == 200:
    print("Scope Created.")
else :
    print(d.content.decode('utf-8'))

# add secrets to scope
secrets_to_add = ['subscription_id', 'client_id', 'client_secret', 'tenant_id', 'resource_group_name', 'storageAccountName', 'databricksToken']

for s in secrets_to_add:
    payload = {"scope": os.environ.get("scopeName"), 'key': s, 'string_value': os.environ.get(s)}
    d = requests.post("https://{}.azuredatabricks.net/api/2.0/secrets/put".format(region), headers=auth_header, json=payload)
    if d.status_code == 200:
        print("Added Secret --> {}".format(s))
    else :
        print(d.content.decode('utf-8'))



