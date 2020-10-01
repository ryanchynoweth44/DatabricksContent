import os
import json
import requests
import time
import uuid
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

##########################################
## Set ARM Template values for databricks
##########################################
with open(databricks_arm_path) as f:
    databricks_arm = json.load(f)


if os.environ.get("databricks_workspace_name") is not None:
    databricks_arm['variables']['managedResourceGroupName'] = "[concat('databricks-rg-', '{}', '-', uniqueString('{}', resourceGroup().id))]".format(os.environ.get("databricks_workspace_name"), os.environ.get("databricks_workspace_name") )

    databricks_arm['variables']['usersManagedResourceGroupName'] = "[concat('databricks-rg-', '{}', '-', uniqueString('{}', resourceGroup().id))]".format(os.environ.get("user_databricks_workspace_name"), os.environ.get("user_databricks_workspace_name") )

    databricks_arm['resources'][0]['name'] = "['{}']".format(os.environ.get("databricks_workspace_name") )

    databricks_arm['resources'][1]['name'] = "['{}']".format(os.environ.get("user_databricks_workspace_name") )

    

if os.environ.get("databricks_sku") is not None:
    databricks_arm['resources'][0]['sku']['name'] = "['{}']".format(os.environ.get("databricks_sku"))
    databricks_arm['resources'][1]['sku']['name'] = "['{}']".format(os.environ.get("databricks_sku"))

if os.environ.get("region") is not None:
    databricks_arm['resources'][0]['location'] = "['{}']".format(os.environ.get("region"))
    databricks_arm['resources'][1]['location'] = "['{}']".format(os.environ.get("region"))
    databricks_arm['resources'][2]['location'] = "['{}']".format(os.environ.get("region"))

if os.environ.get("storageAccountName") is not None:
    databricks_arm['resources'][2]['name'] = "['{}']".format(os.environ.get("storageAccountName"))


# update the file with new values
with open(databricks_arm_path, 'w') as f:
    json.dump(databricks_arm, f)


region = os.environ.get("region")
print("Deploying to {} Azure region.".format(region))

##########################################
## Authentication and Resource Client
##########################################
credentials = ServicePrincipalCredentials(
    client_id=os.environ.get("client_id"), # service principal
    secret=os.environ.get("client_secret"), # service principal secret
    tenant=os.environ.get("tenant_id") # tenant id
)
client = ResourceManagementClient(credentials, os.environ.get("subscription_id"))



##########################################
## Resource Group Deployment
##########################################
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

cnt = 0
deployed = False
while not deployed:
    rgs = client.resource_groups.list()
    for r in rgs:
        if r.name == os.environ.get("resource_group_name"):
            deployed = True if a.properties.provisioning_state == 'Succeeded' else False
            print("Resource Group Deployed. ")
    time.sleep(10)
    cnt+=1
    if cnt > 3:
        print("Unable to deploy resource group. ")
        sys.exit(1)




##########################################
## ARM Deployment
##########################################

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
datalake_client.create_file_system(file_system="sandbox")


#############################
### Connect to Databricks  
### Need to generate our Databricks Tokens


## Generate AAD Tokens
def get_aad_token(client_id, client_secret):
    # Acquire a token to authenticate against Azure management API
    authority_url = 'https://login.microsoftonline.com/'+os.environ.get("tenant_id")
    context = adal.AuthenticationContext(authority_url)
    token = context.acquire_token_with_client_credentials(
        resource='https://management.core.windows.net/',
        client_id=client_id,
        client_secret=client_secret
    )
    return token.get('accessToken')



azToken1 = get_aad_token(os.environ.get('client_id'), os.environ.get('client_secret'))
azToken2 = get_aad_token(os.environ.get('user_client_id'), os.environ.get('user_client_secret'))



# gets an access token for databricks
def get_dbx_access_token(client_id, client_secret):
    # Acquire a token to authenticate against the Azure Databricks Resource
    authority_url = 'https://login.microsoftonline.com/'+os.environ.get("tenant_id")
    context = adal.AuthenticationContext(authority_url)
    token = context.acquire_token_with_client_credentials(
        resource="2ff814a6-3304-4ab8-85cb-cd0e6f879c1d",
        client_id=client_id,
        client_secret=client_secret
    )
    return token.get('accessToken')



# Gets the databricks workspace token
def get_databricks_token(adbToken, azToken, subscription_id, resource_group_name, databricks_workspace_name):
    # Format Request API Url
    dbricks_api = "https://{}.azuredatabricks.net/api/2.0".format(region)

    # Request Authentication
    dbricks_auth = {
        "Authorization": "Bearer {}".format(adbToken),
        "X-Databricks-Azure-SP-Management-Token": azToken2,
        "X-Databricks-Azure-Workspace-Resource-Id": ("/subscriptions/{}/resourceGroups/{}/providers/Microsoft.Databricks/workspaces/{}".format(subscription_id, resource_group_name, databricks_workspace_name) )
        }

    # Optional Paramters 
    payload = {"comment": "This token is generated at time of deployment"}
    # Request and Send Data to Create a Databricks Token
    data = requests.post("{}/token/create".format(dbricks_api), headers= dbricks_auth, json=payload)
    # Decode response, get token, and print token
    dict_content = json.loads(data.content.decode('utf-8'))
    token = dict_content.get('token_value')
    print("This is the databricks token: {}".format(token))
    return token


###### Grant the user sp access to only one databricks workspace ######
auth_header = {'Authorization': 'Bearer {}'.format(azToken1)}
res = requests.get("https://management.azure.com/subscriptions/{}/providers/Microsoft.Authorization/roleDefinitions?$filter=roleName eq 'Contributor'&api-version=2018-01-01-preview".format(os.environ.get("subscription_id")), headers=auth_header)
res.status_code
res.content
d = json.loads(res.content.decode('utf-8'))

# User databricks scope
scope = "subscriptions/{}/resourceGroups/{}/providers/Microsoft.Databricks/workspaces/{}".format(os.environ.get("subscription_id"), os.environ.get("resource_group_name"), os.environ.get("user_databricks_workspace_name"))

url = "https://management.azure.com/{}/providers/Microsoft.Authorization/roleAssignments/{}?api-version=2018-01-01-preview".format(scope, d.get('value')[0].get('name'))

body =     { "properties": {
"roleDefinitionId": d.get('value')[0].get('id'),
"principalId": os.environ.get("user_object_id")
}}

put_response = requests.put(url=url, json=body, headers=auth_header)
if put_response.status_code == 201:
    print("Added service principal as Contributor to {}.".format(os.environ.get("user_databricks_workspace_name")))
else :
    print("FAILED to add service principal as Contributor to {}.".format(os.environ.get("user_databricks_workspace_name")))



# Admin workspace token
token1 = get_databricks_token(get_dbx_access_token(os.environ.get('client_id'), os.environ.get('client_secret')), azToken1, os.environ.get('subscription_id'), os.environ.get('resource_group_name'), os.environ.get('databricks_workspace_name'))

# User workspace token
token2 = get_databricks_token(get_dbx_access_token(os.environ.get('user_client_id'), os.environ.get('user_client_secret')), azToken2, os.environ.get('subscription_id'), os.environ.get('resource_group_name'), os.environ.get('user_databricks_workspace_name'))

# Save tokens to config file
print("Saving Token to config file. ")
environment_variables['databricksToken'] = token1
environment_variables['userDatabricksToken'] = token2
with open(config_path, 'w') as f:
    json.dump(environment_variables, f)


os.environ['databricksToken'] = token1
os.environ['userDatabricksToken'] = token2

#### 
#### Add our scope and secrets to the Admin workspace
auth_header = {'Authorization': 'Bearer {}'.format(token1)}

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


### Add a subset of secrets to the user account
auth_header = {'Authorization': 'Bearer {}'.format(token2)}

# Create scope
payload = {"scope": os.environ.get("scopeName")}

d = requests.post("https://{}.azuredatabricks.net/api/2.0/secrets/scopes/create".format(region), headers=auth_header, json=payload)
if d.status_code == 200:
    print("Scope Created.")
else :
    print(d.content.decode('utf-8'))

# add secrets to scope
secrets_to_add = ['subscription_id', 'user_client_id', 'user_client_secret', 'tenant_id', 'resource_group_name', 'storageAccountName', 'userDatabricksToken']

for s in secrets_to_add:
    if 'user' not in s:
        payload = {"scope": os.environ.get("scopeName"), 'key': s, 'string_value': os.environ.get(s)}
    else :
        payload = {"scope": os.environ.get("scopeName"), 'key': s.replace('user_', '').replace('user', ''), 'string_value': os.environ.get(s)}

    d = requests.post("https://{}.azuredatabricks.net/api/2.0/secrets/put".format(region), headers=auth_header, json=payload)
    if d.status_code == 200:
        print("Added Secret --> {}".format(s))
    else :
        print(d.content.decode('utf-8'))





############### Granting access to ADLS ###############
## Admin SP will have contributor access at the account level
## User SP will have read access to silver and gold, and contributor in a sandbox environment
#######################################################

#### Add service principal as Storage Blob Data Contributor to ADLS
print("Adding service principal as Storage Blob Data Contributor in ADLS Gen2")

auth_header = {'Authorization': 'Bearer {}'.format(azToken1)}
res = requests.get("https://management.azure.com/subscriptions/{}/providers/Microsoft.Authorization/roleDefinitions?$filter=roleName eq 'Storage Blob Data Contributor'&api-version=2018-01-01-preview".format(os.environ.get("subscription_id")), headers=auth_header)
res.status_code
contributor_d = json.loads(res.content.decode('utf-8'))



scope = "subscriptions/{}/resourceGroups/{}/providers/Microsoft.Storage/storageAccounts/{}".format(os.environ.get("subscription_id"), os.environ.get("resource_group_name"), os.environ.get("storageAccountName"))


auth_header['Content-Type'] = "application/json"
url = "https://management.azure.com/{}/providers/Microsoft.Authorization/roleAssignments/{}?api-version=2018-01-01-preview".format(scope, str(uuid.uuid1()))

body =     { "properties": {
"roleDefinitionId": contributor_d.get('value')[0].get('id'),
"principalId": os.environ.get("object_id")
}}

put_response = requests.put(url=url, json=body, headers=auth_header)
if put_response.status_code == 201:
    print("Added our service principal as Storage Blob Data Contributor in ADLS Gen2.")
else :
    print("FAILED to add service principal as Storage Blob Data Contributor in ADLS Gen2.")



### Add user service principal as reader to silver and gold and contributor to sandbox
auth_header = {'Authorization': 'Bearer {}'.format(azToken1)}
res = requests.get("https://management.azure.com/subscriptions/{}/providers/Microsoft.Authorization/roleDefinitions?$filter=roleName eq 'Storage Blob Data Reader'&api-version=2018-01-01-preview".format(os.environ.get("subscription_id")), headers=auth_header)
res.status_code
reader_d = json.loads(res.content.decode('utf-8'))


## silver container
silver_scope = "subscriptions/{}/resourceGroups/{}/providers/Microsoft.Storage/storageAccounts/{}/blobServices/default/containers/silver".format(os.environ.get("subscription_id"), os.environ.get("resource_group_name"), os.environ.get("storageAccountName"))

auth_header['Content-Type'] = "application/json"
url = "https://management.azure.com/{}/providers/Microsoft.Authorization/roleAssignments/{}?api-version=2018-01-01-preview".format(silver_scope, str(uuid.uuid1()))

body =     { "properties": {
"roleDefinitionId": reader_d.get('value')[0].get('id'),
"principalId": os.environ.get("user_object_id")
}}

put_response = requests.put(url=url, json=body, headers=auth_header)
if put_response.status_code == 201:
    print("Added our service principal as Storage Blob Data Reader in Silver.")
else :
    print("FAILED to add service principal as Storage Blob Data Reader in Silver.")




## gold container
gold_scope = "subscriptions/{}/resourceGroups/{}/providers/Microsoft.Storage/storageAccounts/{}/blobServices/default/containers/gold".format(os.environ.get("subscription_id"), os.environ.get("resource_group_name"), os.environ.get("storageAccountName"))

auth_header['Content-Type'] = "application/json"
url = "https://management.azure.com/{}/providers/Microsoft.Authorization/roleAssignments/{}?api-version=2018-01-01-preview".format(gold_scope, str(uuid.uuid1()))

body =     { "properties": {
"roleDefinitionId": reader_d.get('value')[0].get('id'),
"principalId": os.environ.get("user_object_id")
}}

put_response = requests.put(url=url, json=body, headers=auth_header)
if put_response.status_code == 201:
    print("Added our service principal as Storage Blob Data Reader in Gold.")
else :
    print("FAILED to add service principal as Storage Blob Data Reader in Gold.")



## sandbox container
sandbox_scope = "subscriptions/{}/resourceGroups/{}/providers/Microsoft.Storage/storageAccounts/{}/blobServices/default/containers/sandbox".format(os.environ.get("subscription_id"), os.environ.get("resource_group_name"), os.environ.get("storageAccountName"))

url = "https://management.azure.com/{}/providers/Microsoft.Authorization/roleAssignments/{}?api-version=2018-01-01-preview".format(sandbox_scope, str(uuid.uuid1()))

body =     { "properties": {
"roleDefinitionId": contributor_d.get('value')[0].get('id'),
"principalId": os.environ.get("user_object_id")
}}

put_response = requests.put(url=url, json=body, headers=auth_header)
if put_response.status_code == 201:
    print("Added our service principal as Storage Blob Data contributor in sandbox.")
else :
    print("FAILED to add service principal as Storage Blob Data contributor in sandbox.")





