import requests
import adal
import json

# set variables 
clientId = "<Service Principal Id>"
tenantId = "<Tenant Id>"
clientSecret = "<Service Principal Secret>"
subscription_id = "<Subscription Id>"
resource_group = "<Resource Group Name>"
databricks_workspace = "<Databricks Workspace Name>"
dbricks_location = "<Databricks Azure Region i.e. westus>"



# Acquire a token to authenticate against Azure management API
authority_url = 'https://login.microsoftonline.com/'+tenantId
context = adal.AuthenticationContext(authority_url)
token = context.acquire_token_with_client_credentials(
    resource='https://management.core.windows.net/',
    client_id=clientId,
    client_secret=clientSecret
)
azToken = token.get('accessToken')



# Acquire a token to authenticate against the Azure Databricks Resource
token = context.acquire_token_with_client_credentials(
    resource="2ff814a6-3304-4ab8-85cb-cd0e6f879c1d",
    client_id=clientId,
    client_secret=clientSecret
)
adbToken = token.get('accessToken')


# Format Request API Url
dbricks_api = "https://{}.azuredatabricks.net/api/2.0".format(dbricks_location)


# Request Authentication
dbricks_auth = {
    "Authorization": "Bearer {}".format(adbToken),
    "X-Databricks-Azure-SP-Management-Token": azToken,
    "X-Databricks-Azure-Workspace-Resource-Id": ("/subscriptions/{}/resourceGroups/{}/providers/Microsoft.Databricks/workspaces/{}".format(subscription_id, resource_group, databricks_workspace) )
    }


# Optional Paramters 
payload = {
    "comment": "This token is generated through AAD and Databricks APIs", # optional parameter
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