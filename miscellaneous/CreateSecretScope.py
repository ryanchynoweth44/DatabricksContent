
###########################
## << NOTE >>
## This script is not yet supported by Databricks APIs. AAD authentication for key vault scope creation is not available. 
## * saving here in case of future support
###########################

import os 
import requests
import adal
import json

# set variables 
os.environ['clientId'] = ""
os.environ['tenantId'] = ""
os.environ['clientSecret'] = ""
os.environ['keyVaultName'] = ''
os.environ['databricksWorkspace'] = ""
os.environ['databricksScope'] = ''
os.environ['subscriptionId'] = ""
os.environ['resourceGroup'] = ""
os.environ['region'] = ""

# Acquire aad token
authority_url = 'https://login.microsoftonline.com/'+os.environ['tenantId']
context = adal.AuthenticationContext(authority_url)
token = context.acquire_token_with_client_credentials(
    resource='https://management.core.windows.net/',
    client_id=os.environ['clientId'],
    client_secret=os.environ['clientSecret']
)
azToken = token.get('accessToken')


# Acquire a token to authenticate against the Azure Databricks Resource
token = context.acquire_token_with_client_credentials(
    resource="2ff814a6-3304-4ab8-85cb-cd0e6f879c1d",
    client_id=os.environ['clientId'],
    client_secret=os.environ['clientSecret']
)
adbToken = token.get('accessToken')


### Add token to key vault
key_vault_url = "https://{}.vault.azure.net".format(os.environ['keyVaultName'])

auth_header = {"Authorization": "Bearer {}".format(adbToken)}

auth_header = {
    "Authorization": "Bearer {}".format(adbToken),
    "X-Databricks-Azure-SP-Management-Token": azToken,
    "X-Databricks-Azure-Workspace-Resource-Id": ("/subscriptions/{}/resourceGroups/{}/providers/Microsoft.Databricks/workspaces/{}".format(os.environ['subscriptionId'], os.environ['resourceGroup'], os.environ['databricksWorkspace']) ),
    "Content-Type': 'text/json"
    }

payload = {
    "scope": 'adminscope2',
    "scope_backend_type": "AZURE_KEYVAULT",
    "initial_manage_principal": "users",
    "backend_azure_keyvault":
        {
            "resource_id": "/subscriptions/{}/resourceGroups/{}/providers/Microsoft.KeyVault/vaults/{}".format(os.environ['subscriptionId'], os.environ['resourceGroup'], os.environ['keyVaultName']),
            "dns_name": "https://{}.vault.azure.net/".format(os.environ['keyVaultName'])
        }   
    }

d = requests.post("https://{}.azuredatabricks.net/api/2.0/secrets/scopes/create".format(os.environ['region']), headers=auth_header, json=payload)
if d.status_code == 200:
    print("Scope Created.")
else :
    print(d.content.decode('utf-8'))



