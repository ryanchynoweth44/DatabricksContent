import os
import json
import requests
import time
from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.resource.resources.models import DeploymentMode


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

## authentication
credentials = ServicePrincipalCredentials(
    client_id=os.environ.get("client_id"), # service principal
    secret=os.environ.get("client_secret"), # service principal secret
    tenant=os.environ.get("tenant_id") # tenant id
)
client = ResourceManagementClient(credentials, os.environ.get("subscription_id"))


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



