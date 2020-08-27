# Setup

The following instructions use a Python 3.7 Anaconda environment. Please pip install the following requirements:
```
pip install azure-mgmt-resource==3.0.0
pip install azure-storage-file-datalake
pip install azure.mgmt.storage
```


At the top of the [deployment script](InfrastructureDeploy.py) you will need to provide the following values. Please note that this can be switched to using either command line arguments or environment variables. 
```python
config_path = "DeployDatabricks/Setup/setup_config.json"
databricks_arm_path = "DeployDatabricks/Setup/databricks_arm.json"
```

The `databricks_arm_path` is the path to the ARM template that is used to deploy a Databricks workspace and Azure Data Lake Gen2 storage account. This json file is provided in the repository. 


The `config_path` is the path to a json file that has the structure below. This json file is used to set values in the environment variables. Please note that everything will be deployed using the provided service principal. 
```json
{
    "region": "westus",
    "subscription_id": "11111111-1111-1111-1111-111111111111",
    "client_id": "11111111-1111-1111-1111-111111111111", 
    "client_secret": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 
    "tenant_id": "11111111-1111-1111-1111-111111111111",
    "resource_group_name": "rg1",
    "databricks_workspace_name": "dbrx1",
    "databricks_sku": "premium", 
    "storageAccountName": "adls2", 
    "scopeName": "AdminScope" 

}
```
Please note the following:
- The notebooks provided will work out of the box if the scope name remains "AdminScope"
- The storage account name must follow the naming conventions of azure storage
- I recommend a databricks_sku of "premium" 



## Deployment

Please execute the following steps in order to deploy a Databricks environment. 

1. Run the [Infrastructure Deployment Script](InfrastructureDeploy.py) which will complete the following:
    - Create or use existing resource group
    - Create a Databricks workspace
    - Create an ADLS Gen2 storage account
    - Create bronze, silver, and gold file systems
    - Added the following secrets to the `AdminScope` Databricks scope.
        - subscription_id 
        - client_id 
        - client_secret 
        - tenant_id 
        - resource_group_name 
        - storageAccountName 
        - databricksToken

1. Execute the [Run Mount Data Lake](RunMountDataLake.py) script which will complete the following:
    - Imports the [`MountDataLake.py`](MountDataLake.py) notebook to the Setup directory in the workspace root
    - Creates a small cluster
        - Saves cluster id to config file
    - Executes MountDataLake notebook to mount our three storage containers: `bronze`, `silver`, `gold`

