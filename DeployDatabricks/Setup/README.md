# Setup


## Deployment

The deployment will complete the following:
1. Create or use existing resource group
1. Create a Databricks workspace
1. Create an ADLS Gen2 storage account


The following instructions use a Python 3.7 Anaconda environment. Please pip install the following requirements:
```
pip install azure-mgmt-resource==3.0.0
pip install azure-storage-file-datalake
```


At the top of the [deployment script](DeployDatabricks/Setup/InfrastructureDeploy.py) you will need to provide the following values. Please note that this can be switched to using either command line arguments or environment variables. 
```python
config_path = "DeployDatabricks/Setup/setup_config.json"
databricks_arm_path = "DeployDatabricks/Setup/databricks_arm.json"
```


The `config_path` is the path to a json file that has the structure below. This json file is used to set values in the environment variables. Please note that everything will be deployed using the provided service principal. 
```json
{
    "region": "westus",
    "subscription_id": "11111111-1111-1111-1111-111111111111",
    "client_id": "11111111-1111-1111-1111-111111111111", // service principal
    "client_secret": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", // service principal secret
    "tenant_id": "11111111-1111-1111-1111-111111111111",
    "resource_group_name": "rg1",
    "databricks_workspace_name": "dbrx1",
    "databricks_sku": "premium",
    "storageAccountName": "adls2"

}
```

The `databricks_arm_path` is the path to the ARM template that is used to deploy a Databricks workspace and Azure Data Lake Gen2 storage account. This json file is provided in the repository. 
