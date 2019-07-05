# Deploy Resources

One of my favorite parts of Databricks is how easy it is to separate compute resources with storage resources. Azure Databricks provides a workspace that enables users to create cluster, develop notebooks, and schedule jobs.    

I acknowledge that not everyone has an Azure subscription available and may not want to sign up for a free trial. Therefore, one can complete this demo using the [Databricks Community](https://community.cloud.databricks.com/login.html) or an [Azure Subscription](https://portal.azure.com). Please note that in either case you still have the ability to mount an Azure Data Lake Gen2 to your cluster so that your data is persistent and globally available. If you do not have a subscription available then instead of saving to a mounted storage location you will need to save directly to the databricks file system (dbfs).   

1. To set up an environment, log into your Azure Subscription and deploy the following two resources to the same resource group:  
    - [Databricks](https://docs.azuredatabricks.net/getting-started/try-databricks.html)
    - [Azure Data Lake Gen2](https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-quickstart-create-account). You can also use the default storage account that is created when deploying an Azure Databricks Workspace.  

1. Create one File Systems in your ADLS Gen2 called **Delta**

1. [Create a service principle](https://docs.microsoft.com/en-us/azure/active-directory/develop/howto-create-service-principal-portal) and client secret so that we can mount our Azure Data Lake Gen2 to our databricks cluster. Please make note of your Azure Tenant Id as well.     

1. In your ADLS, under Access control (IAM) add a Role assignment, where the role is Storage Blob Data Contributor assigned to the service principle that was just created.  


1. [Create a cluster](https://docs.databricks.com/getting-started/quick-start.html#step-2-create-a-cluster) in your Azure Databricks workspace. 


## Mount ADLS Gen2

In our case we will be using built in datasets that Databricks provides. We will read data from DBFS and land it directly into our Bronze Delta tables that live inside an ADLS Gen2. Therefore, we will structure our DBFS as follows.   
- Delta: Delta Lake Tables. The mount path will be */mnt/delta*. 
    - Bronze: raw data
    - Silver: tabularized and saved as parquet files
    - Gold: Business and Query table i.e. transformed, aggregated, joined


1. Now create a Databricks Python Notebook called [`00_MountStorage`](../code/00_MountStorage.py) and attach it to the newly created cluster.  

1. Provide values for the following variables in plain text or  by using the [Azure Databricks CLI and the Secrets API](https://docs.databricks.com/user-guide/secrets/index.html).  
    ```python
    account_name = ""
    key = ""
    client_id = ""
    client_secret = ""
    tenant_id = ""
    ```

1. We need to authenticate against our ADLS Gen2, so lets set some configuration that we will use in the next step.  
    ```python
    configs = {"fs.azure.account.auth.type": "OAuth",
            "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            "fs.azure.account.oauth2.client.id": client_id,
            "fs.azure.account.oauth2.client.secret": client_secret,
            "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/{}/oauth2/token".format(tenant_id)}
    ```

1. Mount our delta file system to the cluster with the following code. 
    ```python
    try : 
        dbutils.fs.mount(
        source = "abfss://{}@{}.dfs.core.windows.net/".format('delta', account_name),
        mount_point = "/mnt/delta",
        extra_configs = configs)
        print("Storage Mounted.")
    except Exception as e:
        if "Directory already mounted" in str(e):
            pass # Ignore error if already mounted.
        else:
            raise e
    print("Success.")
    ```


Next we will set up our [data ingestion processes](./02_SetupDataIngestion.md), in Delta Lake fashion we will implement a batch process and a stream process.  
