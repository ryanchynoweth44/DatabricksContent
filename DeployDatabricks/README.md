# Databricks Deployment 

<WORK IN PROGRESS  ---- FIRST VERSION IS NOT COMPLETE>


In all of my Databricks and Data Lake projects there are common themes and functions that are consistent. In an effort to streamline my own projects and provide assistance to clients I am creating a repository of notebooks and scripts that can be used to configure, maintain, and leverage Azure Databricks at an enterprise level. 

The notebooks that are currently available are only the beginning and I will be adding more to them over time. 


There are a number of key **assumptions** that I will make:
- A single [Azure Data Lake Store Gen2](https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction) account. 
    - This account will have three separate containers:
        - `bronze`: our raw data directory
        - `silver`: intermediate layer used to apply structure to our data. Note that there should be a minimum amount of transformations between bronze and silver, as our goal should be to tabularize and structure our data. We will assume this uses [Databricks Delta](https://databricks.com/blog/2017/10/25/databricks-delta-a-unified-management-system-for-real-time-big-data.html).
            - In some scenarios it could mean splitting the bronze datasets into multiple silver tables. 
        - `gold`: business data layer used to support solutions. Note, we leverage the silver layer as our source data to apply business logic and transformations to support specific business needs. We will assume this uses Databricks Delta.

- Each of the containers will be mounted to our Databricks workspace using the following format: 
    - `bronze` --- > `/mnt/datalake/<storage account name>/bronze`
    - `silver` --- > `/mnt/datalake/<storage account name>/silver`
    - `gold` --- > `/mnt/datalake/<storage account name>/gold`

- Under each of our mount points we will expect there to be three levels: solution, project, table. 
    - These levels allow for logical groupings of our data and allow the data lake to not only grow in depth but in width as more sources are added. 


Using the assumptions made here let's provide and example. Assume you are moving data from a Microsoft SQL Server to a data lake. The bronze layer may contain any file format json, parquet, csv etc. Since our source system is a relational database let's assume we are using parquet files that are being uploaded on a daily basis. Since we are only getting data on a daily basis we will land our data into folders organized by year and simply add a datetime to our file name. Our bronze layer would likely take the form: 

`/mnt/datalake/<storage account name>/bronze/<database name>/<schema name>/<table name>/2020/<table_name>_YYYYMMDDHHmmss.parquet`

Since our silver and gold layers are using Databricks Delta they will follow a similar format, excluding the year directory.

`/mnt/datalake/<storage account name>/silver/<database name>/<schema name>/<table name>`


## Notebooks Available

These notebooks aim to provide a jumpstart to configuring a Databricks workspace for ETL/ELT processing, and for continued management of a workspace. 


### Setup Notebooks

- [InfrastrucureDeployment]()
    - Python script to deploy minimum required infrastructure. 
    - Please reference the [Setup Readme]() for more information.

- [CreateSecret.py](DeployDatabricks/Setup/CreateSecret.py)
    - Assumes that you have created a Databricks scope called "AdminScope" and a secret in that scope called "DatabricksToken". This requirement is to be eliminated once roadmap item 1 is implemented. 
    - Given a secret scope, secret key, and secret value it will create the scope if needed and will add the secret to the scope. 

- [CreateUtilityDatabase.py](DeployDatabricks/Setup/CreateUtilityDatabase.py)
    - Creates a utility database that we will leverage for data collection processes. 

- [MountDataLake.py](DeployDatabricks/Setup/MountDataLake.py)
    - Mounts all three containers highlighted in our assumptions above. 
    - Assumes secrets are stored in a Databricks secret scope, which will be eliminated with item 1 on the roadmap being created.  

### Admin
Note - some of these notebooks have been altered from Databricks employees - references provided upon request.  


