# Databricks Deployment 


In all of my Databricks and Data Lake projects there are common themes and functions that are consistent. In an effort to streamline my own projects and provide assistance to clients I am creating a repository of notebooks and scripts that can be used to configure, maintain, and leverage Azure Databricks at an enterprise level. 

The notebooks that are currently available are only the beginning and I will be adding more to them over time. 

The main feature at this time is the automated deployment and configuration of a Databricks workspace with ADLS Gen2. 


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

- Under each of our mount points we will expect there to be three levels: **solution**, **project**, **table**. 
    - These levels allow for logical groupings of our data and allow the data lake to not only grow in depth but in width as more sources are added. 


Using the assumptions made here let's provide and example. Assume you are moving data from a Microsoft SQL Server to a data lake. The bronze layer may contain any file format json, parquet, csv etc. Since our source system is a relational database let's assume we are using parquet files that are being uploaded on a daily basis. Since we are only getting data on a daily basis we will land our data into folders organized by year and simply add a datetime to our file name. Our bronze layer would likely take the form: 

`/mnt/datalake/<storage account name>/bronze/<database name>/<schema name>/<table name>/2020/<table_name>_YYYYMMDDHHmmss.parquet`

Since our silver and gold layers are using Databricks Delta they will follow a similar format, excluding the year directory.

`/mnt/datalake/<storage account name>/silver/<database name>/<schema name>/<table name>`



Please note that all Delta tables will be created as external Hive tables in the following format: `<solution>__<project>__<gold or silver>.<table> `. This will allow users to easily access data via the Databricks Database and the file system.  This will allow users to easily access data via the Databricks Database and the file system. Please reference the following diagram for a high-level depiction of the solution.      
<br></br>
![](imgs/DatabricksHighlevelDiagram.jpg)
<br></br>


Notice that we deploy two Databricks workspaces, the Admin workspace is purposed for your big data team that creates jobs, manages production data processes, and has the ability to read/write accross the entire data lake. The analytics workspace gives analyst users that use the delta lake read access to the silver and gold tables, and contributor access to a sandbox environment. The analytics workspaces enables user experimentation without interfering with production data pipelines. Currently, the Analytics Workspace uses shared mounts for data access but Databricks does have the ability to use [passthrough authentication](https://docs.microsoft.com/en-us/azure/databricks/security/credential-passthrough/adls-passthrough) on Azure Data Lake Store, however, there are some [limitations](https://docs.microsoft.com/en-us/azure/databricks/security/credential-passthrough/adls-passthrough#limitations) surrounding that feature therefore, it should only be used if required. 

For example, passthrough authentication on a standard cluster can only support one user at a time and standard clusters are the only clusters that can interactively execute scala commands. Which means there is no sharing compute if your data team writes code in scala. Note - this is not the case with high concurrency clusters (SQL, R, and Python).  

## Notebooks Available

These notebooks aim to provide a jumpstart to configuring a Databricks workspace for ETL/ELT processing, and for continued management of a workspace. 


Notebooks are organized into different folders depending on their purpose in the solution. 
- [Admin:](Admin) a collection of notebooks purposed for managing and optimizing your Databricks and Delta Lake solution. This may include capturing data from the Databricks REST APIs or executing regular tasks to manage your solution. 
- [Setup:](Setup) a collection of notebooks and scripts purposed for automating the deployment and configuration of a Databricks workspace and Azure Data Lake Storage Gen2. 

### Admin Notebooks


The following notebooks can be *manually* imported into a workspace and executed. Please note that any admin notebooks require delta tables will be organized using the `utility` solution. For example, our data collection notebooks will be located at: `/mnt/datalake/<storage account name>/silver/utility/data_collection/<table name>`
- [CreateSecret.py](Admin/CreateSecret.py): a notebook that will put a secret in a secret scope. 
- [CreateHiveDeltaTables.py](Admin/CreateHiveDeltaTables.py): scans gold and silver delta table directories and creates external hive tables in the Databricks Database - this is intended to run on a daily or weekly basis 
- [ParallelTableOptimize.scala](Admin/ParallelTableOptimize.scala): this notebook lists all the databases in our Databricks Databases and runs a parallel optimize command over all the tables. Each database is executed sequentially, while tables are operated on in parallel. 
    - NOTE: this original version of this notebook is **not** mine, I altered it very slightly. Reference to the individual who wrote this notebook is available upon request. 
- [GetCompletedJobRunData.py](Admin/GetCompletedJobRunData.py): this notebook collects completed job run data from the Databricks REST API and saves the results to a Delta Table. Please note this is raw API data so analysis and further formatting may be required. Data is saved to `utility__data_collection__silver` hive table.  
- [ParseJobRunData.py](Admin/ParseJobRunData.py): this notebook parses the silver job run data into 5 separate tables: cluster info, schedule info, state info, task info, and job run info. The structure of the tables largely depends on the arrays from the REST API source data. Currently supports completed only jobs. This notebook utilizes Delta streaming capabilities, therefore, can be executed continuously or as a batch via the `.trigger(once=True)` argument.  

### Setup Notebooks

Please reference the [Setup README](Setup). 



### Roadmap

Additional notebooks will be created to do the following:
- Additions to add running jobs to data collection using the `2.0/jobs/runs/list` endpoint. We will then track each active run using the `2.0/jobs/runs/get` endpoint so that it is removed from the table once it is completed. 
- Auto Refreshing our Databricks token that is stored in the secret scope. 
- Group management
- Scheduled jobs data i.e. which jobs are scheduled, status etc. 
- Dashboard based on out of the box data. 