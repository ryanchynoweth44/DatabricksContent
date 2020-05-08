# Introduction

This repository aims to provide various Databricks tutorials and demos.  

PLEASE NOTE THAT THIS REPOSITORY IS NOT ACTIVELY MAINTAINED, BUT WILL BE UPDATED AS ISSUES ARE CREATED. 

If you would like to follow along, check out the [Databricks Community Cloud](https://community.cloud.databricks.com/).

## Demos

### Stream Databricks Example
The demo is broken into logic sections using the [New York City Taxi Tips](https://www.kaggle.com/dhimananubhav/predicting-taxi-tip-rates-in-nyc) dataset. Please complete in the following order:  
1. [Blog](https://github.com/ryanchynoweth44/DatabricksContent/blob/master/blogs/StreamingDatabricksBlog.md)
1. [Send Data to Azure Event Hub (python)](./streaming_walkthrough/Docs/01_SendStreamingWithDatabricks.md)
1. [Read Data from Azure Event Hub (scala)](./streaming_walkthrough/Docs/02_ReadStreamingData.md)
1. [Train a Basic Machine Learning Model on Databricks (scala)](./streaming_walkthrough/Docs/03_TrainMachineLearningModel.md)
1. [Create new Send Data Notebook](./streaming_walkthrough/Docs/04_ModifedStreamingData.md)
1. [Make Streaming Predictions](./streaming_walkthrough/Docs/05_MakeStreamingPredictions.md)


### Databricks Delta 
The demo is broken into logic sections. Please complete in the following order:  
1. [Blog](https://github.com/ryanchynoweth44/DatabricksContent/blob/master/blogs/DatabricksDelta.md)
1. [Setup Environment](./delta_lake/Docs/01_CreateEnironment.md)
1. [Data Ingestion](./delta_lake/Docs/02_SetupDataIngestion.md)
1. [Bronze Data to Silver Data](./delta_lake/Docs/03_BronzeToSilver.md)
1. [A quick ML Model](./delta_lake/Docs/04_MachineLearningWithDeltaLake.md)
1. [Silver Data To Gold Data](./delta_lake/Docs/05_SilverToGold.md)
1. [A Few Cool Features of Delta](./delta_lake/Docs/06_DeltaFeatures.md)
1. [Summary](./delta_lake/Docs/07_Summary.md)


### Programmatically Generate a Databricks Access Token
Using Service Principals to Automate the creation of a Databricks Access Token
1. [README](./generate_access_token)
1. [Reference Blog](https://cloudarchitected.com/2020/01/using-azure-ad-with-the-azure-databricks-api/)



### Delta Lake Views
This is a lie. Delta Lake does not actually support views but it is a common ask from many clients. Whether views are desired to help enforce row-level security or provide different views of data here are a few ways to get it done.
1. [README](./delta_lake_views)  
1. [Hive Views with Delta Lake](./delta_lake_views/HiveViews.py)
1. [Delta Lake "Views"](./delta_lake_views/DeltaLakeTablesAsViews.py)


## Contact
Please feel free to recommend demos or contact me if there are any confusing/broken steps. For any additional comments or questions email me at ryanachynoweth@gmail.com. 
