# Introduction

This repository aims to provide various Databricks tutorials and demos.     

## Blogs

### Data Streaming and Machine Learning
Streaming data process and predictive analytics are a match made in heaven when pursuing real-time predictive insights for your organization. In an effort to show an end to end demonstration of setting up a simple streaming data pipeline and adding a machine learning solution on top of it we will use the popular [New York City Taxi Tips](https://www.kaggle.com/dhimananubhav/predicting-taxi-tip-rates-in-nyc) dataset. We demonstrate how to utilize [Azure Databricks'](https://docs.azuredatabricks.net/index.html) streaming capabilities using an [Azure Event Hub](https://docs.microsoft.com/en-us/azure/event-hubs/). Check out my [Streaming Databricks Blog Post](https://github.com/ryanchynoweth44/DatabricksContent/blob/master/blogs/StreamingDatabricksBlog.md)! 

### Quick Review of Databricks Delta
Databricks Delta allows organizations to build scalable enterprise level data lake solutions with the ability to process big data as batches and streams. We demonstrate some of the basic capabilities and generally processes of Delta Lakes. Check out the [Quick Review: Databricks Delta](https://github.com/ryanchynoweth44/DatabricksContent/blob/master/blogs/DatabricksDelta.md) blog post. 



## Demos

Stream Databricks Example - The demo is broken into logic sections. Please complete in the following order:  
1. [Send Data to Azure Event Hub (python)](./streaming_walkthrough/Docs/01_SendStreamingWithDatabricks.md)
1. [Read Data from Azure Event Hub (scala)](./streaming_walkthrough/Docs/02_ReadStreamingData.md)
1. [Train a Basic Machine Learning Model on Databricks (scala)](./streaming_walkthrough/Docs/03_TrainMachineLearningModel.md)
1. [Create new Send Data Notebook](./streaming_walkthrough/Docs/04_ModifedStreamingData.md)
1. [Make Streaming Predictions](./streaming_walkthrough/Docs/05_MakeStreamingPredictions.md)


Databricks Delta - The demo is broken into logic sections. Please complete in the following order:  
1. [Setup Environment](./delta_lake/Docs/01_CreateEnironment.md)
1. [Data Ingestion](./delta_lake/Docs/02_SetupDataIngestion.md)
1. [Bronze Data to Silver Data](./delta_lake/Docs/03_BronzeToSilver.md)
1. [A quick ML Model](./delta_lake/Docs/04_MachineLearningWithDeltaLake.md)
1. [Silver Data To Gold Data](./delta_lake/Docs/05_SilverToGold.md)
1. [A Few Cool Features of Delta](./delta_lake/Docs/06_DeltaFeatures.md)
1. [Summary](./delta_lake/Docs/07_Summary.md)


Programmatically Generate a Databricks Access Token: using Service Principals to Automate the creation of a Databricks Access Token
1. [README](./generate_access_token)
1. [Reference Blog](https://cloudarchitected.com/2020/01/using-azure-ad-with-the-azure-databricks-api/)

## Contact
Please feel free to recommend demos or contact me if there are any confusing/broken steps. For any additional comments or questions email me at ryanachynoweth@gmail.com. 
