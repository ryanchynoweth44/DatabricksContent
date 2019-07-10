# Introduction

This repository aims to provide various Databricks tutorials and demos.     

## Prerequisites
 - [Azure Subscription](https://azure.microsoft.com/en-us/free/search/?&OCID=AID719825_SEM_KX8R84uR&lnkd=Bing_Azure_Brand&msclkid=6e706d7f2c60158ed7103168c2415255&dclid=CNmloKvCp98CFVJgwQodwMcKKQ)
 - Basic Knowledge of Python and/or Scala

## Blogs

### Data Streaming and Machine Learning
Streaming data process and predictive analytics are a match made in heaven when pursuing real-time predictive insights for your organization. In an effort to show an end to end demonstration of setting up a simple streaming data pipeline and adding a machine learning solution on top of it we will use the popular [New York City Taxi Tips](https://www.kaggle.com/dhimananubhav/predicting-taxi-tip-rates-in-nyc) dataset. We deomonstrate how to utilize [Azure Databricks'](https://docs.azuredatabricks.net/index.html) streaming capabilities using an [Azure Event Hub](https://docs.microsoft.com/en-us/azure/event-hubs/). Check out my [Stream Databricks Blog Post](https://ryansdataspot.com/2019/01/09/streaming-machine-learning-with-azure-databricks/)! 

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

## Contact
Please feel free to recommend demos or contact me if there are any confusing/broken steps. For any additional comments or questions email me at ryanachynoweth@gmail.com. 
