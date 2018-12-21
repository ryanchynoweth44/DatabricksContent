## Introduction

This example hopes to provide a demonstration on how to utilize [Azure Databricks](https://docs.azuredatabricks.net/index.html) streaming capabilities using an [Azure Event Hub](https://docs.microsoft.com/en-us/azure/event-hubs/). This blog will walk you through the creating the required resources and the code to deploy in an Azure Databricks notebook. 

### Prerequisites
 - [Azure Subscription](https://azure.microsoft.com/en-us/free/search/?&OCID=AID719825_SEM_KX8R84uR&lnkd=Bing_Azure_Brand&msclkid=6e706d7f2c60158ed7103168c2415255&dclid=CNmloKvCp98CFVJgwQodwMcKKQ)
 - Basic Knowledge of Python and/or Scala

### Blog
Check out the [blog](./blog/StreamingDatabricksBlog.md) I wrote to accompany this Streaming Machine Learning Walkthrough. It describes benefits and ways to implement a streaming data solution.  

### Demo Walkthrough
The demo is broken into logic sections. Please complete in the following order:  
1. [Send Data to Azure Event Hub (python)](./walkthrough/01_SendStreamingWithDatabricks.md)
1. [Read Data from Azure Event Hub (scala)](./walkthrough/02_ReadStreamingData.md)
1. [Train a Basic Machine Learning Model on Databricks (scala)](./walkthrough/03_TrainMachineLearningModel.md)
1. [Create new Send Data Notebook](./walkthrough/04_ModifedStreamingData.md)
1. [Make Streaming Predictions](./walkthrough/05_MakeStreamingPredictions.md)

### Contact
Please feel free to add to the walkthrough, or let me know of any confusing or broken steps. For any additional comments or questions email me at rchynoweth@10thmagnitude.com. 