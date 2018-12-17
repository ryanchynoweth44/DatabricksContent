## Introduction

This example hopes to provide a demonstration on how to utilize [Azure Databricks](https://docs.azuredatabricks.net/index.html) streaming capabilities using an [Azure Event Hub](https://docs.microsoft.com/en-us/azure/event-hubs/). This blog will walk you through the creating the required resources and the code to deploy in an Azure Databricks notebook. 

### Prerequisites
 - [Azure Subscription](https://azure.microsoft.com/en-us/free/search/?&OCID=AID719825_SEM_KX8R84uR&lnkd=Bing_Azure_Brand&msclkid=6e706d7f2c60158ed7103168c2415255&dclid=CNmloKvCp98CFVJgwQodwMcKKQ)
 - Basic Knowledge of Python and/or Scala

### Demo Walkthrough
The demo is currently broken into logic sections. Please complete in the following order:  
1. [Send Data to Azure Event Hub - python](./blog/01_SendStreamingWithDatabricks.md)
1. [Read Data from Azure Event Hub - scala](./blog/02_ReadStreamingData.md)