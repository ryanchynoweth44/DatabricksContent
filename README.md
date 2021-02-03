# Introduction

This repository aims to provide various Databricks tutorials and demos.  

If you would like to follow along, check out the [Databricks Community Cloud](https://community.cloud.databricks.com/).

## Demos

### Stream Databricks Example
The demo is broken into logic sections using the [New York City Taxi Tips](https://www.kaggle.com/dhimananubhav/predicting-taxi-tip-rates-in-nyc) dataset. Please complete in the following order:  
1. [Send Data to Azure Event Hub (python)](./streaming_walkthrough/Docs/01_SendStreamingWithDatabricks.md)
1. [Read Data from Azure Event Hub (scala)](./streaming_walkthrough/Docs/02_ReadStreamingData.md)
1. [Train a Basic Machine Learning Model on Databricks (scala)](./streaming_walkthrough/Docs/03_TrainMachineLearningModel.md)
1. [Create new Send Data Notebook](./streaming_walkthrough/Docs/04_ModifedStreamingData.md)
1. [Make Streaming Predictions](./streaming_walkthrough/Docs/05_MakeStreamingPredictions.md)


### Databricks Delta 
The demo is broken into logic sections. Please complete in the following order:  
1. [Setup Environment](./delta_lake_intro/Docs/01_CreateEnironment.md)
1. [Data Ingestion](./delta_lake_intro/Docs/02_SetupDataIngestion.md)
1. [Bronze Data to Silver Data](./delta_lake_intro/Docs/03_BronzeToSilver.md)
1. [A quick ML Model](./delta_lake_intro/Docs/04_MachineLearningWithDeltaLake.md)
1. [Silver Data To Gold Data](./delta_lake_intro/Docs/05_SilverToGold.md)
1. [A Few Cool Features of Delta](./delta_lake_intro/Docs/06_DeltaFeatures.md)
1. [Summary](./delta_lake_intro/Docs/07_Summary.md)


### Programmatically Generate a Databricks Access Token
Using Service Principals to Automate the creation of a Databricks Access Token
1. [README](./generate_access_token)
1. [Reference Blog](https://cloudarchitected.com/2020/01/using-azure-ad-with-the-azure-databricks-api/)



### Delta Lake Views
This is a lie. Delta Lake does not actually support views but it is a common ask from many clients. Whether views are desired to help enforce row-level security or provide different views of data here are a few ways to get it done.
1. [README](./delta_lake_views)  
1. [Hive Views with Delta Lake](./delta_lake_views/HiveViews.py)
1. [Delta Lake "Views"](./delta_lake_views/DeltaLakeTablesAsViews.py)


### Delta Lake CDC Operations  
Batch processing changes within a delta lake is common practice and easy to do. We provide a few examples on how to use the Delta Lake time travel capabilities to get different views on how a table has changed between two versions. 
1. [README](./delta_lake_cdc)
1. [Python Script](./delta_lake_cdc/cdc_example_python.py)
1. [Scala Script](./delta_lake_cdc/cdc_example_scala.scala)


### Databricks Autoloader
An example of using the Autoloader capabilities for file-based processing. Ensures exactly one-time processing for files.  
1. [README](./databricks_autoloader)


## Resources

In this directory I keep a central repository of articles written and helpful resource links with short descriptions. 

Below are a number of link with quick descriptions on what they cover. 
- [Upsert Databricks Blog](https://databricks.com/blog/2019/03/19/efficient-upserts-into-data-lakes-databricks-delta.html)
    - This blog provides a number of very helpful use cases that can be solved using an upsert operation. The parts I found most interesting were different functionality when it came to the actions available when rows are matched or not matched. Users have the ability to delete rows, updates specific values, insert rows, or update entire rows. The `foreachBatch` function is crucial for CDC operations. 

- [Upsert Notebook Example](https://docs.databricks.com/_static/notebooks/merge-in-streaming.html):
    - Python and Scala example completing an upsert with the `foreachBatch` function. 

- [Delta Table Updates](https://docs.databricks.com/delta/delta-update.html)
    - Shows various scenarios for updating delta tables via updates, inserts, and deletes. 
    - There is specific information surrounding schema evolution with the upsert operations, specifically, schema can evolve when using `insertAll` or `updateAll`, but it will not work if you try inserting a row with a column that does not exist yet. 
    - There can be 1, 2, or 3 whenMatched or whenNotMatched clauses. Of these, at most 2 can be whenMatched clauses, and at most 1 can be a whenNotMatched clause.
        - There is more specifics about what actions each of these clause can take as well. 
        - [Automatic Schema Evolution](https://docs.databricks.com/delta/delta-update.html#merge-schema-evolution)

- [Z-ordering Databricks Blog](https://databricks.com/blog/2018/07/31/processing-petabytes-of-data-in-seconds-with-databricks-delta.html)

- [Optimize and Partition Columns](https://docs.databricks.com/delta/best-practices.html#compact-files)

- [Dynamic Partition Pruning](https://kb.databricks.com/delta/delta-merge-into.html#)
    - [Blog](https://databricks.com/blog/2020/04/30/faster-sql-queries-on-delta-lake-with-dynamic-file-pruning.html)

## Contact
Please feel free to recommend demos or contact me if there are any confusing/broken steps. For any additional comments or questions email me at ryanachynoweth@gmail.com. 

## Disclaimer

These examples are not affiliated or purposed to be official documentation for Databricks. For official documentation and tutorials please go to the [Databricks Academy](https://academy.databricks.com/) or the Databricks [blog](https://databricks.com/blog)
