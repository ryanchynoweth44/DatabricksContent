# Quick Review: Databricks Delta

As the number of data sources grow and the size of that data increases, organizations have moved to building out data lakes in the cloud in order to provide scalable data engineering workflows and predictive analytics to support business solutions. I have worked with several companies to build out these structured data lakes and the solutions that sit on top of them. While data lakes provide a level of scalability, ease of access, and ability to quickly iterate over solutions, they have always fallen a little short on the structure and reliability that traditional data warehouses have provided. 

Historically I have recommended that customers apply structure, not rules, to their data lake so that it makes the aggregation and transformation of data easier for engineers to serve to customers. The recommended structure was usually similar [lambda architecture](https://en.wikipedia.org/wiki/Lambda_architecture), as not all organizations have streaming data, but they would build out their data lake knowing this was a possibility in the future. The flow of data generally followed the process described below:  

- Batch and streaming data sources are aggregated into **raw** data tables with little to no transforms applied i.e. streaming log data from a web application or batch loading application database deltas.   
- Batch and streaming jobs in our raw data tables are cleaned, transformed, and saved to **staging** tables by executing the minimum number of transforms on a single data source i.e. we tabularize a json file and save it as a parquet file without joining any other data or we aggregate granular data. 
- Finally we aggregate data, join sources, and apply business logic to create our **summary** tables i.e. the tables data analysts, data scientists, and engineers ingest for their solutions. 

One key to the summary tables is that they are business driven. Meaning that we create these data tables to solve specific problems and to be queried on a regular basis. Additionally, I recently took a Databricks course and instead of the terms raw, staging, and summary; they used bronze, silver, and gold tables respectfully. I now prefer the Databricks terminology over my own.    

[Delta Lake](https://delta.io/) is an open source project designed to make big data solutions easier and has been mostly developed by [Databricks](https://databricks.com). Data lakes have always worked well, however, since Delta Lake came onto the scene, organizations are able to take advantage of additional features when updating or creating their data lakes. 

 - **ACID Transactions**: Serial transactions to ensure data integrity.
 - **Data Versioning**: Delta Lake provides data snapshots allowing developers to access and revert earlier versions of data for audits, rollbacks, and reproducing predictive experiments. 
 - **Open Format**: Data stored as in Parquet format making it easy to convert existing data lakes into Delta Lakes.  
 - **Unified Batch and Streaming**: Combine streaming and batch data sources into a single location, and use Delta tables can act as a streaming source as well.  
 - **Schema Enforcement**: Provide and enforce a schema as need to ensure correct data types and columns.  
 - **Schema Evolution**: Easily change the schema of your data as it evolves over time.  

Generally, Delta Lake offers a very similar development and consumption pattern as a typical data lake, however, the items listed above are added features that bring an enterprise level of capabilities that make the lives of data engineers, analysts, and scientists easier. 

As an Azure consultant, Databricks Delta is the big data solution I recommend to my clients. To get started developing a data lake solution with Azure Databricks and Databrick Delta check out the demo provided on my [GitHub](https://github.com/ryanchynoweth44/DatabricksContent/blob/master/delta_lake/Docs/01_CreateEnironment.md). We take advantage of traditional cloud storage by using an [Azure Data Lake Gen2](https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction) to serve as the storage layer on our Delta Lake. 

