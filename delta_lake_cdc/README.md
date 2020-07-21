# Getting Data Changes in Delta Lake

Azure Databricks is an excellent tool to handle data operations in the cloud as streams and as batches. In this example I would like to focus on batch processing within a Delta Lake using Azure Databricks. Please note that some of these operations can be applied to stream processing as well.  

In big data scenarios it is common to only process the changes since the last processing took place, allowing us to reduce the overall size of the data we need to transform. This is opposed to loading the entire dataset each time, applying transformations, and overwriting the target dataset. 

There are a number of operations you should be familiar with in order to get changes within a Delta Lake. I will provide short code snippets showing how to do the following:
1. Get the inserted and updated rows 
    1. [DataFrame Comparison](DataFrameComparison.md)
    1. [Streaming API](StreamingAPI.md) - Recommended 
1. [Get the deleted and updated rows](DataFrameComparison.md)
1. [Get only the deleted rows](DataFrameComparison.md)



