# Summary

When I first started developing using Databricks Delta and Delta Lake there was a bit of a mystery surrounding it. I had a difficult time understanding what it was and how it differed from the data lakes I have worked with in the past. However, after working on a handful of teams where we built out an enterprise data lake using Databricks Delta I realized that I have the same workflow as I did when building out a traditional data lake, except I had a lot more functionality that made it easier.  

Delta Lake is my go to storage solution for most structured and unstructure solutions no matter the size of the data. Databricks makes it easy by providing a seemless integration point between your data storage, data pipelines, and data serving.  

In this demo we talked through the following:
- How to manage data ingestion and serving with Bronze, Silver, and Gold Tables.
- Work with both streaming and batch data.
- Completed Upserting and Time Travel Excercises.
- Develop and deploy a machine learning model with MLFlow and MLlib. 

Please feel free to check out my source files in the [`code`](../code) directory. I provide python source files and jupyter notebooks for each step of the demo. Additionally, there is a [DBC Archive](../code/DeltaLakeDemo.dbc) of the entire solution that you can easily import into your own workspace.   

Thanks for checking out this demo. Please feel free to provide any feedback by emailing me at ryanachynoweth@gmail.com. 