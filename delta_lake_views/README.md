# Creating Views in a Delta Lake Solution

Databricks Delta ([Apache Delta](https://delta.io/)), has become a go to solution for developing and deploying data lakes in the cloud. A common ask from many clients is the ability to create views in a Delta Lake for security, business, or hide query complexity. 

When developing a Delta Lake I primarily use Databricks to access and analyze data. With Databricks, developers are provided a Hive metastore which is simply a collection of databases and databases are a collection of tables. 


In order to achieve the "View" that most clients are asking for we have two options:
1. Create additional processes and tables in our Delta Lake
1. Create External Hive Tables and Hive Views 


If you would like to follow along, check out the [Databricks Community Cloud](https://community.cloud.databricks.com/).


## Implementing "Views" as Delta Tables

In order to truly create a "Delta View" on a delta lake, developers are required to create ETL processes that continuously or on a scheduled cadence update a delta table. Within a data lake we are able to apply granular permissions on the lake to allow specific users access to certain directories. By creating additional delta tables to act as views we are able to satisfy several needs that typical sql views satisfy. The drawback with this implementation is that these "views" require a refresh. This can easily be avoided by taking advantage of stream processing which is supported by Apache Delta, but may be more costly than many clients want to pay. 

The following steps are required to implement a successful "view" in delta lake. 
1. Create Delta table
1. Create Delta "View"
1. Create streaming notebook to keep the "view" up to date


In most cases, data lakes are structured to branch directories based off a single top level directory which we will call `datalake`. Within the data lake directory we segment our data based on the level of transformations applied to the dataset. I like to call them bronze, silver, and gold but they are commonly referred as raw, curated, and enriched. Below each of the second directories we will organize our data by source and data type (tables). 

In our case views will be created in the enhanced area of our data lake. Therefore, we may have a gold sample directory structure as follows `datalake/gold/DataSourceOne/Tables` and `datalake/gold/DataSourceOne/Views`


To start we will need to generate data and create our source Delta table. 
```python
# import pandas for easy data creation
import pandas as pd
from pyspark.sql.functions import col
# create a spark dataframe
df = pd.DataFrame([{"ColumnA": 1, "ColumnB": 3}, {"ColumnA": 11, "ColumnB": 31}])
df = spark.createDataFrame(df)
display(df)
# create delta table
df.write.format("delta").mode("overwrite").save("/mnt/deltalake/gold/demosource/Tables/demo_delta_table")
```

Next let's create our intial "View" of the data. We will apply a simply filter function so that the two Delta tables are different. 
```python
# create our delta "view"
df.filter(col("ColumnA") >= 5).write.format("delta").mode("overwrite").save("/mnt/deltalake/gold/demosource/Views/demo_delta_view")
```

Since the "view" is actually a Delta table we need to make sure it is getting updated with new data. We could schedule a cron job to simply insert new records or overwrite the job entirely.   
```python
# overwrite our "view" on a schedule
df.filter(col("ColumnA") >= 5).write.format("delta").mode("overwrite").save("/mnt/deltalake/gold/demosource/Views/demo_delta_view")

# append only new records
changesOnlyDf.filter(col("ColumnA") >= 5).write.format("delta").mode("append").save("/mnt/deltalake/gold/demosource/Views/demo_delta_view")
```

My preferred implementation uses Delta streaming to continuously update our table. First you need to initialize a readStream so that we can load our source Delta table. Then we will start a writeStream so that we are always updating our Delta "view". 
```python
streamDF = spark.readStream.format('delta').load("/mnt/deltalake/gold/demosource/Tables/demo_delta_table")
# display(streamDF) # - use this command to see the dataframe before we make any updates. 
```

Next we need to start a writeStream using the readStream we just created. Notice that our read stream is reading all records in our source Delta table, therefore, we must perform our filter in our writeStream command. 
```python
(streamDF.filter(col("ColumnA") >= 5)
.writeStream
.format("delta")
.option("checkpointLocation", "/delta/checkpoints/view_demo/streaming_demo_view") #allows us to pick up where we left off if we lose connectivity
.outputMode("append") # appends data to our table
.start("/mnt/deltalake/gold/demosource/Views/demo_delta_view") ) 
```


Now we can insert some new data into our source Delta table. Which will then automatically pick up the new data, filter, and append to our "view".  
```python
df = pd.DataFrame([{"ColumnA": 1, "ColumnB": 30}, {"ColumnA": 55, "ColumnB": 80}])
df = spark.createDataFrame(df)
df.write.format("delta").mode("append").save("/mnt/deltalake/gold/demosource/Tables/demo_delta_table")
```

Then we can view our Delta "view" contents. 
```python
display(spark.read.format("delta").load("/mnt/deltalake/gold/demosource/Views/demo_delta_view"))
```
Output: 
<br>
![](/delta_lake_views/imgs/05_display_stream.png) 



Overall, this is a very clean way to implement views within a Data Lake enabling users to query directly without having to go to another system for different data views. However, one drawback for this implementation is that we are duplicating data stored in the data lake. 

In the next example, I walk through a process that avoids this issue but has drawbacks as well. 



## Creating External Hive Tables and Hive Views

To clarify this option, we will complete the following tasks within a Databricks notebook using Python. 
1. Create a Delta table
1. Create an external Hive table
1. Create a Hive view using the external table
1. Update our Delta table
1. Verify that the Delta table update is present in the Hive View


First let's create a delta table. We will import pandas so that we can easily create a spark DataFrame. 
```python
import pandas as pd

# create a spark dataframe
df = pd.DataFrame([{"ColumnA": 1, "ColumnB": 3}, {"ColumnA": 11, "ColumnB": 31}])
df = spark.createDataFrame(df)
display(df)
```

Now create our delta table. Typically, we would have an Azure Storage Account mounted to our Databricks Workspace. In this case we will simply save a delta table to our local file system (ignore the `/mnt/` as that is just for show). 
```python
# create delta table
df.write.format("delta").mode("overwrite").save("/mnt/deltalake/demo/demo_delta_table")
```

Creating an external hive table is extremly easy. We simply provide the table name and the delta table location.   
```python
# create external hive table
spark.sql("CREATE TABLE default.demo_delta_table USING DELTA LOCATION '/mnt/deltalake/demo/demo_delta_table' ")
```

You will notice that this table is now registered to our Databricks Database.  
<br>
![](/delta_lake_views/imgs/01_external_table.png) 


Often views are meant for row-level security purposes. This means we need to explicitly grant permissions to tables or views in our Hive metastore. The best practice for giving users access to many tables or views is to give them access to a database. Therefore, we will create a separate database that users can leverage to query our data. 
```python
# create a second database and view using the previously created external table
spark.sql("CREATE DATABASE IF NOT EXISTS new_demo_database ")
spark.sql("CREATE VIEW new_demo_database.demo_view AS SELECT * FROM default.demo_delta_table ")
```

Notice that we now have a new database and a new view. It is worth pointing out that this view references a table from a different database. It is common to house all external hive tables that point to delta tables in a single database, then create secondary database to provide different views to end users.   
<br> 
![](/delta_lake_views/imgs/02_hive_view.png)


Check out the contents of our new view and you will see that it matches what was in our original pandas dataframe.  
```python
# Display the contents of our view
display( spark.sql("SELECT * FROM new_demo_database.demo_view ") )
```
Output: 
<br> 
![](/delta_lake_views/imgs/03_display_view.png) 



Next we will update our Delta table by appending a new spark dataframe to the table. Notice, we are updating the delta table so we should expect the external hive table to update as well. 
```python
# create a new dataframe
df = pd.DataFrame([{"ColumnA": 1123, "ColumnB": 3178}, {"ColumnA": -11, "ColumnB": 37891}])
df = spark.createDataFrame(df)
# append dataframe to existing delta table
df.write.format("delta").mode("append").save("/mnt/deltalake/demo/demo_delta_table")
```


Lets look at both our delta table and our hive view.  
```python
# Display our delta table
display(spark.read.format("delta").load("/mnt/deltalake/demo/demo_delta_table"))
# Display the contents of our view
display( spark.sql("SELECT * FROM new_demo_database.demo_view ") )
```

Output:
![](/delta_lake_views/imgs/04_view_updates.png)


That is how developers can easily implement views into a data lake solution allowing end users to access the data that they need. While there may be other drawbacks, it is worth pointing out that the Hive metastore is only available if there is a cluster running. However, these tables and views can easily be consumed using Spark connectors. 