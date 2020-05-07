# Creating Views in a Delta Lake Solution

Databricks Delta ([Apache Delta](https://delta.io/)), has become a go to solution for developing and deploying data lakes in the cloud. A common ask from many clients is the ability to create views in a Delta Lake for security, business, or hide query complexity. 

When developing a Delta Lake I primarily use Databricks to access and analyze data. With Databricks, developers are provided a Hive metastore which is simply a collection of databases and databases are a collection of tables. 


In order to achieve the "View" that most clients are asking for we have two options:
1. Create additional processes and tables in our Delta Lake
1. Create External Hive Tables and Hive Views 


If you would like to follow along, check out the [Databricks Community Cloud](https://community.cloud.databricks.com/).


## Implementing "Views" as Delta Tables

TO BE COMPLETED IN THE NEAR FUTURE


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