# Query Databricks Data in Scala

A common ask for many Databricks users is the ability to query data outside of Databricks, specifically data that resides in Databricks Delta. When building out a Delta Lake the best practice is to organize data into bronze, silver, and gold zones that build on top of each other and apply business rules and transformations to serve data to users. Once these tables are created, engineers should [create hive tables](https://github.com/delta-io/connectors) using delta table. 

This process allows users to access data using the file path and through the SQL APIs. 
```scala 
var df1 = spark.read.format("delta").load("path/to/delta/table")

var df2 = spark.sql("Select * from delta_table")
```

When a lakehouse leverages these external tables, it becomes extremely easy to query data outside of Databricks. Please note that this solution requires Databricks clusters or SQL Analytics endpoints to query the data, but the data is returned to the extrenal application. If you would like to read data from a delta table without needing a Spark cluster then please reference the [standalone connector](https://github.com/delta-io/connectors). 


Download the [Jar files from Databricks](https://docs.databricks.com/integrations/bi/jdbc-odbc-bi.html), and copy the SparkJDBC42.jar file to the directory of your Scala file. 

Provide the variable values at the top of the file, and run the following command line arugment to run the project.  
```
scala -classpath SparkJDBC42.jar ScalaSqlAnalyticsConnect.scala
```
