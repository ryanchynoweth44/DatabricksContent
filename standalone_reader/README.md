# Databricks Standalone Reader

!! This is current in development. !!

Recently I was working on a project where a customer would like to connect a third-party reporting tool to a cusomter's delta table(s). The delta tables were registered in the hive metastore, but throughout the process it proved extremely cumbersome and required engineers to vacumm the tables beforehand. We ended up deploying a SQL Database and writing data to that system so the reporting tool could query the data. 

Adding an additional database to the system, while it is not overly complex, does add one more resource to the solution that would have otherwise been unneeded. However, no with a natively connector we can accomplish this. 


Link to [blog](https://databricks.com/blog/2020/12/22/natively-query-your-delta-lake-with-scala-java-and-python.html)


In this demo I would like to highlight how engineers can setup, develop, and work with data in a delta table. For this example, we will be simply using my local laptop and visual studio code. But keep in mind that this could apply to other business or web applications that depend on these datasets. 