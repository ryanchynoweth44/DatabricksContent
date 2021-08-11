# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingesting Data into Delta Lake using COPY INTO
# MAGIC 
# MAGIC Ingesting data into Delta Lake is a key component to Lakehouse architecture and design. There are many ways to do so in Databricks, such as AutoLoader, Batch ingestion, streaming etc. Check out this [blog](https://databricks.com/blog/2021/07/23/getting-started-with-ingestion-into-delta-lake.html) which discusses these concepts in more detail. But we will focus on a incremental ingestion pattern for more efficient data processing.  
# MAGIC 
# MAGIC 
# MAGIC In this notebook we will use our COPY INTO SQL Commands to ingest bronze Parquet data into a silver delta table. It should be noted that we will be using Spark SQL functionality even though this is a Python notebook, I find that using the `spark.sql` function with Python commands makes the code more readable and easier to digest for a larger number of users. 
# MAGIC 
# MAGIC Copy Into loads data from a folder location into a delta location. When used it can be executed repeatedly and will only load new files in the source location. 
# MAGIC - Great to use for adhoc and scheduled processing
# MAGIC - Try to limit it for files numbering in the thousands
# MAGIC   - This is a great way to ingest data that arrives about once a day  
# MAGIC - Only use it for: JSON, CSV, AVRO, ORC, PARQUET, TEXT, and BINARYFILE
# MAGIC - Because COPY INTO is a SQL Command it can also be used in Databricks SQL. 
# MAGIC   - This is really important because it allows individuals to build workflows in their queries, then visualize the data they just transformed.. kind of like a stored procedure that results in a dashboard.  

# COMMAND ----------

dbutils.widgets.text("DatabaseName", "rac_demo_db")
database_name = dbutils.widgets.get("DatabaseName")
dbutils.widgets.text("UserName", "ryan.chynoweth@databricks.com")
user_name = dbutils.widgets.get("UserName")

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS {}".format(database_name))
spark.sql("USE {}".format(database_name))

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from shutil import copyfile
from datetime import timedelta
import os 

# COMMAND ----------

dbutils.fs.rm("/Users/{}/bronze/copy_into_demo".format(user_name), True)

# COMMAND ----------

dbutils.fs.rm("/Users/{}/silver/copy_into_demo".format(user_name), True)

# COMMAND ----------

dbutils.fs.mkdirs("/Users/{}/bronze/copy_into_demo".format(user_name))

# COMMAND ----------

# DBTITLE 1,Load some source data to use in the demo
df = (spark
      .read
      .format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("/databricks-datasets/timeseries/Fires/Fire_Department_Calls_for_Service.csv")

)

# https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html
df = df.withColumn("Call Date", to_timestamp(col("Call Date"), "MM/dd/yyyy"))
df = df.withColumn("Watch Date", to_timestamp(col("Watch Date"), "MM/dd/yyyy"))
df = df.withColumn("Received DtTm", to_timestamp(col("Received DtTm"), 'MM/dd/yyyy hh:mm:ss a')) 
df = df.withColumn("Entry DtTm", to_timestamp(col("Entry DtTm"), 'MM/dd/yyyy hh:mm:ss a'))
df = df.withColumn("Dispatch DtTm", to_timestamp(col("Dispatch DtTm"), 'MM/dd/yyyy hh:mm:ss a'))
df = df.withColumn("Response DtTm", to_timestamp(col("Response DtTm"), 'MM/dd/yyyy hh:mm:ss a'))
df = df.withColumn("On Scene DtTm", to_timestamp(col("On Scene DtTm"), 'MM/dd/yyyy hh:mm:ss a'))
df = df.withColumn("Transport DtTm", to_timestamp(col("Transport DtTm"), 'MM/dd/yyyy hh:mm:ss a'))
df = df.withColumn("Hospital DtTm", to_timestamp(col("Hospital DtTm"), 'MM/dd/yyyy hh:mm:ss a'))
df = df.withColumn("Available DtTm", to_timestamp(col("Available DtTm"), 'MM/dd/yyyy hh:mm:ss a'))

display(df)

# COMMAND ----------

for c in df.columns:
  df = df.withColumnRenamed(c, c.replace(" ", ""))

# COMMAND ----------

# DBTITLE 1,Save the data as a delta because it's faster. We use this data to create individual parquet files that we then consume
tables = [t.name for t in spark.catalog.listTables(database_name)]
if "fires" not in tables:
  df.write.format("delta").saveAsTable("Fires")

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize Fires ZORDER BY (CallDate) 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Fires limit 50

# COMMAND ----------

if os.path.exists("/dbfs/tmp/Fires"):
  dbutils.fs.rm("/tmp/Fires", True)

# COMMAND ----------

# DBTITLE 1,Create a function to create parquet files
def publish_parquet_file():
  current_date = spark.sql("SELECT min(`CallDate`) from Fires").collect()[0][0]
  max_date = spark.sql("SELECT max(`CallDate`) from Fires").collect()[0][0]

  while current_date < max_date:
    # select 1 day of data and write it as single file
    spark.sql("""
      SELECT * 
      FROM Fires
      WHERE `CallDate` = '{}'
      """.format(current_date)).coalesce(1).write.format('parquet').mode("overwrite").save("/tmp/Fires/")
    
    # copy single parquet file
    dirs = os.listdir("/dbfs/tmp/Fires")
    parquet_file = dirs[-1]
    copyfile("/dbfs/tmp/Fires/{}".format(parquet_file), "/dbfs/Users/{}/bronze/copy_into_demo/{}".format(user_name, parquet_file))
    
    
    yield current_date
    current_date = current_date + timedelta(days=1)

# COMMAND ----------

file_creator = publish_parquet_file()

# COMMAND ----------

# create some files
for i in range(0,3):
  print(next(file_creator))

# COMMAND ----------

# DBTITLE 1,Copy the data into silver delta
spark.sql("""
  COPY INTO delta.`/Users/{}/silver/copy_into_demo/`
  FROM '/Users/{}/bronze/copy_into_demo/'
  FILEFORMAT = PARQUET
""".format(user_name, user_name))

# COMMAND ----------

# DBTITLE 1,Register table in the Database
spark.sql("""
CREATE TABLE fires_delta
USING DELTA
LOCATION '/Users/{}/silver/copy_into_demo/'

""".format(user_name))

# COMMAND ----------

# DBTITLE 1,View Counts by Date
# MAGIC %sql
# MAGIC SELECT CallDate, Count(1) 
# MAGIC FROM fires_delta
# MAGIC GROUP BY CallDate
# MAGIC ORDER BY CallDate

# COMMAND ----------

# create some more files
for i in range(0,5):
  print(next(file_creator))

# COMMAND ----------

# DBTITLE 1,Add only the new files to our delta table using COPY INTO - note you can force a full load if wanted
## NOTICE:
## If you look at the jobs and one of the stages will have the same number of tasks as the number of files that we copied i.e. 5
## 

spark.sql("""
  COPY INTO delta.`/Users/{}/silver/copy_into_demo/`
  FROM '/Users/{}/bronze/copy_into_demo/'
  FILEFORMAT = PARQUET
""".format(user_name, user_name))

# COMMAND ----------

# DBTITLE 1,Let's check out the counts to ensure we didn't process old data
# MAGIC %sql
# MAGIC SELECT CallDate, Count(1) 
# MAGIC FROM fires_delta
# MAGIC GROUP BY CallDate
# MAGIC ORDER BY CallDate

# COMMAND ----------


