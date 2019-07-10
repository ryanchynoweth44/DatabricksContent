# Delta Features

Delta Lake, and therefore Databricks Delta offer the following features giving engineers the ability to easily develop and manage enterprise data lakes. 

 - **ACID Transactions**: Serial transactions to ensure data integrity.
 - **Data Versioning**: Delta Lake provides data snapshots allowing developers to access and revert earlier versions of data for audits, rollbacks, and reproducing predictive experiments. 
 - **Open Format**: Data stored as in Parquet format making it easy to convert existing data lakes into Delta Lakes.  
 - **Unified Batch and Streaming**: Combine streaming and batch data sources into a single location, and use Delta tables can act as a streaming source as well.  
 - **Schema Enforcement**: Provide and enforce a schema as need to ensure correct data types and columns.  
 - **Schema Evolution**: Easily change the schema of your data as it evolves over time. 

The features above are universally available and are on most of the marketing content of Delta Lake and Databricks Delta. The features above allow us to perform upsert (update or insert) operations, and time travel on our versioned datasets (not just the schema but data too!). 


## The Upsert

To demonstrate an upsert activity we will be creating a new Silver to Gold pipeline using our hourly batch bike sharing dataset. We would like to summarize the demand of bicycles at the day level so that we can easily monitor activity. Since the data is generated hourly, that means the most current day will need to be updated each hour for 24 hours.  

1. Create python notebook called [`05_DeltaFeatures`](../code/05_DeltaFeatures.py).

1. Lets load in our dataset from our silver delta table.  
    ```python
    # Read in our data
    df = spark.read.format('delta').load("/mnt/delta/silver/bikeSharing/hourly")
    ```

1. For this demo we will want to filter our a few hours from our bike sharing dataset so that we are actually able to perform an upsert operation. Run the following code to get our filter value.  
    ```python
    # subtract a few hours from the max date so we can filter
    from datetime import timedelta
    max_datetime = df.agg({"dteday": "max"}).collect()[0][0]-timedelta(hours=4)
    max_datetime
    ```
1. Next we want to summarize our hourly dataframe to the day level. Please note that we add a datetime column by adding the `hr` column to the `dteday` column. Then display the output and use the Databricks HTML table to see that the latest date is '2012-12-30' and the count is 1592.  
    ```python
    # Summarize data, filter to keep away the most recent 2 hours, write the data
    from pyspark.sql.functions import col, unix_timestamp
    from pyspark.sql.types import DateType
    summary_df = (df 
                .withColumn("datetime", (unix_timestamp(col("dteday"))+col("hr")*3600).cast("timestamp")) # we create a datetime column to filter on using the hr column and dteday column
                .filter(col("datetime") < max_datetime)
                .select("dteday", "cnt")
                .groupBy("dteday")
                .sum("cnt")
                .select(col("dteday").alias("date"), col("sum(cnt)").alias("cnt")))

    # display, and use the html table to sort and see '2012-12-30'
    display(summary_df)
    ```

1. Now write the dataframe as a new delta table in gold.  
    ```python
    # Write the data
    summary_df.write.format("delta").save("/mnt/delta/gold/bikeSharing/daily_summar_for_upsert")
    ```

1. As a quick side, lets check out how delta lake helps us enforce a consistent schema. Here we are trying to overwrite the data we just wrote to gold, but the command fails as we expect simply because we have a date type column instead of a timestamp column.  
    ```python
    #### This should throw an Error
    ## See the schema enforcement
    # Summarize data, filter to keep away the most recent 2 hours, write the data
    from pyspark.sql.functions import col, unix_timestamp
    from pyspark.sql.types import DateType
    summary_df = (df 
                .withColumn("datetime", (unix_timestamp(col("dteday"))+col("hr")*3600).cast("timestamp"))
                .filter(col("datetime") < max_datetime)
                .select("dteday", "cnt")
                .groupBy("dteday")
                .sum("cnt")
                .select(col("dteday").alias("date").cast("date"), col("sum(cnt)").alias("cnt")))

    summary_df.write.format("delta").mode("overwrite").save("/mnt/delta/gold/bikeSharing/daily_summar_for_upsert")
    ```

1. Back to our upsert operation we will load a new df with all our available data, and register it as a temp table in Databricks.  
    ```python
    # Reread the data, summarize and upsert
    whole_df = spark.read.format('delta').load("/mnt/delta/silver/bikeSharing/hourly")

    upsert_df = (whole_df
                .withColumn("date", col("dteday").cast(DateType()))
                .select("date", "cnt")
                .groupBy("date")
                .sum("cnt")
                .select(col("date"), col("sum(cnt)").alias("cnt")))

    # register as a temp table
    upsert_df.registerTempTable("bike_upsert")
    ```

1. Display our new dataframe and filter to the most recent date in the HTML table. We will see that we have added a new row on date '2012-12-31' and the value on '2012-12-30' is now 1796.  
    ```python
    display(upsert_df)
    ```

1. We will need to register our gold delta table to the Databricks database.  
    ```python
    # register delta table in our database. Open the "Data" tab and you will see it in the default database. 
    spark.sql("""
    DROP TABLE IF EXISTS bike_counts
    """)

    spark.sql("""
    CREATE TABLE bike_counts
    USING DELTA
    LOCATION '{}'
    """.format("/mnt/delta/gold/bikeSharing/daily_summar_for_upsert"))
    ```

1. Run our upsert!
    ```sql
    %sql

    MERGE INTO bike_counts
    USING bike_upsert
    ON bike_counts.date = bike_upsert.date
    WHEN MATCHED THEN
    UPDATE SET cnt = bike_upsert.cnt
    WHEN NOT MATCHED THEN
    INSERT *
    ```

1. Run the following command to see if it worked!
    ```python
    # Load and display delta table to see that it was updated appropriately
    display(spark.sql("select * from bike_counts"))
    ```


## Time Traveling

In the same notebook, lets check out how we can use the data versioning capabilities. Our `bike_counts` delta table has now gone through two different versions, one when it was created and one when we upserted. 

1. We can describe the history of our table with the following command. Notice it is returned as a spark dataframe for easy consumption.    
    ```sql
    %sql 
    DESCRIBE HISTORY bike_counts
    ```

1. Let's select data from a previous version of our table.  
    ```sql
    %sql
    SELECT *
    FROM bike_counts
    VERSION AS OF 0
    ```

1. When can even compare different versions of our data!
    ```sql
    %sql
    SELECT count(*) - (
    SELECT count(*)
    FROM bike_counts
    VERSION AS OF 0 ) AS new_entries
    FROM bike_counts
    ```


In general you have completed an end to end implementation of Databricks Delta for two data sources. Check out the [summary](./07_Summary) for a quick recap of the demo!