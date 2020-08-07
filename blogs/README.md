# Resources

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