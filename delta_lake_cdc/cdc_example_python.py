# Databricks notebook source
# MAGIC %md
# MAGIC First let's create our delta table with two different versions. 
# MAGIC 1. In the first version we will simply create and insert 4 rows of data
# MAGIC 1. In the second version we will delete 2 rows, insert 1 rows, and update 1 row. 

# COMMAND ----------

import pandas as pd

# COMMAND ----------

initDF = pd.DataFrame([[18, "bat"],
  [6, "mouse"],
  [-27, "horse"],
  [14, "dog"],
  [24, "cat"]], columns=["number", "word"])

display(initDF)

# COMMAND ----------

initDF = spark.createDataFrame(initDF)

initDF.write.format("delta").mode("overwrite").option("schemaOverwrite", "true").save("/tmp/cdc_demo/delta_table_cdc")


# COMMAND ----------

updateDF = pd.DataFrame([[18, "bat"],
  [6, "mouse"],
  [-20, "horse"],
  [40, "john"]], columns=["number", "word"])

display(updateDF)

# COMMAND ----------

updateDF = spark.createDataFrame(updateDF)

updateDF.write.format("delta").mode("overwrite").option("schemaOverwrite", "true").save("/tmp/cdc_demo/delta_table_cdc")

# COMMAND ----------

# MAGIC %md
# MAGIC Check out the history of the table

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY delta.`/tmp/demo/delta_table_cdc`

# COMMAND ----------

# MAGIC %md
# MAGIC Read both versions of the table

# COMMAND ----------

currentDeltaTableDF = spark.read.format("delta").load("/tmp/demo/delta_table_cdc")
olderDeltaTableDF = spark.read.format("delta").option("versionAsOf", 0).load("/tmp/demo/delta_table_cdc") 

# COMMAND ----------

# MAGIC %md
# MAGIC Get the insert and updates since the old version

# COMMAND ----------

display(currentDeltaTableDF.subtract(olderDeltaTableDF))

# COMMAND ----------

# MAGIC %md
# MAGIC Get the deletes and updates since the old version

# COMMAND ----------

display(olderDeltaTableDF.subtract(currentDeltaTableDF))

# COMMAND ----------

# MAGIC %md
# MAGIC Get only the deletes

# COMMAND ----------

display(olderDeltaTableDF.join(currentDeltaTableDF, olderDeltaTableDF.word == currentDeltaTableDF.word, "leftanti"))

# COMMAND ----------


