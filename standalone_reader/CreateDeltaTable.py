# Databricks notebook source
# MAGIC %md 
# MAGIC Load data into Pyspark Dataframe

# COMMAND ----------

## PROVIDE A DELTA PATH FOR OUTPUT
delta_path = ""

# COMMAND ----------

df = (spark
      .read
      .format("csv")
      .option("inferSchema", True)
      .option("header", True)
      .load("/databricks-datasets/bikeSharing/data-001/hour.csv")
     
     )
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Write data to Delta Table

# COMMAND ----------

df.write.format("delta").save(delta_path)

# COMMAND ----------


