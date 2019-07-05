# Databricks notebook source
# MAGIC %md
# MAGIC This is our master bike sharing pipeline. It executes notebooks using the notebook workflow capabilties of databricks. 
# MAGIC 
# MAGIC This notebook will:
# MAGIC  - Execute 
# MAGIC  - Execute 
# MAGIC  - Execute 

# COMMAND ----------

# MAGIC %md 
# MAGIC Load data from source to bronze

# COMMAND ----------

dbutils.notebook.run("01b_BatchSourceToBronze", 0)

# COMMAND ----------

# MAGIC %md 
# MAGIC Load data from bronze to silver

# COMMAND ----------

dbutils.notebook.run("02b_BatchBronzeToSilver", 0)

# COMMAND ----------

# MAGIC %md
# MAGIC Load data from silver to gold

# COMMAND ----------

dbutils.notebook.run("02b_BatchSilver", 0)
