# Databricks notebook source
# MAGIC %md
# MAGIC #Move Databricks Sample Retail data to Managed Bronze Folder

# COMMAND ----------

# MAGIC %md
# MAGIC ##Configure environmental variables

# COMMAND ----------

# DBTITLE 1,Set User Specific Variables for Isolation
setup_responses = dbutils.notebook.run("../utils/Get-Metadata", 0).split()

local_data_path = setup_responses[0]
dbfs_data_path = setup_responses[1]
database_name = setup_responses[2]

print(f"Local data path is {local_data_path}")
print(f"DBFS path is {dbfs_data_path}")
print(f"Database name is {database_name}")
      
#print("Local data path is {}".format(local_data_path))
#print("DBFS path is {}".format(dbfs_data_path))
#print("Database name is {}".format(database_name))

# COMMAND ----------

dbutils.fs.rm(f'{local_data_path}/bronze',True)
dbutils.fs.rm(f'{local_data_path}/silver',True)
dbutils.fs.rm(f'{local_data_path}/gold',True)
dbutils.fs.rm(f'{local_data_path}/_checkpoints',True)
dbutils.fs.rm(f'{local_data_path}/_schema',True)


# COMMAND ----------


dbutils.fs.mkdirs(f'{local_data_path}/bronze')
dbutils.fs.mkdirs(f'{local_data_path}/silver')
dbutils.fs.mkdirs(f'{local_data_path}/gold')
dbutils.fs.mkdirs(f'{local_data_path}/_checkpoints')
dbutils.fs.mkdirs(f'{local_data_path}/_schema')




# COMMAND ----------

display(dbutils.fs.ls(dbfs_data_path))

# COMMAND ----------

display(dbutils.fs.ls(local_data_path))

# COMMAND ----------


bronzePath = f"{local_data_path}bronze"
silverPath = f"{local_data_path}silver"
goldPath = f"{local_data_path}gold"

print(bronzePath)
print(silverPath)
print(goldPath)


# COMMAND ----------

dbutils.fs.ls(bronzePath)

# COMMAND ----------

# MAGIC %md
# MAGIC ##dbutils Commands
# MAGIC These commands are great for moving files around from Databricks managed folders to Azure Data Lake Store folders.
# MAGIC 
# MAGIC You can create folders (mkdirs), remove folders (rm), list contents of folders (ls) and copy files to/from folders (cp)

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Now Copy databricks sample retail data to Bronze folder in ADLS

# COMMAND ----------

# copy the Customers source data into the bronze folder
dbutils.fs.cp ("/databricks-datasets/retail-org/customers/", f"{bronzePath}/customers/", True) 

# confirm the file is where it should be
dbutils.fs.ls(f"{bronzePath}/customers")

# COMMAND ----------

display(dbutils.fs.ls(f"{bronzePath}/customers"))

# COMMAND ----------

# copy the Customers source data into the bronze folder
dbutils.fs.cp ("/databricks-datasets/retail-org/sales_orders/", f"{bronzePath}/sales_orders/", True) 

# confirm the file is where it should be
display(dbutils.fs.ls(f"{bronzePath}/sales_orders"))

# COMMAND ----------

# MAGIC %md
# MAGIC #Ready to Start Delta Live Tables Pipeline
# MAGIC At this point, we have data in our bronze folder and can now execute the 03.2 - sample-DLT-pipeline-notebook within a delta live table pipeline.
# MAGIC 
# MAGIC This entails creating a pipeline and point the pipeline to this notebook for execution.
