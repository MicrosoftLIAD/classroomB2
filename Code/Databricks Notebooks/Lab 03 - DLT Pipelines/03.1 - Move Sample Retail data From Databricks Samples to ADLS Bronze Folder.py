# Databricks notebook source
# MAGIC %md
# MAGIC #Move Databricks Sample Retail data to Azure Data Lake Store (ADLS) Bronze Folder

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

studentName = 'richjohn'
storageName = 'stgliadadls'

bronzePath = f"abfss://{studentName}@{storageName}.dfs.core.windows.net/bronze"
silverPath = f"abfss://{studentName}@{storageName}.dfs.core.windows.net/silver"
goldPath = f"abfss://{studentName}@{storageName}.dfs.core.windows.net/gold"
miscPath = f"abfss://{studentName}@{storageName}.dfs.core.windows.net/misc"
print(bronzePath)
print(silverPath)
print(goldPath)
print(miscPath)

# COMMAND ----------

#create a mountpoint for the path to be used for delta live table path location to your individual container per each student
configs = {
  "fs.azure.account.auth.type": "CustomAccessToken",
  "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = f"abfss://{studentName}@{storageName}.dfs.core.windows.net/",
  mount_point = f"/mnt/{studentName}lakehouse",
  extra_configs = configs)

#if it exists then drop with following command
#dbutils.fs.unmount(f"/mnt/{studentName}lakehouse")  


# COMMAND ----------

#list mount points
dbutils.fs.mounts()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Erase and recreate Bronze folder in ADLS to make sure no leftover data

# COMMAND ----------

dbutils.fs.ls(bronzePath)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Delete and Recreate Bronze Folder in ADLS
# MAGIC We want to start with an empty bronze folder for this lab

# COMMAND ----------

#remove files and folder if exists
dbutils.fs.rm(bronzePath)

#Add bronze folder
dbutils.fs.mkdirs(bronzePath)

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

# copy the Customers source data into the bronze folder
dbutils.fs.cp ("/databricks-datasets/retail-org/sales_orders/", f"{bronzePath}/sales_orders/", True) 

# confirm the file is where it should be
display(dbutils.fs.ls(f"{bronzePath}/sales_orders"))

# COMMAND ----------

# MAGIC %md
# MAGIC #Ready to Start Delta Live Tables Pipeline
# MAGIC At this point, we have data in our bronze folder and can now execute the 03.2 - sample-DLT-pipeline-notebook within a delta live table pipeline.
# MAGIC 
# MAGIC This entails creating a pipeline and point to this notebook for execution.
