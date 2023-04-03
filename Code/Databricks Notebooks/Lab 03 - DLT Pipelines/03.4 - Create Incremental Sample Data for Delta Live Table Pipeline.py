# Databricks notebook source
# MAGIC %md
# MAGIC #This Notebook will create sample data to test delta live tables piplelines

# COMMAND ----------

# MAGIC %md
# MAGIC ##Create some sample sales data for delta live tables

# COMMAND ----------

# DBTITLE 1,Define our classroom student variables again
setup_responses = dbutils.notebook.run("./utils/Get-Metadata", 0).split()

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

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS  sales_orders_raw;
# MAGIC 
# MAGIC CREATE TABLE sales_orders_raw
# MAGIC USING JSON
# MAGIC OPTIONS(path "/databricks-datasets/retail-org/sales_orders/");

# COMMAND ----------

dbutils.fs.ls("/databricks-datasets/retail-org/sales_orders/")

# COMMAND ----------

sales_df = spark.sql("SELECT f.customer_id, f.customer_name, f.number_of_line_items,   TIMESTAMP(from_unixtime((cast(f.order_datetime as long)))) as order_datetime,   DATE(from_unixtime((cast(f.order_datetime as long)))) as order_date,   CAST(f.order_number as long) as order_number, f.ordered_products FROM sales_orders_raw f limit 3")
                     
                     
display(sales_df)

sales_df.createOrReplaceTempView("sampledata")


# COMMAND ----------

from pyspark.sql.functions import col, to_date, current_date
from pyspark.sql.functions import rand
import random
# Import date class from datetime module
from datetime import date

random_order_number = random.randrange(999)

sales_df = sales_df.withColumn("order_date", current_date())
sales_df = sales_df.withColumn("order_number",col("order_number") + random_order_number)


display(sales_df)



# COMMAND ----------

#next steps:
#  copy sales dataset from databricks samples to misc folder in container for each student by reading to df and
#  writing df to the misc folder.
#  run the previous few cells and random generator to write new file to the misc foldr which will use autoloader
#  for the sales dlt pipeline.    View Results in Power BI refresh.
#  use autoloader queries to see the execution run!!!
#  Time travel!!

sales_df.write.json(f"{miscPath}/sales.json")

