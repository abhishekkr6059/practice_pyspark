# Databricks notebook source
# MAGIC %md
# MAGIC - Write a Pyspark program to find Employees Earning More Than Their Managers

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

employees_schema = StructType([
StructField("id", IntegerType(), True),
StructField("name", StringType(), True),
StructField("salary", IntegerType(), True),
StructField("managerId", IntegerType(), True)
])
 
employees_data = [
(1, 'Joe', 70000, 3),
(2, 'Henry', 80000, 4),
(3, 'Sam', 60000, None),
(4, 'Max', 90000, None)
]

# COMMAND ----------

employees_df = spark.createDataFrame(employees_data, schema=employees_schema)
display(employees_df)

# COMMAND ----------

# MAGIC %md
# MAGIC - Implement a PYSPARK program to find the employee who is earning more than their manager
# MAGIC |name|
# MAGIC |----|
# MAGIC |Joe|

# COMMAND ----------


