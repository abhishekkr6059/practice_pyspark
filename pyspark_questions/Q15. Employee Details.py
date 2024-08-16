# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

data = [
 [1, "Vikas", "Ahlawat", 600000.0, "2013-02-15 11:16:28.290", "IT", "Male"],
 [2, "nikita", "Jain", 530000.0, "2014-01-09 17:31:07.793", "HR", "Female"],
 [3, "Ashish", "Kumar", 1000000.0, "2014-01-09 10:05:07.793", "IT", "Male"],
 [4, "Nikhil", "Sharma", 480000.0, "2014-01-09 09:00:07.793", "HR", "Male"],
 [5, "anish", "kadian", 500000.0, "2014-01-09 09:31:07.793", "Payroll", "Male"],
]
 
schema = StructType([
 StructField("EmployeeID", IntegerType(), True),
 StructField("First_Name", StringType(), True),
 StructField("Last_Name", StringType(), True),
 StructField("Salary", DoubleType(), True),
 StructField("Joining_Date", StringType(), True),
 StructField("Department", StringType(), True),
 StructField("Gender", StringType(), True)
])

# COMMAND ----------

df = spark.createDataFrame(data, schema=schema)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Write a pyspark code for combine FirstName and LastName and display it as "Name" (also include white space between first name & last name)
# MAGIC |First_Name|Last_Name|Name|
# MAGIC |----------|---------|----|
# MAGIC |Vikas|Ahlawat|Vikas Ahlawat|
# MAGIC |nikita|Jain|nikita Jain|
# MAGIC |Ashish|Kumar|Ashish Kumar|
# MAGIC |Nikhil|Sharma|Nikhil Sharma|
# MAGIC |anish|kadian|anish kadian|

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC 2. Get all employee detail from EmployeeDetail table whose "FirstName" start with letter 'a'.
# MAGIC |EmployeeID|First_Name|Last_Name|Salary|Joining_Date|Department|Gender|
# MAGIC |----------|----------|---------|------|------------|----------|------|
# MAGIC |5|anish|kadian|500000|2014-01-09 09:31:07.793|Payroll|Male|

# COMMAND ----------


