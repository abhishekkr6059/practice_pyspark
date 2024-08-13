# Databricks notebook source
# MAGIC %md
# MAGIC - Write a Pyspark program to find Employees Earning More Than Their Managers

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# Define the schema for the "employees"
employees_schema = StructType([
StructField("id", IntegerType(), True),
StructField("name", StringType(), True),
StructField("salary", IntegerType(), True),
StructField("managerId", IntegerType(), True)
])
 
# Define data for the "employees"
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

res_df = (
    employees_df.alias("emp_df1").join(employees_df.alias("emp_df2"), col("emp_df1.managerId") == col("emp_df2.id"))
    .filter(col("emp_df1.salary") > col("emp_df2.salary"))
    .select("emp_df1.name")
)
display(res_df)
