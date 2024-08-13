# Databricks notebook source
# MAGIC %md
# MAGIC - Write a solution to report the name and bonus amount of each employee with a bonus less than 1000.
# MAGIC Return the result table in any order

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

employee_schema = StructType([
 StructField("empId", IntegerType(), True),
 StructField("name", StringType(), True),
 StructField("supervisor", IntegerType(), True),
 StructField("salary", IntegerType(), True)
])
 
employee_data = [
 (3, 'Brad', None, 4000),
 (1, 'John', 3, 1000),
 (2, 'Dan', 3, 2000),
 (4, 'Thomas', 3, 4000)
]
 
bonus_schema = StructType([
 StructField("empId", IntegerType(), True),
 StructField("bonus", IntegerType(), True)
])
 
bonus_data = [
 (2, 500),
 (4, 2000)
]

# COMMAND ----------

employee_df = spark.createDataFrame(employee_data, schema=employee_schema)
display(employee_df)

# COMMAND ----------

bonus_df = spark.createDataFrame(bonus_data, schema=bonus_schema)
display(bonus_df)

# COMMAND ----------

# MAGIC %md
# MAGIC - report the name and bonus amount of each employee with a bonus less than 1000
# MAGIC |name|bonus|
# MAGIC |----|-----|
# MAGIC |Brad|	|
# MAGIC |John|	|
# MAGIC |Dan|500

# COMMAND ----------

res_df = (
    employee_df.join(bonus_df, ["empId"], "left")
    .filter((col("bonus").isNull()) | (col("bonus") < 1000))
    .select("name", "bonus")
)
display(res_df)
