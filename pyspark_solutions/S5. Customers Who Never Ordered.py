# Databricks notebook source
# MAGIC %md
# MAGIC - Write a Pyspark program to find all customers who never ordered.

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

customers_schema = StructType([
StructField("id", IntegerType(), True),
StructField("name", StringType(), True)
])
 
customers_data = [
(1, 'Joe'),
(2, 'Henry'),
(3, 'Sam'),
(4, 'Max')
]

orders_schema = StructType([
StructField("id", IntegerType(), True),
StructField("customerId", IntegerType(), True)
])
 
orders_data = [
(1, 3),
(2, 1)
]

# COMMAND ----------

customers_df = spark.createDataFrame(customers_data, schema=customers_schema)
display(employees_df)

# COMMAND ----------

orders_df = spark.createDataFrame(orders_data, schema=orders_schema)
display(orders_df)

# COMMAND ----------

# MAGIC %md
# MAGIC - Find the customers who never ordered
# MAGIC |id|name|
# MAGIC |--|----|
# MAGIC |2|Henry|
# MAGIC |4|Max|

# COMMAND ----------

res_df = (
    customers_df.join(orders_df, orders_df["customerId"] == customers_df["id"], "left_anti")
)
display(res_df)

# COMMAND ----------

# MAGIC %md
# MAGIC - 
