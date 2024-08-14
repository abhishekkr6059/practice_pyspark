# Databricks notebook source
# MAGIC %md
# MAGIC - Write a pypsark code to retrieve the top three cities that have the highest number of completed trade orders listed in descending order. Output the city name and the corresponding number of completed trade orders.

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

trades_schema = StructType([
 StructField("order_id", IntegerType(), True),
 StructField("user_id", IntegerType(), True),
 StructField("price", FloatType(), True),
 StructField("quantity", IntegerType(), True),
 StructField("status", StringType(), True),
 StructField("timestamp", StringType(), True)
])
 
users_schema = StructType([
 StructField("user_id", IntegerType(), True),
 StructField("city", StringType(), True),
 StructField("email", StringType(), True),
 StructField("signup_date", StringType(), True)
])
 
trades_data = [
 (100101, 111, 9.80, 10, 'Cancelled', '2022-08-17 12:00:00'),
 (100102, 111, 10.00, 10, 'Completed', '2022-08-17 12:00:00'),
 (100259, 148, 5.10, 35, 'Completed', '2022-08-25 12:00:00'),
 (100264, 148, 4.80, 40, 'Completed', '2022-08-26 12:00:00'),
 (100305, 300, 10.00, 15, 'Completed', '2022-09-05 12:00:00'),
 (100400, 178, 9.90, 15, 'Completed', '2022-09-09 12:00:00'),
 (100565, 265, 25.60, 5, 'Completed', '2022-12-19 12:00:00')
]
 
users_data = [
 (111, 'San Francisco', 'rrok10@gmail.com', '2021-08-03 12:00:00'),
 (148, 'Boston', 'sailor9820@gmail.com', '2021-08-20 12:00:00'),
 (178, 'San Francisco', 'harrypotterfan182@gmail.com', '2022-01-05 12:00:00'),
 (265, 'Denver', 'shadower_@hotmail.com', '2022-02-26 12:00:00'),
 (300, 'San Francisco', 'houstoncowboy1122@hotmail.com', '2022-06-30 12:00:00')
]

# COMMAND ----------

trades_df = spark.createDataFrame(trades_data, schema=trades_schema)
display(trades_df)

# COMMAND ----------

users_df = spark.createDataFrame(users_data, schema=users_schema)
display(users_df)

# COMMAND ----------

# MAGIC %md
# MAGIC - Retrieve the top three cities that have the highest number of completed trade orders listed in descending order
# MAGIC |city|order_count|
# MAGIC |----|-----------|
# MAGIC |San Francisco|3|
# MAGIC |Boston|2|
# MAGIC |Denver|1|

# COMMAND ----------

res_df = (
    trades_df.join(users_df, ["user_id"])
    .filter(col("status") == "Completed")
    .groupBy("city")
    .agg(count("order_id").alias("order_count"))
    .select("city", "order_count")
    .orderBy(col("order_count").desc())
    .limit(3)
)
display(res_df)
