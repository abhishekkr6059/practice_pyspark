# Databricks notebook source
# MAGIC %md
# MAGIC - Write a Pyspark program to report the first name, last name, city, and state of each person in the Person dataframe. If the address of a personId is not present in the Address dataframe, report null instead

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

persons_schema = StructType([
StructField("personId", IntegerType(), True),
StructField("lastName", StringType(), True),
StructField("firstName", StringType(), True)
])
 
addresses_schema = StructType([
StructField("addressId", IntegerType(), True),
StructField("personId", IntegerType(), True),
StructField("city", StringType(), True),
StructField("state", StringType(), True)
])
 
persons_data = [
(1, 'Wang', 'Allen'),
(2, 'Alice', 'Bob')
]
 
addresses_data = [
(1, 2, 'New York City', 'New York'),
(2, 3, 'Leetcode', 'California')
]

# COMMAND ----------

persons_df = spark.createDataFrame(persons_data, schema=persons_schema)
display(persons_df)

# COMMAND ----------

addresses_df = spark.createDataFrame(addresses_data, schema=addresses_schema)
display(addresses_df)

# COMMAND ----------

# MAGIC %md
# MAGIC - Implement a PYSPARK program to join the two df
# MAGIC |firstName|lastName|city|state|
# MAGIC |---------|--------|----|-----|
# MAGIC |Allen|Wang|null|null|
# MAGIC |Bob|Alice|New York City|New York|

# COMMAND ----------

combined_df = (
    persons_df.join(addresses_df, ["personId"], "left")
    .select("firstName", "lastName", "city", "state")
)
display(combined_df)
