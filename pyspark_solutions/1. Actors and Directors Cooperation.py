# Databricks notebook source
# MAGIC %md
# MAGIC - Write a pyspark program to generate a report that provides pairs (actor_id, director_id) where the actor has cooperated with the director at least 3 times

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

schema = StructType([
StructField("ActorId",IntegerType(),True),
StructField("DirectorId",IntegerType(),True),
StructField("Movie",StringType(),True)
])
 
data = [
(1, 1, "A"),
(1, 1, "B"),
(1, 1, "C"),
(1, 2, "D"),
(1, 2, "E"),
(2, 1, "F"),
(2, 1, "G")
]

df = spark.createDataFrame(data, schema=schema)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC - Implement a pyspark program to find pairs where actors and directors have collaborated at least 3 times.
# MAGIC
# MAGIC | ActorId	| DirectorId  | count |
# MAGIC |---------|-------------|-------|
# MAGIC |    1	  |     1	      |   3   |

# COMMAND ----------

res_df = (
    df.groupBy(["ActorId","DirectorId"]).count()
    .filter(col("count") >= 3)
)
display(res_df)
