# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "39ee1fda-b6c7-4b9d-8525-1829c8ce7b60",
# META       "default_lakehouse_name": "Shopping_Lakehouse",
# META       "default_lakehouse_workspace_id": "63f70da2-ae03-49e1-9d3e-f7e0d1980727",
# META       "known_lakehouses": [
# META         {
# META           "id": "39ee1fda-b6c7-4b9d-8525-1829c8ce7b60"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

df= spark.read.format("csv").option("header","True").load("Files/Shopping.csv")
df.schema

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import *

schema = StructType([StructField('Date', StringType(), True), 
StructField('Subtype', StringType(), True), 
StructField('PurchaseMethod', StringType(), True), 
StructField('Out', StringType(), True)])

dfs = spark.readStream.option("header","True").schema(schema).format("csv").load("Files/Shop*.csv")
deltatablepath="Tables/Shop_table"
query= dfs.writeStream.format("delta").outputMode("append")\
        .option("checkpointLocation",deltatablepath+'/_checkpoint')\
        .option("path",deltatablepath).start()
query.awaitTermination()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
