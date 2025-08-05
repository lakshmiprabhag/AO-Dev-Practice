# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "2e6163e9-9008-4221-83d0-877139faeaa5",
# META       "default_lakehouse_name": "Dev_AO_Lakehouse",
# META       "default_lakehouse_workspace_id": "095e1e3c-3b29-4e8a-8472-f3f2e711fd22",
# META       "known_lakehouses": [
# META         {
# META           "id": "2e6163e9-9008-4221-83d0-877139faeaa5"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
from pyspark.sql.functions import col, to_timestamp
df= spark.read.table("Dev_AO_Lakehouse.custom_event_object_with_ids__c")
#display(df)
df_conversion= df.select(col("Event_ID__c"),
    col("OwnerId__c"),
    col("Event_Status__c"),
    to_timestamp("Event_Start_Date__c", "dd/MM/yy H:mm").alias("Event_Start_Date__c"),
    to_timestamp("Event_End_Date__c", "dd/MM/yy H:mm").alias("Event_End_Date__c"))


df_fact= df_conversion.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("Fact_Event_C")
df_conversion.show(truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession

# Spark session Creation
spark= SparkSession.builder.appName("ExcludeColumns").getOrCreate()

# Source table
df = spark.sql("SELECT * FROM Dev_AO_Lakehouse.custom_event_object_with_ids__c")


# Columns to exclude
exclude_cols=["Event_Start_Date__c","Event_End_Date__c","Event_Status__c"]

# Select only columns NOT in exclude_cols
selected_cols = [col for col in df.columns if col not in exclude_cols]

# Apply selection
df_selected=df.select(*selected_cols)


df2= df_selected.write.mode("overwrite").format("delta").saveAsTable("Dim_Event_C")
display (df2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Dev_AO_Lakehouse.custom_event_object_with_ids__c LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
