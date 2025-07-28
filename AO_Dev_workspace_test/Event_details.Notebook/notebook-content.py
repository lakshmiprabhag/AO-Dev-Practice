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


from pyspark.sql import SparkSession

# Read both tables
fact_df = spark.sql("SELECT Event_ID__c, Event_Status__c, Event_Start_Date__c FROM fact_event_c")
dim_df = spark.sql("SELECT Event_ID__c, Event_Display_Name__c, Country__c, City__c, Event_Owner_Name__c, Event_Specialist__c FROM Dev_AO_Lakehouse.dim_event_c")

# Join and select required columns
event_details_df = fact_df.join(dim_df, "Event_ID__c").select(
    "Event_ID__c", "Event_Display_Name__c", "Event_Start_Date__c",
    "Event_Status__c", "Country__c", "City__c",
    "Event_Owner_Name__c", "Event_Specialist__c"
)

# Write to Delta table
event_details_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("Event_Details")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
