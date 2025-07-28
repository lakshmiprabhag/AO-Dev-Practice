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
from pyspark.sql.functions import current_timestamp,add_months,expr,col

# Read both tables
fact_event_df = spark.sql("SELECT Event_ID__c, Event_Status__c, Event_Start_Date__c FROM fact_event_c")
dim_event_df = spark.sql("SELECT Event_ID__c,Event_Owner_Name__c FROM Dev_AO_Lakehouse.dim_event_c")

# Join and select required columns
next_4_months= fact_event_df.join(dim_event_df, "Event_ID__c").select( "Event_Owner_Name__c","Event_Status__c")\
                .filter (((fact_event_df.Event_Start_Date__c) >= current_timestamp()) & ((fact_event_df.Event_Start_Date__c)<= add_months(current_timestamp(),4)))\
                .groupBy(fact_event_df.Event_Status__c,dim_event_df.Event_Owner_Name__c).count()\
                .sort(dim_event_df.Event_Owner_Name__c)
#display(next_4_months)
# Write to Delta table
next_4_months.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("My_Events_by_status")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
