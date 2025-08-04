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

tables = ["custom_event_object_with_ids__c"]

for table_name in tables:
    # 1. Get max LastModifiedDate from the table
    df = spark.sql(f"SELECT MAX(LastModifiedDate) AS last_modified FROM {table_name}")
    max_lm_date = df.collect()[0]["last_modified"]
    
    if max_lm_date is None:
        print(f"[WARN] No data found in table '{table_name}', skipping watermark update.")
        continue

    formatted_date = max_lm_date.isoformat()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
