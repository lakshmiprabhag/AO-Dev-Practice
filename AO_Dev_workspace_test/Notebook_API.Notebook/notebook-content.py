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

import requests
import json
import pandas as pd
from pyspark.sql import SparkSession

auth_token=auth_token
lakehouse_path = "Tables/onestream_metadatEntity"  

url = "https://aofoundation-dev.onestreamcloud.com/onestreamapi/api/DataProvider/GetAdoDataSetForAdapter?api-version=5.2.0"

payload = json.dumps({
  "BaseWebServerUrl": "https://aofoundation-dev.onestreamcloud.com/onestreamweb",
  "ApplicationName": "AO_TEST2",
  "IsSystemLevel": "False",
  "AdapterName": "Endpoint_Metadata",
  "ResultDataTableName": "ResultsTable",
  "CustomSubstVarsAsCommaSeparatedPairs": "EP_Dimension=Entity"
})
headers = {
  'Authorization': f'Bearer {auth_token}',
  'Content-Type': 'application/json'
}

response = requests.request("POST", url, headers=headers, data=payload)

data_json = response.json()
df = pd.DataFrame(data_json["ResultsTable"])


# -------------------------
# WRITE TO FABRIC LAKEHOUSE
# -------------------------
spark = SparkSession.builder.getOrCreate()
spark_df = spark.createDataFrame(df)

# Save as Delta table in Lakehouse
spark_df.write.format("delta").mode("overwrite").save(lakehouse_path)

print(f"✅ Data saved to Lakehouse table at: {lakehouse_path}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
