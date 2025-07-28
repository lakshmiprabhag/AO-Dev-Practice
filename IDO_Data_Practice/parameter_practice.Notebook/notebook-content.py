# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5ff92c5e-15c1-4f8e-ac83-4f040a3944d9",
# META       "default_lakehouse_name": "Practice_lakehouse",
# META       "default_lakehouse_workspace_id": "63f70da2-ae03-49e1-9d3e-f7e0d1980727",
# META       "known_lakehouses": [
# META         {
# META           "id": "5ff92c5e-15c1-4f8e-ac83-4f040a3944d9"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

LName="Lopaz"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df= spark.read.table ("DimCustomer")
df=df.where(df.LastName== LName)
display(df)

df.write.mode("overwrite").format("delta").saveAsTable("DimCustomerExtract")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
