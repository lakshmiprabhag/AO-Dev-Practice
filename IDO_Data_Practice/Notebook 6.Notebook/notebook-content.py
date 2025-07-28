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

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Practice_lakehouse.dimgeography")
df= df.write.mode("overwrite").format("delta").partitionBy("CountryRegionCode").saveAsTable("DimGeographyPartition")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dfpartition= spark.read.format("parquet").load("Tables/dimgeographypartition/CountryRegionCode=FR")
display(dfpartition)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
