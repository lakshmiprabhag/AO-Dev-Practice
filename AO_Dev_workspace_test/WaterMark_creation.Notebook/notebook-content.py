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
from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("CreateWaterMarkTable").getOrCreate()

spark.sql("""
create TABLE watermarktable(
    TableName string,
    watermarkvalue timestamp
) """)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp

spark = SparkSession.builder.getOrCreate()

# Sample data
dfactual_data = [
    {'TableName': 'custom_event_object_with_ids__c', 'watermarkvalue': '1/1/2010 12:00:00 AM'}
]

# Create initial DataFrame
dfactual = spark.createDataFrame(dfactual_data)

# Convert 'watermarkvalue' string to timestamp
dfactual = dfactual.withColumn("watermarkvalue", to_timestamp("watermarkvalue", "M/d/yyyy h:mm:ss a"))

# Write to the Delta table (overwrite mode)
dfactual.write.mode("overwrite").format("delta").saveAsTable("watermarktable")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from notebookutils import mssparkutils
from pyspark.sql.functions import col, lit, to_timestamp, when
from datetime import datetime

# Step 1: Read parameters from pipeline
#table_name = mssparkutils.notebook.getContext().parameters.get("table_name")
#last_modified_time = mssparkutils.notebook.getContext().parameters.get("last_modified_time")

# Step 2: Define the stored-procedure-like logic

def usp_write_watermark(table_name: str, last_modified_time: str):
    """
    Simulates a stored procedure in PySpark.
    Updates or inserts a watermark value for a given table.
    """
    # Read existing watermark table
    watermark_df = spark.table("watermarktable")

    # Check if the table already exists
    exists = watermark_df.filter(col("TableName") == table_name).count() > 0

    if exists:
        # Update the WatermarkValue
        updated_df = watermark_df.withColumn(
            "WatermarkValue",
            when(col("TableName") == table_name, to_timestamp(lit(last_modified_time)))
            .otherwise(col("WatermarkValue"))
        )
    else:
        # Insert new record
        new_row = spark.createDataFrame(
            [(table_name, datetime.strptime(last_modified_time, "%Y-%m-%d %H:%M:%S"))],
            ["TableName", "WatermarkValue"]
        )
        updated_df = watermark_df.union(new_row)

    # Overwrite the table with updated data
    updated_df.write.mode("overwrite").format("delta").saveAsTable("watermarktable")
    print(f"Watermark updated for table: {table_name}")

# Step 3: Run it
usp_write_watermark(table_name, last_modified_time)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, lit, when
from datetime import datetime

# Convert string timestamp to datetime object
formatted_time = datetime.strptime(LastModifiedtime, "%Y-%m-%d %H:%M:%S")

# Load existing watermark table
watermark_df = spark.read.table("watermarktable")

# Check if TableName exists
exists = watermark_df.filter(col("TableName") == TableName).count() > 0

if exists:
    # Update existing record
    updated_df = watermark_df.withColumn(
        "WatermarkValue",
        when(col("TableName") == TableName, lit(formatted_time))
        .otherwise(col("WatermarkValue"))
    )
else:
    # Insert new record
    new_row = spark.createDataFrame([(TableName, formatted_time)], ["TableName", "WatermarkValue"])
    updated_df = watermark_df.union(new_row)
    display(updated_df)

# Save updated data back
#updated_df.write.mode("overwrite").format("delta").saveAsTable("watermarktable")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
