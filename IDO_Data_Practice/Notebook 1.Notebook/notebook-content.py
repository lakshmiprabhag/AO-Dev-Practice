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

df = spark.sql("SELECT * FROM Practice_lakehouse.DimCustomer LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import *
schemachage=StructType([StructField('Country', StringType(), True), 
                        StructField('Location', StringType(), True), 
                        StructField('Actual', IntegerType(), True)])
df = spark.read.format("csv").option("header","true").schema(schemachage).load("Files/MtoMActual.csv")
# df now is a Spark DataFrame containing CSV data from "Files/MtoMActual.csv".
display(df)
df2=df.write.format("delta").saveAsTable("MtoMActualStruct")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *
df3=spark.sql("select * from mtomactual")

df4=df3.withColumn("ColDate",current_date())
display(df4)
display(df4.select(df4.Country,df4.Location, df4.Actual,date_format(df4.ColDate,"EEE d MMM yyyy").alias("Formateddatecol")))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.write.mode("overwrite").format("csv").save("Files/MtoMActual2.csv")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df2=df.write.mode("overwrite").format("delta").saveAsTable("MtoMActual")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df3=spark.read.table("MtoMActual")
display(df3)
df3.schema

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df=spark.read.csv("abfss://Ido_data_Practice_workspace@onelake.dfs.fabric.microsoft.com/Practice_lakehouse.Lakehouse/Files/MtoMActual2.csv/part-00000-8a24ad87-a012-42f4-a502-352f3e58855b-c000.csv")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.csv("abfss://Ido_data_Practice_workspace@onelake.dfs.fabric.microsoft.com/Practice_lakehouse.Lakehouse/Files/MtoMActual.csv")

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.load("abfss://Ido_data_Practice_workspace@onelake.dfs.fabric.microsoft.com/Practice_lakehouse.Lakehouse/Files/MtoMActual.csv",format="csv",header=True)
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df=spark.read.table("MtoMActual")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df2=spark.read.format("delta").load("Tables/mtomactual")
display(df2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from mtomactual

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ABFS path: abfss://Ido_data_Practice_workspace@onelake.dfs.fabric.microsoft.com/Practice_lakehouse.Lakehouse/Files/MtoMActual.csv

# CELL ********************

df.schema

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.collect()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.summary()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df=spark.read.table("dimgeography")
dfNSW=(df.select("City","StateProvinceCode"). where(df.StateProvinceCode == "NSW"))
display(dfNSW)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dfDE= df.select(df.City,df.StateProvinceCode.alias("State"),df.CountryRegionCode)
display(dfDE.where(dfDE.CountryRegionCode == 'DE'))
dfUnion= dfNSW.unionByName(dfDE,allowMissingColumns=True)
display(dfUnion)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df=spark.read.table("DimCustomer")
dfCustomer=df.select("CustomerKey", "GeographyKey", "FirstName" ,"LastName")
display(dfCustomer)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df2= spark.read.table("DimGeography")
dfGeography=df2.select(df2.GeographyKey.alias("GeoKey"),df2.City,df2.EnglishCountryRegionName)
display(dfGeography)
dfjoin= dfCustomer.join(dfGeography,dfCustomer.GeographyKey == dfGeography.GeoKey ,"full")
display(dfjoin.where(dfjoin.GeographyKey.isNull()))
display(dfjoin.where(dfjoin.GeoKey.isNull()))
display(dfjoin.groupby("LastName").count().where("count>1"))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
