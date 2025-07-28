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

df = spark.read.format("csv").option("header","true").load("Files/DimGeography.csv")
# df now is a Spark DataFrame containing CSV data from "Files/DimGeography.csv".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.write.mode("overwrite").format("delta").saveAsTable("dimgeography")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df2= spark.sql("select * from dimgeography limit 10")
display(df2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#dfreduced=df2.select("City","postalcode")
#dfreduced=df2.select(df2.City,df2.PostalCode)
dfreduced=df2.select(df2.City,df2.PostalCode.alias("PINCode"))
#display(dfreduced.where("City = 'Malabar'"))
#display(dfreduced.filter("PostalCode <= 2036"))
#display(dfreduced.limit(5))
#display(dfreduced.tail(2))
display(df2.withColumn("PINCode",df2.PostalCode)\
            .withColumn("IPLocator",df2.IpAddressLocator.substr(8,12)))
display(df2.withColumn)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df=spark.sql("select * from dimgeography")
display(df)
df.schema

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


df4=df.describe(["GeographyKey","City","PostalCode"]).show()
df5=df.select(df.City,df.PostalCode,df.IpAddressLocator.cast("int"))

display(df4)
display(df5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df2=df.select(df.City, df.StateProvinceName.alias("State"))
display (df2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df3= df.filter(df.GeographyKey >= 200)
display(df3.withColumn("IPLocation",df.IpAddressLocator.substr(1,3)))
display(df3.filter(df.CountryRegionCode == 'FR' | df.CountryRegionCode== 'GB'))
display(df3.filter(df.CountryRegionCode.isin('FR','GB')))
display(df3.filter((df.CountryRegionCode == 'FR')|(df.CountryRegionCode == 'GB')))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *
df=spark.read.table("DimCustomer")
df2=df.select("CustomerKey","NumberCarsOwned","FirstName","LastName","DateFirstPurchase","YearlyIncome")\
        .where (df.YearlyIncome ==130000)
display(df2.withColumn("FullName",concat(df2.FirstName,lit(" "),df2.LastName))\
        .withColumn("FormattedDate",date_format("DateFirstPurchase","EEEE d MMMM yyyy"))
        )
display(df2.sort(df2.NumberCarsOwned.desc(), df2.CustomerKey.asc()))
#, asc(df2.CustomerKey))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *
df3= df.groupBy("EnglishOccupation").avg("YearlyIncome")
df3= df3.withColumnRenamed("avg(YearlyIncome)","AverageIncome")
df3=df3.withColumn("AverageIncome",df3.AverageIncome.cast("int"))

display(df3)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df=spark.read.table("mtomactual")
dfstruct = spark.read.table("mtomactualstruct")
#display(df)
#display(dfstruct)
dfunion=df.union(dfstruct)
display(dfunion.distinct())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
spark= SparkSession.builder.getOrCreate()

dfactual_data= [
    {'Country': 'France', 'ActualTotal': 4000},
    {'Country': 'Italy', 'ActualTotal': 16000},
    {'Country': 'England', 'ActualTotal': 23000}
    ]
dftarget_data= [
    {'Country': 'Germany', 'TargetTotal': 12000},
    {'Country': 'France', 'TargetTotal': 10500},
    {'Country': 'England', 'TargetTotal': 15000}
]

dfactual= spark.createDataFrame(dfactual_data)
dftarget= spark.createDataFrame(dftarget_data)

display(dfactual)
display(dftarget)

dfAct=dfactual.write.mode("overwrite").format("delta").saveAsTable("MtoMActualSum")
dfTar=dftarget.write.mode("overwrite").format("delta").saveAsTable("MtoMTargetSum")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dfacttable= spark.read.table("MtoMActualSum")
dftartable= spark.read.table("MtoMTargetSum")
dfbridge=(dfacttable.select("Country")).union(dftartable.select('Country')).distinct()
display(dfbridge)
dfjoin=(dfbridge.join(dfacttable,'Country',"left"))
#.join(dftartable,'Country',"left"))
#dfjoinfinal=dfjoin.join(dftarget,'Country')
display(dfjoin)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(dfbridge.join(dfacttable,'Country',"left").join(dftartable,'Country',"left"))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
