# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as F


# COMMAND ----------


jsonString= """{ 
    "coffee": {
        "region": [
            {"id":1,"name":"John Doe"},
            {"id":2,"name":"Don Joeh"}
        ],
        "country": {"id":2,"company":"ACME"}
    }, 
    "brewing": {
        "region": [
            {"id":1,"name":"John Doe"},
            {"id":2,"name":"Don Joeh"}
        ],
        "country": {"id":2,"company":"ACME"}
    }
     } """
#Create a Dataframe 
df=spark.createDataFrame([(1, jsonString)],["id","value"])     

# COMMAND ----------

#Applying schema for the dataframe
df_schema= StructType([
    StructField('coffee',StructType([
        StructField('region',ArrayType(
            StructType([
                StructField('id',IntegerType(),True),
                StructField('name',StringType(),True)
                ])
               )),
        StructField('country',StructType([
            StructField('id',IntegerType(),True),
            StructField('company',StringType(),True)

        ]))
    ])),
    StructField('brewing',StructType([
        StructField('region',ArrayType(
            StructType([
                StructField('id',IntegerType(),True),
                StructField('name',StringType(),True)

            ])
        )),
        StructField('country',StructType([
            StructField('id',IntegerType(),True),
            StructField('company',StringType(),True)
        ]))
    ]))
])
#parsing the Json data into Struct Type
parsed_df = df.select(
    from_json(col("value"), df_schema).alias("new_df"))

# COMMAND ----------

# display the struct type data 
display(parsed_df)
#print the schema of the Struct type data
parsed_df.printSchema()

# COMMAND ----------

#spilting the new_df struct data into 2 columns named as COFFEE and BREWING
parsed_df1=parsed_df.select(
    col("new_df.coffee").alias("COFFEE"),
    col("new_df.brewing").alias("BREWING")

)
parsed_df1.printSchema()
display(parsed_df1)

# COMMAND ----------

#spliting the COFFEE and BREWING
parsed_df2=parsed_df1.select(
    col("COFFEE.region").alias("COFFEE_REGION"),
    col("COFFEE.country").alias("CoFFEE_COUNTRY"),
    col("BREWING.region").alias("BREWING_REGION"),
    col("BREWING.country").alias("BREWING_COUNTRY")
)
parsed_df2.printSchema()
display(parsed_df2)

# COMMAND ----------

#explode the B_REGION and C_REGION
import pyspark.sql.functions as F

parsed_df3=parsed_df2.withColumn('Coffee_Region', F.explode('COFFEE_REGION'))\
                      .withColumn('Brewing_Region',F.explode('BREWING_REGION'))

parsed_df3.printSchema()
display(parsed_df3)

# COMMAND ----------

#spliting the B_REGION and C_REGION 
parsed_df4=parsed_df3.withColumn('Coffee_REGION_ID',col('Coffee_Region.id')) \
        .withColumn('Coffee_REGION_NAME',col('Coffee_Region.name'))\
        .withColumn('Coffee_REGION_Country_ID',col('Coffee_COUNTRY.id'))\
          .withColumn('Coffee_REGION_Country_COMPANY',col('Coffee_COUNTRY.company')) \
        .withColumn('Brewing_REGION_ID',col('Brewing_Region.id'))\
        .withColumn('Brewing_REGION_NAME',col('Brewing_Region.name'))\
          .withColumn( 'Brewing_REGION_Country_ID',col('Brewing_COUNTRY.id'))\
          .withColumn('Brewing_Region_Country_COMPANY',col('Brewing_COUNTRY.company'))
display(parsed_df4)                     
            

# COMMAND ----------

final_DF=parsed_df4.select('Coffee_REGION_ID','Coffee_REGION_NAME','Coffee_REGION_Country_ID','Coffee_REGION_Country_COMPANY','Brewing_REGION_ID','Brewing_REGION_NAME','Brewing_REGION_Country_ID','Brewing_Region_Country_COMPANY')
display(final_DF)

# COMMAND ----------

final_DF.write.format("delta").saveAsTable("Json_table1")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from json_table1

# COMMAND ----------


