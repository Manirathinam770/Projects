#import the necessary package 
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as F
import time

kafka_topic_name = "jan29"
kafka_bootstrap_servers = 'localhost:9092'

# create a spark session object
spark = SparkSession \
        .builder \
        .appName("PySpark Structured Streaming with Kafka and Message Format as JSON") \
        .master("local[*]") \
        .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

#create a dataframe 
dataframe1= spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .option("startingOffsets", "latest") \
        .load()

# casting the value into string and select "value","timestamp" column
dataframe5 = dataframe1.select(col("value").cast("string"),"timestamp")



# Define your schema
df_schema = StructType() \
    .add("stock_name", StringType()) \
    .add("Indian_Market_Price", FloatType()) \
    .add("market_details", ArrayType(StructType().add("marketchange", FloatType()).add("marketchangepercent", StringType()).add("data_produced_timestamp", DoubleType())))

# Parse the JSON data and split 'stock_name'
parsed_df = dataframe5.select(
    from_json(col("value"), df_schema).alias("new_df"),
    col("timestamp")) \
   .select(regexp_extract(col("new_df.stock_name"), '^(.*?) \(', 1).alias("Company_Name"),
    regexp_extract(col("new_df.stock_name"), '(.*?)$', 1).alias("Symbol"),

    col("new_df.Indian_Market_Price").alias("Indian_Market_Price"),
    col("new_df.market_details"),
    col("timestamp")
      )

# Explode 'market_details' and rename columns
silver = parsed_df.select(
    "Company_Name",
    "Symbol",
    "Indian_Market_Price",
    explode("market_details").alias("market_detail"),
    "timestamp"
).select(
    "Company_Name",
    "Symbol",
    "Indian_Market_Price",
    col("market_detail.marketchange").alias("Market_Price_Change"),
    col("market_detail.marketchangepercent").alias("Market_ChangePercent"),
    "timestamp"
    )

# Add date and time columns from confluent_timestamp
silver = silver.withColumn("Date", date_format(col("timestamp"), "yyyy-MM-dd"))
silver = silver.withColumn("Time", date_format(col("timestamp"), "HH:mm:ss"))

# apply window and aggregation 
golddf = (                 
  silver
    .groupBy(
      silver.Symbol,
      window(silver.timestamp, "1 minute"))
    .agg(F.max("Indian_Market_Price").alias("max_price")))
# write the streaming data from "goldDF" to the console in complete mode
golddf.writeStream \
    .format("console")\
    .outputMode("complete")\
    .start()\
    .awaitTermination()

