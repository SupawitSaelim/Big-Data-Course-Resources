from pyspark.sql import SparkSession
from pyspark.sql.functions import split, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Streaming Data") \
    .getOrCreate()

# Define the schema
file_schema = StructType([
    StructField("status_id", StringType(), True),
    StructField("status_type", StringType(), True),
    StructField("status_published", StringType(), True),
    StructField("num_reactions", StringType(), True),
    StructField("num_comments", StringType(), True),
    StructField("num_shares", StringType(), True),
    StructField("num_likes", StringType(), True),
    StructField("num_loves", StringType(), True),
    StructField("num_wows", StringType(), True),
    StructField("num_hahas", StringType(), True),
    StructField("num_sads", StringType(), True),
    StructField("num_angrys", StringType(), True)
])

# Read the streaming data
lines = spark \
    .readStream \
    .format("csv") \
    .option("maxFilesPerTrigger", 1) \
    .option("header", True) \
    .schema(file_schema) \
    .load("C:/Users/Supawit/Desktop/Big_Data_Lecture/SourceandSink/data/stream")

# Add additional columns and transformations
words = lines \
    .withColumn("date", split(lines["status_published"], " ").getItem(1)) \
    .withColumn("timestamp", current_timestamp()) \
    .withWatermark("timestamp", "10 seconds")

# Group by and aggregate
wordCounts = words \
    .groupBy("date", "status_type", "timestamp") \
    .count()

# Write the results to CSV
query = wordCounts \
    .writeStream \
    .format("csv") \
    .option("path", "C:/Users/Supawit/Desktop/Big_Data_Lecture/SourceandSink/data/savetofile") \
    .trigger(processingTime='5 seconds') \
    .option("checkpointLocation", "C:/Users/Supawit/Desktop/Big_Data_Lecture/SourceandSink/data/savetofile/checkpoint") \
    .outputMode("append") \
    .option("truncate", False) \
    .start()

# Await termination
query.awaitTermination()