from pyspark.sql import SparkSession
from pyspark.sql.functions import window, count, to_timestamp, col

# Initialize Spark session
spark = SparkSession.builder \
    .appName("StructuredStreamingExample") \
    .getOrCreate()

# Define the schema for the CSV files
schema = "Table STRING,status_id STRING, status_type STRING, status_published STRING, num_reactions INT, num_comments INT"

# Read streaming data from CSV files
df = spark.readStream \
    .format("csv") \
    .option("header", "true") \
    .schema(schema) \
    .load("C:/Users/oatsu/Documents/Big-Data-Course-Resources/data")

# Convert status_published to TIMESTAMP with correct format
df = df.withColumn("status_published", to_timestamp(col("status_published"), "M/d/yyyy H:mm"))

# Define the processing logic
processed_df = df.groupBy(
    window("status_published", "1 hour"),  # Group by 1-hour windows
    "status_type"
).agg(count("*").alias("count"))

# Write the output to the console
query = processed_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .start()

# Await termination of the streaming query
query.awaitTermination()
