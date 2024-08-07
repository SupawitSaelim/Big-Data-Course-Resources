from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split
from pyspark.sql.types import StructType, StringType, StructField

# Initialize SparkSession
spark = SparkSession.builder.appName("FileSourceStreamingExample").getOrCreate()

# Define the schema for the CSV files
schema = StructType([
StructField("status_id", StringType(), True),
StructField("status_type", StringType(), True),
StructField("status_published", StringType(), True)

# Add more StructField definitions as necessary
])

# Read from the CSV file source in a streaming fashion
lines = spark.readStream.format("csv") \
.option("maxFilesPerTrigger", 1) \
.option("header", True) \
.option("path", "data") \
.schema(schema) \
.load()

# Extract the date from the 'status_published' column
words = lines.withColumn("date", split(lines["status_published"], " ").getItem(0))

# Group by 'date' and 'status_type', then count the occurrences
wordCounts = words.groupBy("date", "status_type").count()

# Start running the query that prints the word counts to the console
query = wordCounts.writeStream \
.outputMode("complete") \
.format("console") \
.start()

# Await termination of the query
query.awaitTermination()