import os
from pyspark.sql import SparkSession

# Explicitly set Hadoop home directory
os.environ['HADOOP_HOME'] = 'C:\\hadoop'

# Create Spark session
spark = SparkSession.builder \
    .appName("WordCountStreaming") \
    .config("spark.hadoop.home.dir", "C:\\hadoop") \
    .getOrCreate()

# Verify Spark session
print("Spark session started successfully")
