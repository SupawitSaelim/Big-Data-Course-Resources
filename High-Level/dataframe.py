from pyspark.sql import SparkSession

# Create Spark Session
spark = SparkSession.builder.appName("ExampleApp").getOrCreate()

# Read file
read_file = spark.read.format("csv")\
    .option("header", "True")\
    .load("fb_live_thailand.csv")

# Print schema
read_file.printSchema()
