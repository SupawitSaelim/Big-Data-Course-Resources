from pyspark.sql import SparkSession

# Create Spark Session
spark = SparkSession.builder.appName("ExampleApp").getOrCreate()

# Read CSV file
read_file = spark.read.format("csv")\
    .option("header", "true")\
    .load("fb_live_thailand.csv")

# Select specific columns
sqlDF = read_file.select("status_published", "num_reactions")

# Show result
sqlDF.show(10)  # Replace 10 with the number of rows you want to display
