from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CSV to DataFrame") \
    .getOrCreate()

file_path = "./fb_live_thailand.csv"  
read_file = spark.read.csv(file_path, header=True, inferSchema=True)

sqlDF = read_file.select("status_id", "status_type")
sqlDF.show(10)
spark.stop()
