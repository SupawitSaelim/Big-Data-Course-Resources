from pyspark.sql import SparkSession

# Create or get the existing SparkSession
spark = SparkSession.builder.getOrCreate()

# Define a list and parallelize it to create an RDD with 4 partitions
alphabet = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h']
rdd = spark.sparkContext.parallelize(alphabet, 4)

# Print the number of partitions of the first RDD
print('Number of partitions for rdd: ' + str(rdd.getNumPartitions()))

# Create an RDD by reading a CSV file with 5 partitions
rdd2 = spark.sparkContext.textFile('fb_live_thailand.csv', 5)

# Print the number of partitions of the second RDD
print('Number of partitions for rdd2: ' + str(rdd2.getNumPartitions()))
