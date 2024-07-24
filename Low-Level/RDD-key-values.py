from pyspark import SparkContext, SparkConf

# Initialize Spark context
conf = SparkConf().setAppName("test")
sc = SparkContext(conf=conf)

# Create a simple RDD
rdd = sc.parallelize([1, 2, 3, 4, 5])

# Perform a basic operation
count = rdd.count()
print(f"Count: {count}")

# Stop Spark context
sc.stop()
