from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
alphabet = [('a', 1), ('b', 2), ('c', 3), ('a', 1), ('b', 2)]
rdd = spark.sparkContext.parallelize(alphabet)

max_value = rdd.max()
print("Maximum value in RDD:")
print(max_value)

min_value = rdd.min()
print("Minimum value in RDD:")
print(min_value)
