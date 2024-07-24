# from pyspark.sql import SparkSession
# spark = SparkSession.builder.getOrCreate()
# alphabet = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h']

# rdd = spark.sparkContext.textFile('fb_live_thailand.csv', 5)

# count_distinct = rdd.distinct().count()
# print('Count of distinct elements: ' + str(count_distinct))

# filter_rdd = rdd.filter(lambda x: x.splite(',')[1] == 'link').collect()
# print('Filtered RDD: ' + str(filter_rdd))

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

rdd = spark.sparkContext.textFile('fb_live_thailand.csv', 5)

count_distinct = rdd.distinct().count()
print('Count of distinct elements: ' + str(count_distinct))

# Corrected the lambda function to use .split(',') and check the second element (index 1)
filter_rdd = rdd.filter(lambda x: x.split(',')[1] == 'link').collect()
print('Filtered RDD: ' + str(filter_rdd))
