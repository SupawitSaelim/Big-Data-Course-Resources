from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
alphabet = [('a', 1), ('b', 2), ('c', 3), ('a', 1), ('b', 2)]
rdd = spark.sparkContext.parallelize(alphabet)

# ใช้ first เพื่อดึงแถวแรกจาก RDD
first = rdd.first()

# แสดงผลลัพธ์
print("First element of RDD:")
print(first) #('a', 1)
