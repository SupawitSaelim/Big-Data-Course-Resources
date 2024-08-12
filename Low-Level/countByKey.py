from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
alphabet = [('a', 1), ('b', 2), ('c', 3), ('a', 1), ('b', 2)]
rdd = spark.sparkContext.parallelize(alphabet)

# ใช้ countByKey เพื่อคำนวณจำนวนค่าที่มีคีย์เดียวกัน
count = rdd.countByKey()

# แสดงผลลัพธ์
print("Count by Key:")
print(count)
