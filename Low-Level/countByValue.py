from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
alphabet = [('a', 1), ('b', 2), ('c', 3), ('a', 1), ('b', 2)]
rdd = spark.sparkContext.parallelize(alphabet)

# ใช้ countByValue เพื่อคำนวณจำนวนค่าที่มีคู่เดียวกัน
count_val = rdd.countByValue()

# แสดงผลลัพธ์
print("Count by Value:")
print(count_val)

#Count by Value:
# defaultdict(<class 'int'>, {('a', 1): 2, ('b', 2): 2, ('c', 3): 1})