from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
alphabet = [('a', 1), ('b', 2), ('c', 3), ('a', 1), ('b', 2)]
rdd = spark.sparkContext.parallelize(alphabet)

# ใช้ lookup เพื่อค้นหาค่าทั้งหมดที่สัมพันธ์กับคีย์ 'a'
look = rdd.lookup('a')

# แสดงผลลัพธ์
print("Lookup for key 'a':")
print(look)
