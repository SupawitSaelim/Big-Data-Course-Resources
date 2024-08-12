from pyspark.sql import SparkSession
from operator import add

# สร้าง SparkSession
spark = SparkSession.builder.getOrCreate()

# สร้าง RDD จากรายการที่มีการใช้คีย์-ค่า
alphabet = [('a', 1), ('b', 2), ('c', 3), ('a', 1), ('b', 2)]
rdd = spark.sparkContext.parallelize(alphabet, 4)

# ใช้ foldByKey เพื่อรวมค่าตามคีย์ โดยใช้ฟังก์ชัน add
fold = sorted(rdd.foldByKey(0, add).collect())

# แสดงผลลัพธ์
print(fold)
