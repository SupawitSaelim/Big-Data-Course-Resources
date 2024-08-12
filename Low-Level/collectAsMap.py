from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
alphabet = [('a', 1), ('b', 2), ('c', 3), ('a', 1), ('b', 2)]
rdd = spark.sparkContext.parallelize(alphabet)

# ใช้ collectAsMap เพื่อรวบรวมข้อมูลเป็นพจนานุกรม
col_asmap = rdd.collectAsMap()

# แสดงผลลัพธ์
print("Collected As Map:")
print(col_asmap)
