from pyspark.sql import SparkSession
import os

# สร้าง SparkSession
spark = SparkSession.builder.getOrCreate()

# สร้าง RDD
alphabet = [('a', 1), ('b', 2), ('c', 3), ('a', 1), ('b', 2)]
rdd = spark.sparkContext.parallelize(alphabet)

# ระบุเส้นทางโฟลเดอร์สำหรับบันทึก
output_folder = "output_folder"

# บันทึก RDD ลงในไฟล์
rdd.saveAsTextFile(output_folder)

print(f"Saved RDD to: {output_folder}")

