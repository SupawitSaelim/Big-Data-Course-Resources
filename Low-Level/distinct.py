from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
rdd = spark.sparkContext.textFile('fb_live_thailand.csv')
print("Number of partitions before distinct: " + str(rdd.getNumPartitions()))

distinct_rdd = rdd.distinct()
print("Number of partitions after distinct: " + str(distinct_rdd.getNumPartitions()))
print("Distinct RDD sample:")
print(distinct_rdd.take(5)) 


"""
การทำงานของโค้ด:
rdd.getNumPartitions(): แสดงจำนวน partitions ของ RDD ก่อนและหลังการใช้ distinct.
 จำนวน partitions อาจเปลี่ยนแปลงหลังจากการใช้ distinct เนื่องจาก Spark 
 อาจมีการรีดทรัพยากรเพื่อจัดการกับข้อมูลที่ไม่ซ้ำกัน.

distinct_rdd = rdd.distinct(): สร้าง RDD ใหม่ที่มีเฉพาะแถวที่ไม่ซ้ำกันจาก RDD เดิม.

distinct_rdd.take(5): แสดงตัวอย่าง 5 แถวแรกจาก RDD ที่มีข้อมูลไม่ซ้ำกัน.
"""