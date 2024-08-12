from pyspark.sql import SparkSession

# สร้าง SparkSession
spark = SparkSession.builder.getOrCreate()

# อ่านไฟล์ CSV เป็น RDD
rdd = spark.sparkContext.textFile('fb_live_thailand.csv')

# ตรวจสอบจำนวน partitions ของ RDD
print("Number of partitions: " + str(rdd.getNumPartitions()))

# ใช้ map เพื่อทำการแปลงข้อมูล
# ตัวอย่างนี้จะทำการแปลงแต่ละบรรทัดให้เป็น tuple ของข้อมูลในคอลัมน์ที่ 1 และ 2
mapped_rdd = rdd.map(lambda line: line.split(',')[:2])  # แปลงข้อมูลให้เป็น tuple ของคอลัมน์ที่ 1 และ 2

# แสดงตัวอย่างของ mapped RDD
print("Mapped RDD sample:")
for record in mapped_rdd.take(5):  # แสดง 5 แถวแรกของ mapped RDD
    print(record)

# นับจำนวนแถวใน mapped RDD
count_mapped = mapped_rdd.count()
print('Number of mapped records:', count_mapped)
