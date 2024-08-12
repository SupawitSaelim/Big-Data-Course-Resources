from pyspark.sql import SparkSession

# สร้าง SparkSession
spark = SparkSession.builder.getOrCreate()

# อ่านไฟล์ CSV เป็น RDD
rdd = spark.sparkContext.textFile('fb_live_thailand.csv')

# ตรวจสอบจำนวน partitions
print("Number of partitions: " + str(rdd.getNumPartitions()))

# ใช้ filter เพื่อกรองข้อมูลที่คอลัมน์ที่สองมีค่าเป็น 'link'
# โดยการใช้ lambda function เพื่อแยกข้อมูลในแต่ละบรรทัดและตรวจสอบค่าในคอลัมน์ที่สอง
filter_rdd = rdd.filter(lambda line: line.split(',')[1] == 'link').collect()

# แสดงข้อมูลที่ผ่านการกรอง
print('Filtered records:')
for record in filter_rdd:
    print(record)

# หากต้องการนับจำนวน record ที่กรองได้
count_filtered = len(filter_rdd)
print('Number of filtered records:', count_filtered)
