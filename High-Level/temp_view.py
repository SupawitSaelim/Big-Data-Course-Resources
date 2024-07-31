from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType

# สร้าง Spark Session
spark = SparkSession.builder.appName("ExampleApp").getOrCreate()

# อ่านไฟล์ CSV
read_file = spark.read.format("csv")\
    .option("header", "true")\
    .load("fb_live_thailand.csv")

# สร้าง Temporary View
read_file.createOrReplaceTempView("temp_view_name")

# ใช้ SQL Query เพื่อคิวรีข้อมูล
sqlDF = spark.sql("SELECT * FROM temp_view_name")

# แสดงผลลัพธ์
sqlDF.show(10)  # แสดง 10 แถวแรก

'''
อธิบายโค้ด:
สร้าง Spark Session: เริ่มต้นการทำงานกับ Spark โดยสร้าง Spark Session.
อ่านไฟล์ CSV: โหลดข้อมูลจากไฟล์ CSV ลงใน DataFrame.
สร้าง Temporary View: สร้าง Temporary View ที่ชื่อว่า temp_view_name เพื่อให้สามารถใช้ SQL Query คิวรีข้อมูลจาก DataFrame นี้.
คิวรีข้อมูลด้วย SQL: ใช้ SQL Query ดึงข้อมูลทั้งหมดจาก Temporary View.
แสดงผลลัพธ์: แสดงผลลัพธ์ของ SQL Query บนคอนโซล โดยแสดงแถวแรก 10 แถว.
หมายเหตุ:
เปลี่ยน "temp_view_name" เป็นชื่อที่คุณต้องการใช้สำหรับ Temporary View.
ใช้ SQL Query ให้ตรงตามข้อมูลที่ต้องการคิวรี.
ปรับจำนวนแถวที่จะแสดงใน show ตามความต้องการ.
'''