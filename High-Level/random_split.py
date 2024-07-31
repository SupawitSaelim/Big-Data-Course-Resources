from pyspark.sql import SparkSession

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

# แบ่งข้อมูลเป็นสองชุด
split = sqlDF.randomSplit([0.7, 0.3])  # 70% ข้อมูลในชุดแรก และ 30% ข้อมูลในชุดที่สอง

# แสดงข้อมูลในชุดแรก
split[0].show(10)  # แสดง 10 แถวแรกจากชุดที่หนึ่ง

# แสดงข้อมูลในชุดที่สอง
split[1].show(10)  # แสดง 10 แถวแรกจากชุดที่สอง


'''
การใช้เมธอด `randomSplit` ของ DataFrame ใน PySpark เป็นวิธีที่ดีในการแบ่งข้อมูลออกเป็นหลายชุดเพื่อการวิเคราะห์หรือการฝึกโมเดล Machine Learning โดย `randomSplit` จะสุ่มแบ่งข้อมูลตามสัดส่วนที่ระบุ

### วิธีการใช้งาน:

1. **การแบ่งข้อมูล**:
   - `randomSplit([number1, number2])` จะสุ่มแบ่งข้อมูลใน DataFrame ตามสัดส่วนที่กำหนดในลิสต์
   - `number1` และ `number2` ต้องรวมกันเป็น 1.0 หรือจำนวนที่ต้องการแบ่ง (เช่น 0.7 และ 0.3 สำหรับการแบ่งข้อมูลเป็น 70% และ 30%)

2. **แสดงผลลัพธ์**:
   - ใช้ `show()` เพื่อแสดงข้อมูลในแต่ละชุดที่ได้จากการแบ่ง

### ตัวอย่างโค้ด

สมมุติว่าเราต้องการแบ่งข้อมูลใน DataFrame ออกเป็นสองชุด โดยชุดแรกมีสัดส่วน 70% และชุดที่สองมีสัดส่วน 30%:

```python
from pyspark.sql import SparkSession

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

# แบ่งข้อมูลเป็นสองชุด
split = sqlDF.randomSplit([0.7, 0.3])  # 70% ข้อมูลในชุดแรก และ 30% ข้อมูลในชุดที่สอง

# แสดงข้อมูลในชุดแรก
split[0].show(10)  # แสดง 10 แถวแรกจากชุดที่หนึ่ง

# แสดงข้อมูลในชุดที่สอง
split[1].show(10)  # แสดง 10 แถวแรกจากชุดที่สอง
```

### คำอธิบาย:

1. **`randomSplit([0.7, 0.3])`**:
   - `0.7` และ `0.3` เป็นสัดส่วนที่ใช้ในการแบ่งข้อมูล
   - ข้อมูลจะถูกสุ่มแบ่งออกเป็นสองชุดตามสัดส่วนนี้

2. **`split[0].show(10)`**:
   - แสดงข้อมูล 10 แถวแรกจากชุดที่หนึ่ง (70% ของข้อมูลทั้งหมด)

3. **`split[1].show(10)`**:
   - แสดงข้อมูล 10 แถวแรกจากชุดที่สอง (30% ของข้อมูลทั้งหมด)

### หมายเหตุ:
- **ความสมดุล**: `randomSplit` จะสุ่มแบ่งข้อมูล ดังนั้นการแบ่งข้อมูลอาจไม่เป็นไปตามสัดส่วนที่แม่นยำ 100% แต่โดยรวมแล้วจะอยู่ใกล้เคียงกับสัดส่วนที่ระบุ.
- **การใช้งาน**: การแบ่งข้อมูลแบบนี้เป็นประโยชน์สำหรับการสร้างชุดข้อมูลฝึกและทดสอบสำหรับการฝึกโมเดล Machine Learning หรือการวิเคราะห์ข้อมูลต่างๆ.

คุณสามารถปรับสัดส่วนใน `randomSplit` และจำนวนแถวที่ต้องการแสดงใน `show` ตามความต้องการของคุณ.
'''