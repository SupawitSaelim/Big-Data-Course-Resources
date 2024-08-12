from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.appName("AverageExample").getOrCreate()
read_file = spark.read.format("csv")\
    .option("header", "true")\
    .load("data/*.csv")

read_file.createOrReplaceTempView("tempTable")
sqlDF = spark.sql("SELECT * FROM tempTable")
sqlDF = sqlDF.withColumn('new_num_likes', 
                         sqlDF['num_likes'].cast(IntegerType()))

# คำนวณค่าเฉลี่ยของคอลัมน์ใหม่
average_df = sqlDF.select(avg('new_num_likes').alias('average_value'))

# แสดงผลลัพธ์
average_df.show()





'''
ในการคำนวณค่าเฉลี่ย (average) ของคอลัมน์ใน PySpark DataFrame คุณสามารถใช้ฟังก์ชัน `avg` จาก `pyspark.sql.functions` ได้ นี่คือวิธีการที่ถูกต้องในการคำนวณค่าเฉลี่ยและการแปลงคอลัมน์เป็น Integer ก่อนการคำนวณ:

### **ตัวอย่างโค้ดที่แก้ไขแล้ว:**

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
from pyspark.sql.types import IntegerType

# สร้าง Spark Session
spark = SparkSession.builder.appName("AverageExample").getOrCreate()

# อ่านข้อมูลจากไฟล์ CSV หลายไฟล์
read_file = spark.read.format("csv")\
    .option("header", "true")\
    .load("data/*.csv")

# สร้าง Temporary View
read_file.createOrReplaceTempView("tempTable")

# ใช้ SQL Query เพื่อเลือกข้อมูลจาก Temporary View
sqlDF = spark.sql("SELECT * FROM tempTable")

# แปลงคอลัมน์เป็นประเภท Integer และเพิ่มเป็นคอลัมน์ใหม่
sqlDF = sqlDF.withColumn('new_column_name', 
                         sqlDF['old_column_name'].cast(IntegerType()))

# คำนวณค่าเฉลี่ยของคอลัมน์ใหม่
average_df = sqlDF.select(avg('new_column_name').alias('average_value'))

# แสดงผลลัพธ์
average_df.show()
```

### **คำอธิบายโค้ด:**

1. **นำเข้าคลาสและฟังก์ชันที่จำเป็น:**
   ```python
   from pyspark.sql import SparkSession
   from pyspark.sql.functions import avg
   from pyspark.sql.types import IntegerType
   ```
   - `SparkSession`: ใช้ในการสร้างและจัดการ Spark Session.
   - `avg`: ฟังก์ชันที่ใช้ในการคำนวณค่าเฉลี่ย.
   - `IntegerType`: ใช้ในการแปลงประเภทข้อมูลของคอลัมน์เป็น Integer.

2. **สร้าง Spark Session:**
   ```python
   spark = SparkSession.builder.appName("AverageExample").getOrCreate()
   ```
   - สร้าง Spark Session โดยตั้งชื่อว่า `"AverageExample"`.

3. **อ่านข้อมูลจากไฟล์ CSV หลายไฟล์:**
   ```python
   read_file = spark.read.format("csv")\
       .option("header", "true")\
       .load("data/*.csv")
   ```
   - ใช้ฟังก์ชัน `read` เพื่อโหลดข้อมูลจากไฟล์ CSV ที่อยู่ในโฟลเดอร์ `"data"`, โดยตั้งค่าให้ใช้แถวแรกเป็นชื่อคอลัมน์.

4. **สร้าง Temporary View:**
   ```python
   read_file.createOrReplaceTempView("tempTable")
   ```
   - สร้าง Temporary View ชื่อ `"tempTable"` เพื่อให้สามารถใช้ SQL queries บน DataFrame นี้ได้.

5. **ใช้ SQL Query เพื่อเลือกข้อมูลจาก Temporary View:**
   ```python
   sqlDF = spark.sql("SELECT * FROM tempTable")
   ```
   - ใช้ SQL query เพื่อเลือกข้อมูลทั้งหมดจาก Temporary View `"tempTable"`.

6. **แปลงคอลัมน์เป็น Integer และเพิ่มเป็นคอลัมน์ใหม่:**
   ```python
   sqlDF = sqlDF.withColumn('new_column_name', 
                            sqlDF['old_column_name'].cast(IntegerType()))
   ```
   - เปลี่ยนคอลัมน์ `old_column_name` เป็นประเภท Integer และเก็บผลลัพธ์ในคอลัมน์ใหม่ที่ชื่อ `new_column_name`.

7. **คำนวณค่าเฉลี่ยของคอลัมน์ใหม่:**
   ```python
   average_df = sqlDF.select(avg('new_column_name').alias('average_value'))
   ```
   - ใช้ฟังก์ชัน `avg('new_column_name')` เพื่อคำนวณค่าเฉลี่ยของคอลัมน์ `new_column_name`.
   - ใช้ `alias('average_value')` เพื่อเปลี่ยนชื่อคอลัมน์ของผลลัพธ์ให้เป็น `"average_value"`.

8. **แสดงผลลัพธ์:**
   ```python
   average_df.show()
   ```
   - ใช้ `show()` เพื่อแสดงผลลัพธ์ที่ได้จากการคำนวณค่าเฉลี่ย.

### **สรุป**
โค้ดนี้ทำการ:
1. สร้าง Spark Session และโหลดข้อมูลจากไฟล์ CSV.
2. สร้าง Temporary View สำหรับ SQL queries.
3. แปลงคอลัมน์เป็นประเภท Integer และเพิ่มคอลัมน์ใหม่.
4. คำนวณค่าเฉลี่ย (`avg`) ของคอลัมน์ใหม่.
5. แสดงผลลัพธ์ที่ได้จากการคำนวณค่าเฉลี่ย.

การใช้ `avg` ช่วยให้สามารถวิเคราะห์ข้อมูลได้อย่างง่ายดายใน PySpark.
'''