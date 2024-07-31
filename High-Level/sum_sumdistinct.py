from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, sumDistinct
from pyspark.sql.types import IntegerType

# สร้าง Spark Session
spark = SparkSession.builder.appName("SumExample").getOrCreate()

# อ่านข้อมูลจากไฟล์ CSV หลายไฟล์
read_file = spark.read.format("csv")\
    .option("header", "true")\
    .load("data/*.csv")

# สร้าง Temporary View
read_file.createOrReplaceTempView("tempTable")

# ใช้ SQL Query เพื่อเลือกข้อมูลจาก Temporary View
sqlDF = spark.sql("SELECT * FROM tempTable")

# แปลงคอลัมน์เป็นประเภท Integer และเพิ่มเป็นคอลัมน์ใหม่
sqlDF = sqlDF.withColumn('new_num_likes', 
                         sqlDF['num_likes'].cast(IntegerType()))

# คำนวณผลรวมของค่าทั้งหมดในคอลัมน์ใหม่
total_sum_df = sqlDF.select(sum('num_likes').alias('total_sum'))

# คำนวณผลรวมของค่าที่ไม่ซ้ำกันในคอลัมน์ใหม่
distinct_sum_df = sqlDF.select(sumDistinct('new_num_likes').alias('distinct_sum'))

# แสดงผลลัพธ์
total_sum_df.show()
distinct_sum_df.show()


'''
ในการคำนวณผลรวม (sum) และผลรวมของค่าที่ไม่ซ้ำกัน (sumDistinct) ใน PySpark คุณสามารถใช้ฟังก์ชัน `sum` และ `sumDistinct` จาก `pyspark.sql.functions` ได้ นี่คือวิธีการที่ถูกต้องในการใช้งานฟังก์ชันเหล่านี้:

### **ตัวอย่างโค้ดที่แก้ไขแล้ว:**

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, sumDistinct
from pyspark.sql.types import IntegerType

# สร้าง Spark Session
spark = SparkSession.builder.appName("SumExample").getOrCreate()

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

# คำนวณผลรวมของค่าทั้งหมดในคอลัมน์ใหม่
total_sum_df = sqlDF.select(sum('new_column_name').alias('total_sum'))

# คำนวณผลรวมของค่าที่ไม่ซ้ำกันในคอลัมน์ใหม่
distinct_sum_df = sqlDF.select(sumDistinct('new_column_name').alias('distinct_sum'))

# แสดงผลลัพธ์
total_sum_df.show()
distinct_sum_df.show()
```

### **คำอธิบายโค้ด:**

1. **นำเข้าคลาสและฟังก์ชันที่จำเป็น:**
   ```python
   from pyspark.sql import SparkSession
   from pyspark.sql.functions import sum, sumDistinct
   from pyspark.sql.types import IntegerType
   ```
   - `SparkSession`: ใช้ในการสร้างและจัดการ Spark Session.
   - `sum`, `sumDistinct`: ฟังก์ชันที่ใช้ในการคำนวณผลรวมและผลรวมของค่าที่ไม่ซ้ำกัน.
   - `IntegerType`: ใช้ในการแปลงประเภทข้อมูลของคอลัมน์เป็น Integer.

2. **สร้าง Spark Session:**
   ```python
   spark = SparkSession.builder.appName("SumExample").getOrCreate()
   ```
   - สร้าง Spark Session โดยตั้งชื่อว่า `"SumExample"`.

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

7. **คำนวณผลรวมของค่าทั้งหมดในคอลัมน์ใหม่:**
   ```python
   total_sum_df = sqlDF.select(sum('new_column_name').alias('total_sum'))
   ```
   - ใช้ฟังก์ชัน `sum('new_column_name')` เพื่อคำนวณผลรวมของค่าทั้งหมดในคอลัมน์ `new_column_name`.
   - ใช้ `alias('total_sum')` เพื่อเปลี่ยนชื่อคอลัมน์ของผลลัพธ์ให้เป็น `"total_sum"`.

8. **คำนวณผลรวมของค่าที่ไม่ซ้ำกันในคอลัมน์ใหม่:**
   ```python
   distinct_sum_df = sqlDF.select(sumDistinct('new_column_name').alias('distinct_sum'))
   ```
   - ใช้ฟังก์ชัน `sumDistinct('new_column_name')` เพื่อคำนวณผลรวมของค่าที่ไม่ซ้ำกันในคอลัมน์ `new_column_name`.
   - ใช้ `alias('distinct_sum')` เพื่อเปลี่ยนชื่อคอลัมน์ของผลลัพธ์ให้เป็น `"distinct_sum"`.

9. **แสดงผลลัพธ์:**
   ```python
   total_sum_df.show()
   distinct_sum_df.show()
   ```
   - ใช้ `show()` เพื่อแสดงผลลัพธ์ของการคำนวณผลรวมทั้งหมดและผลรวมของค่าที่ไม่ซ้ำกัน.

### **สรุป**
โค้ดนี้ทำการ:
1. สร้าง Spark Session และโหลดข้อมูลจากไฟล์ CSV.
2. สร้าง Temporary View สำหรับ SQL queries.
3. แปลงคอลัมน์เป็นประเภท Integer และเพิ่มคอลัมน์ใหม่.
4. คำนวณผลรวม (`sum`) และผลรวมของค่าที่ไม่ซ้ำกัน (`sumDistinct`) ของคอลัมน์ใหม่.
5. แสดงผลลัพธ์ที่ได้จากการคำนวณ.

การใช้ `sum` และ `sumDistinct` ช่วยให้สามารถทำการวิเคราะห์ข้อมูลในรูปแบบที่ต้องการได้อย่างง่ายดายใน PySpark.
'''