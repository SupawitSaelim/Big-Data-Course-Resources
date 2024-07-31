from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct

# สร้าง Spark Session
spark = SparkSession.builder.appName("DistinctCountExample").getOrCreate()

# อ่านข้อมูลจากไฟล์ CSV หลายไฟล์
read_file = spark.read.format("csv")\
    .option("header", "true")\
    .load("data/*.csv")

# สร้าง Temporary View
read_file.createOrReplaceTempView("tempTable")

# ใช้ SQL Query เพื่อเลือกข้อมูลจาก Temporary View
sqlDF = spark.sql("SELECT * FROM tempTable")

# นับจำนวนค่าที่ไม่ซ้ำกันในคอลัมน์ที่ระบุ
distinct_count_df = sqlDF.select(countDistinct("num_reactions").alias("distinct_count"))

# แสดงผลลัพธ์
distinct_count_df.show()

'''
ใน PySpark, การใช้ฟังก์ชัน `countDistinct` ช่วยในการนับจำนวนค่าที่ไม่ซ้ำกันในคอลัมน์หนึ่งคอลัมน์ ตัวอย่างโค้ดที่คุณให้มานั้นมีการใช้ฟังก์ชัน `countDistinct` แต่มีข้อผิดพลาดในการใช้เครื่องหมายคำพูดและยังไม่ได้ระบุชื่อคอลัมน์จริง ๆ ให้ลองใช้โค้ดที่แก้ไขแล้วด้านล่างนี้:

### **ตัวอย่างโค้ดที่แก้ไขแล้ว:**

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct

# สร้าง Spark Session
spark = SparkSession.builder.appName("DistinctCountExample").getOrCreate()

# อ่านข้อมูลจากไฟล์ CSV หลายไฟล์
read_file = spark.read.format("csv")\
    .option("header", "true")\
    .load("data/*.csv")

# สร้าง Temporary View
read_file.createOrReplaceTempView("tempTable")

# ใช้ SQL Query เพื่อเลือกข้อมูลจาก Temporary View
sqlDF = spark.sql("SELECT * FROM tempTable")

# นับจำนวนค่าที่ไม่ซ้ำกันในคอลัมน์ที่ระบุ
distinct_count_df = sqlDF.select(countDistinct("column_name").alias("distinct_count"))

# แสดงผลลัพธ์
distinct_count_df.show()
```

### **คำอธิบายโค้ด:**

1. **นำเข้าคลาสและฟังก์ชันที่จำเป็น:**
   ```python
   from pyspark.sql import SparkSession
   from pyspark.sql.functions import countDistinct
   ```

2. **สร้าง Spark Session:**
   ```python
   spark = SparkSession.builder.appName("DistinctCountExample").getOrCreate()
   ```
   - สร้างหรือรับ Spark Session ที่มีอยู่แล้ว โดยตั้งชื่อว่า `"DistinctCountExample"`.

3. **อ่านข้อมูลจากไฟล์ CSV หลายไฟล์:**
   ```python
   read_file = spark.read.format("csv")\
       .option("header", "true")\
       .load("data/*.csv")
   ```
   - ใช้ฟังก์ชัน `read` เพื่อโหลดไฟล์ CSV ที่อยู่ในโฟลเดอร์ `"data"`, โดย `option("header", "true")` บอกว่าไฟล์มีแถวหัวข้อ.

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

6. **นับจำนวนค่าที่ไม่ซ้ำกันในคอลัมน์ที่ระบุ:**
   ```python
   distinct_count_df = sqlDF.select(countDistinct("column_name").alias("distinct_count"))
   ```
   - ใช้ฟังก์ชัน `countDistinct("column_name")` เพื่อคำนวณจำนวนค่าที่ไม่ซ้ำกันในคอลัมน์ที่ระบุ (เปลี่ยน `"column_name"` เป็นชื่อคอลัมน์จริงที่คุณต้องการ).
   - ใช้ `alias("distinct_count")` เพื่อเปลี่ยนชื่อคอลัมน์ของผลลัพธ์เป็น `"distinct_count"`.

7. **แสดงผลลัพธ์:**
   ```python
   distinct_count_df.show()
   ```
   - ใช้ `show()` เพื่อแสดงผลลัพธ์ของการนับจำนวนค่าที่ไม่ซ้ำกันในคอลัมน์ที่ระบุ.

### **สรุป**
โค้ดนี้ใช้ PySpark เพื่อ:
1. อ่านข้อมูลจากไฟล์ CSV.
2. สร้าง Temporary View.
3. ใช้ SQL query เพื่อเลือกข้อมูล.
4. ใช้ฟังก์ชัน `countDistinct` เพื่อคำนวณจำนวนค่าที่ไม่ซ้ำกันในคอลัมน์ที่ระบุ.
5. แสดงผลลัพธ์ที่ได้.

การใช้ `countDistinct` ช่วยให้คุณสามารถตรวจสอบความหลากหลายของข้อมูลในคอลัมน์ได้อย่างมีประสิทธิภาพ.
'''