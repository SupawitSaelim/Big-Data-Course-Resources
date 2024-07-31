from pyspark.sql import SparkSession
from pyspark.sql.functions import first, last

# สร้าง Spark Session
spark = SparkSession.builder.appName("FirstLastExample").getOrCreate()

# อ่านข้อมูลจากไฟล์ CSV หลายไฟล์
read_file = spark.read.format("csv")\
    .option("header", "true")\
    .load("data/*.csv")

# สร้าง Temporary View
read_file.createOrReplaceTempView("tempTable")

# ใช้ SQL Query เพื่อเลือกข้อมูลจาก Temporary View
sqlDF = spark.sql("SELECT * FROM tempTable")

# เลือกค่าแรกและค่าหลังสุดจากคอลัมน์ที่ระบุ
result_df = sqlDF.select(
    first("num_shares").alias("first_value"),
    last("num_shares").alias("last_value")
)

# แสดงผลลัพธ์
result_df.show()

'''
ในการใช้ PySpark สำหรับการคำนวณค่าแรกและค่าสุดท้ายในคอลัมน์ที่ระบุของ DataFrame คุณสามารถใช้ฟังก์ชัน `first` และ `last` จาก `pyspark.sql.functions` ได้ โค้ดที่คุณให้มามีการใช้งานฟังก์ชัน `first` และ `last` แต่มีข้อผิดพลาดในการใช้เครื่องหมายคำพูดและการจัดระเบียบโค้ด

### **ตัวอย่างโค้ดที่แก้ไขแล้ว:**

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import first, last

# สร้าง Spark Session
spark = SparkSession.builder.appName("FirstLastExample").getOrCreate()

# อ่านข้อมูลจากไฟล์ CSV หลายไฟล์
read_file = spark.read.format("csv")\
    .option("header", "true")\
    .load("data/*.csv")

# สร้าง Temporary View
read_file.createOrReplaceTempView("tempTable")

# ใช้ SQL Query เพื่อเลือกข้อมูลจาก Temporary View
sqlDF = spark.sql("SELECT * FROM tempTable")

# เลือกค่าแรกและค่าหลังสุดจากคอลัมน์ที่ระบุ
result_df = sqlDF.select(
    first("column_name").alias("first_value"),
    last("column_name").alias("last_value")
)

# แสดงผลลัพธ์
result_df.show()
```

### **คำอธิบายโค้ด:**

1. **นำเข้าคลาสและฟังก์ชันที่จำเป็น:**
   ```python
   from pyspark.sql import SparkSession
   from pyspark.sql.functions import first, last
   ```
   - `SparkSession`: คลาสหลักที่ใช้ในการสร้างและจัดการ Spark Session.
   - `first`, `last`: ฟังก์ชันที่ใช้ในการดึงค่าที่เป็นอันดับแรกและค่าที่เป็นอันดับสุดท้ายในคอลัมน์.

2. **สร้าง Spark Session:**
   ```python
   spark = SparkSession.builder.appName("FirstLastExample").getOrCreate()
   ```
   - สร้าง Spark Session โดยตั้งชื่อว่า `"FirstLastExample"`.

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

6. **เลือกค่าแรกและค่าหลังสุดจากคอลัมน์ที่ระบุ:**
   ```python
   result_df = sqlDF.select(
       first("column_name").alias("first_value"),
       last("column_name").alias("last_value")
   )
   ```
   - ใช้ฟังก์ชัน `first("column_name")` เพื่อดึงค่าที่เป็นอันดับแรกในคอลัมน์ที่ระบุ (เปลี่ยน `"column_name"` เป็นชื่อคอลัมน์จริงที่ต้องการ).
   - ใช้ฟังก์ชัน `last("column_name")` เพื่อดึงค่าที่เป็นอันดับสุดท้ายในคอลัมน์ที่ระบุ.
   - `alias("first_value")` และ `alias("last_value")` ใช้เพื่อเปลี่ยนชื่อคอลัมน์ผลลัพธ์ให้เป็น `"first_value"` และ `"last_value"` ตามลำดับ.

7. **แสดงผลลัพธ์:**
   ```python
   result_df.show()
   ```
   - ใช้ `show()` เพื่อแสดงผลลัพธ์ที่ได้จากการดึงค่าที่เป็นอันดับแรกและอันดับสุดท้ายของคอลัมน์ที่ระบุ.

### **สรุป**
โค้ดนี้ทำการ:
1. สร้าง Spark Session และโหลดข้อมูลจากไฟล์ CSV.
2. สร้าง Temporary View เพื่อใช้ SQL queries.
3. ใช้ฟังก์ชัน `first` และ `last` เพื่อนับค่าที่เป็นอันดับแรกและอันดับสุดท้ายในคอลัมน์ที่ระบุ.
4. แสดงผลลัพธ์ที่ได้จากการคำนวณ.

การใช้ฟังก์ชัน `first` และ `last` ทำให้สามารถดึงค่าที่สำคัญจากคอลัมน์ของ DataFrame ได้อย่างสะดวก.
'''