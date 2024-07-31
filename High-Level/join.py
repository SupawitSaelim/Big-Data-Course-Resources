from pyspark.sql import SparkSession

# สร้าง Spark Session
spark = SparkSession.builder.appName("JoinExample").getOrCreate()

# อ่านข้อมูลจากไฟล์ CSV
df1 = spark.read.format("csv").option("header", "true").load("data/fb_live_thailand.csv")
df2 = spark.read.format("csv").option("header", "true").load("data/fb_live_thailand2.csv")

# สร้าง Temporary View
df1.createOrReplaceTempView("table1")
df2.createOrReplaceTempView("table2")

# ใช้ SQL Query เพื่อเลือกข้อมูลจาก Temporary View
sqlDF1 = spark.sql("SELECT * FROM table1")
sqlDF2 = spark.sql("SELECT * FROM table2")

# กำหนดคอลัมน์ที่ใช้ในการจับคู่ระหว่างสอง DataFrames
join_column = sqlDF1["num_reactions"] == sqlDF2["status_published"]

# ทำการ Join ข้อมูลระหว่างสอง DataFrames โดยใช้คอลัมน์ที่จับคู่
joined_df = sqlDF1.join(sqlDF2, join_column, "inner")  # ใช้ "inner" join เป็นตัวอย่าง

# แสดงผลลัพธ์
joined_df.show()


'''
ในการทำการ **join** ข้อมูลระหว่างสอง DataFrames ใน PySpark คุณต้องระบุคอลัมน์ที่ใช้ในการจับคู่ (matching) ระหว่าง DataFrames นั่นคือคอลัมน์ที่มีข้อมูลที่เหมือนกันในทั้งสอง DataFrame ซึ่งจะใช้ในการรวมข้อมูลเข้าด้วยกัน

### **ตัวอย่างโค้ดในการทำ Join ข้อมูล**

```python
from pyspark.sql import SparkSession

# สร้าง Spark Session
spark = SparkSession.builder.appName("JoinExample").getOrCreate()

# อ่านข้อมูลจากไฟล์ CSV
df1 = spark.read.format("csv").option("header", "true").load("data1.csv")
df2 = spark.read.format("csv").option("header", "true").load("data2.csv")

# สร้าง Temporary View
df1.createOrReplaceTempView("table1")
df2.createOrReplaceTempView("table2")

# ใช้ SQL Query เพื่อเลือกข้อมูลจาก Temporary View
sqlDF1 = spark.sql("SELECT * FROM table1")
sqlDF2 = spark.sql("SELECT * FROM table2")

# กำหนดคอลัมน์ที่ใช้ในการจับคู่ระหว่างสอง DataFrames
join_column = sqlDF1["<column name>"] == sqlDF2["<column name>"]

# ทำการ Join ข้อมูลระหว่างสอง DataFrames โดยใช้คอลัมน์ที่จับคู่
joined_df = sqlDF1.join(sqlDF2, join_column, "inner")  # ใช้ "inner" join เป็นตัวอย่าง

# แสดงผลลัพธ์
joined_df.show()
```

### **คำอธิบายโค้ด:**

1. **สร้าง Spark Session:**
   ```python
   spark = SparkSession.builder.appName("JoinExample").getOrCreate()
   ```
   - สร้าง Spark Session โดยตั้งชื่อว่า `"JoinExample"`.

2. **อ่านข้อมูลจากไฟล์ CSV:**
   ```python
   df1 = spark.read.format("csv").option("header", "true").load("data1.csv")
   df2 = spark.read.format("csv").option("header", "true").load("data2.csv")
   ```
   - ใช้ `spark.read` เพื่อโหลดข้อมูลจากไฟล์ CSV ที่ระบุ. ตั้งค่าทางเลือก `"header"` ให้เป็น `true` เพื่อใช้แถวแรกเป็นชื่อคอลัมน์.

3. **สร้าง Temporary View:**
   ```python
   df1.createOrReplaceTempView("table1")
   df2.createOrReplaceTempView("table2")
   ```
   - สร้าง Temporary View สำหรับแต่ละ DataFrame เพื่อให้สามารถใช้ SQL queries ได้.

4. **ใช้ SQL Query เพื่อเลือกข้อมูลจาก Temporary View:**
   ```python
   sqlDF1 = spark.sql("SELECT * FROM table1")
   sqlDF2 = spark.sql("SELECT * FROM table2")
   ```
   - ใช้ SQL query เพื่อดึงข้อมูลทั้งหมดจาก Temporary View `"table1"` และ `"table2"`.

5. **กำหนดคอลัมน์ที่ใช้ในการจับคู่ระหว่างสอง DataFrames:**
   ```python
   join_column = sqlDF1["<column name>"] == sqlDF2["<column name>"]
   ```
   - กำหนดคอลัมน์ที่ใช้ในการจับคู่ (matching column) ระหว่าง DataFrames. ให้แทนที่ `"<column name>"` ด้วยชื่อคอลัมน์ที่ต้องการใช้ในการจับคู่.

6. **ทำการ Join ข้อมูลระหว่างสอง DataFrames โดยใช้คอลัมน์ที่จับคู่:**
   ```python
   joined_df = sqlDF1.join(sqlDF2, join_column, "inner")
   ```
   - ทำการ join ข้อมูลระหว่าง `sqlDF1` และ `sqlDF2` โดยใช้คอลัมน์ที่จับคู่ `join_column`. ใช้ `"inner"` join เป็นตัวอย่าง, แต่คุณสามารถใช้ join ประเภทอื่นๆ ได้ตามต้องการ เช่น `"left"`, `"right"`, หรือ `"outer"`.

7. **แสดงผลลัพธ์:**
   ```python
   joined_df.show()
   ```
   - ใช้ `show()` เพื่อแสดงผลลัพธ์ของ DataFrame ที่ทำการ join.

### **สรุป**
- **การจับคู่ (Matching):** กำหนดคอลัมน์ที่ใช้ในการจับคู่ระหว่าง DataFrames.
- **การทำ Join:** ใช้ฟังก์ชัน `join` เพื่อรวมข้อมูลระหว่าง DataFrames ตามคอลัมน์ที่จับคู่.
- **การแสดงผล:** ใช้ `show()` เพื่อดูผลลัพธ์หลังจากทำการ join.

การทำ join ข้อมูลใน PySpark สามารถทำได้หลายวิธีตามความต้องการของการวิเคราะห์ข้อมูลและโครงสร้างของข้อมูลที่คุณมี.
'''