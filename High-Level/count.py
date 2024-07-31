from pyspark.sql import SparkSession
from pyspark.sql.functions import count

# สร้าง Spark Session
spark = SparkSession.builder.appName("AggregationExample").getOrCreate()

# อ่านข้อมูลจากไฟล์ CSV หลายไฟล์
read_file = spark.read.format("csv")\
    .option("header", "true")\
    .load("data/*.csv")

# สร้าง Temporary View
read_file.createOrReplaceTempView("tempTable")

# นับจำนวนค่าที่ไม่เป็นค่าว่างในคอลัมน์ที่ระบุ
count_df = spark.sql("SELECT COUNT(status_published) AS count_status_published FROM tempTable")

# แสดงผลลัพธ์
count_df.show()



'''
โค้ดที่ให้มานี้เป็นตัวอย่างการใช้งาน PySpark เพื่ออ่านข้อมูลจากไฟล์ CSV, สร้าง Temporary View, ทำการนับจำนวนข้อมูลในคอลัมน์ที่ระบุ และแสดงผลลัพธ์ ตัวอย่างนี้จะอธิบายการทำงานในแต่ละขั้นตอนเป็นภาษาไทย:

### **1. นำเข้าคลาสและฟังก์ชันที่ต้องการ**

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import count
```
- `SparkSession`: คลาสหลักที่ใช้ในการสร้าง Spark Session ซึ่งเป็นจุดเริ่มต้นในการทำงานกับ Spark DataFrame และ SQL.
- `count`: ฟังก์ชันที่ใช้ในการนับจำนวนแถวที่ไม่เป็นค่าว่างในคอลัมน์ที่ระบุ.

### **2. สร้าง Spark Session**

```python
spark = SparkSession.builder.appName("AggregationExample").getOrCreate()
```
- สร้าง Spark Session ใหม่โดยตั้งชื่อว่า `"AggregationExample"`.
- ใช้ `getOrCreate()` เพื่อรับ Spark Session ที่มีอยู่แล้วหากมีอยู่ หรือสร้างใหม่หากไม่มี.

### **3. อ่านข้อมูลจากไฟล์ CSV หลายไฟล์**

```python
read_file = spark.read.format("csv")\
    .option("header", "true")\
    .load("data/*.csv")
```
- ใช้ `spark.read.format("csv")` เพื่อระบุว่าต้องการอ่านข้อมูลในรูปแบบ CSV.
- `.option("header", "true")` กำหนดให้ PySpark ใช้แถวแรกของไฟล์ CSV เป็นชื่อคอลัมน์.
- `.load("data/*.csv")` ระบุที่อยู่ของไฟล์ CSV ที่ต้องการโหลด ซึ่งในกรณีนี้จะโหลดทุกไฟล์ CSV ที่อยู่ในโฟลเดอร์ `data`.

### **4. สร้าง Temporary View**

```python
read_file.createOrReplaceTempView("tempTable")
```
- สร้าง Temporary View ชื่อ `"tempTable"` จาก DataFrame ที่ได้จากการโหลดข้อมูล.
- `createOrReplaceTempView` ใช้เพื่อสร้างหรือแทนที่ Temporary View ที่มีอยู่แล้วด้วยชื่อเดียวกัน.

### **5. นับจำนวนค่าที่ไม่เป็นค่าว่างในคอลัมน์ที่ระบุ**

```python
count_df = spark.sql("SELECT COUNT(status_published) AS count_status_published FROM tempTable")
```
- ใช้ SQL query เพื่อคำนวณจำนวนค่าที่ไม่เป็นค่าว่างในคอลัมน์ `status_published`.
- `COUNT(status_published)` จะนับจำนวนแถวที่มีค่าในคอลัมน์ `status_published` และตั้งชื่อผลลัพธ์ว่า `count_status_published`.

### **6. แสดงผลลัพธ์**

```python
count_df.show()
```
- ใช้เมธอด `show()` เพื่อแสดงผลลัพธ์ของการคำนวณที่ได้จาก SQL query.

### **สรุป**

โค้ดนี้ทำการ:
1. สร้าง Spark Session เพื่อเริ่มต้นการทำงานกับ Spark.
2. อ่านข้อมูลจากไฟล์ CSV หลายไฟล์และสร้าง DataFrame.
3. สร้าง Temporary View ชื่อ `tempTable` เพื่อให้สามารถใช้ SQL queries ได้.
4. ใช้ SQL query นับจำนวนค่าที่ไม่เป็นค่าว่างในคอลัมน์ `status_published`.
5. แสดงผลลัพธ์ของการนับจำนวนแถวในคอลัมน์ที่ระบุ.

การใช้ PySpark ทำให้การจัดการข้อมูลขนาดใหญ่เป็นไปได้อย่างมีประสิทธิภาพ และการใช้ SQL queries ช่วยให้การวิเคราะห์ข้อมูลสะดวกและเข้าใจง่ายขึ้น.
'''