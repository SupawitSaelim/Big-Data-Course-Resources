from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ExampleApp").getOrCreate()
read_file = spark.read.format("csv")\
    .option("header", "true")\
    .load("fb_live_thailand.csv")

read_file.createOrReplaceTempView("temp_view_name")
sqlDF = spark.sql("SELECT * FROM temp_view_name")

# เขียนผลลัพธ์ลงในไฟล์ CSV
sqlDF.write.mode("overwrite").csv("output_folder")


'''
การใช้ PySpark สำหรับการเขียนข้อมูลจาก DataFrame ลงในไฟล์ CSV เป็นวิธีที่สะดวกในการบันทึกผลลัพธ์การคิวรีหรือการประมวลผลข้อมูล การเขียนข้อมูลลงใน CSV สามารถทำได้โดยใช้เมธอด `write` ของ DataFrame และกำหนดโหมดการเขียนและเส้นทางของไฟล์

### ตัวอย่างการเขียน DataFrame ลงในไฟล์ CSV

```python
sqlDF.write.mode("overwrite").csv("<folder>")
```

### คำอธิบาย

1. **`sqlDF.write`**: 
   - ใช้เมธอด `write` ของ DataFrame เพื่อเริ่มต้นการเขียนข้อมูลออกไปยังระบบจัดเก็บ.

2. **`mode("overwrite")`**: 
   - ใช้เมธอด `mode` เพื่อกำหนดโหมดการเขียนข้อมูล.
   - `"overwrite"` หมายถึงหากมีไฟล์หรือโฟลเดอร์ที่มีชื่อเดียวกันอยู่แล้ว ระบบจะเขียนทับไฟล์หรือโฟลเดอร์นั้น.
   - โหมดอื่นๆ ที่สามารถใช้ได้ ได้แก่:
     - `"append"`: เพิ่มข้อมูลลงในไฟล์หรือโฟลเดอร์ที่มีอยู่แล้ว.
     - `"ignore"`: ไม่ทำการเขียนข้อมูลหากไฟล์หรือโฟลเดอร์มีอยู่แล้ว.
     - `"error"` หรือ `"errorifexists"`: จะเกิดข้อผิดพลาดหากไฟล์หรือโฟลเดอร์มีอยู่แล้ว (โหมดเริ่มต้น).

3. **`.csv("<folder>")`**: 
   - ใช้เมธอด `csv` เพื่อเขียนข้อมูลออกเป็นไฟล์ CSV.
   - `<folder>` คือเส้นทางที่ต้องการบันทึกไฟล์ CSV. สามารถเป็นพาธของโฟลเดอร์ที่ต้องการบันทึกข้อมูลลงไป เช่น `"output_folder"`.

### ตัวอย่างโค้ดที่สมบูรณ์

สมมุติว่าเราใช้โค้ดด้านบนเพื่อบันทึกผลลัพธ์การคิวรีลงในไฟล์ CSV:

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

# เขียนผลลัพธ์ลงในไฟล์ CSV
sqlDF.write.mode("overwrite").csv("output_folder")
```

### หมายเหตุ

- **การกำหนดเส้นทาง**: 
  - เส้นทาง `output_folder` จะเป็นโฟลเดอร์ที่เก็บไฟล์ CSV ผลลัพธ์. หากโฟลเดอร์นี้มีอยู่แล้วและใช้โหมด `"overwrite"`, โฟลเดอร์และไฟล์ภายในจะถูกลบและเขียนใหม่ทั้งหมด.
  - ตรวจสอบให้แน่ใจว่าเส้นทางนี้มีการเขียนและการเข้าถึงที่เหมาะสม.

- **ไฟล์หลายไฟล์**: 
  - เมื่อเขียน DataFrame ลงใน CSV โดยใช้ PySpark, ข้อมูลจะถูกบันทึกลงในหลายไฟล์ CSV ตามจำนวนของ Partition ที่มีใน DataFrame. ไฟล์ CSV แต่ละไฟล์จะมีชื่อเช่น `part-00000`, `part-00001`, เป็นต้น.

- **การจัดการโฟลเดอร์**: 
  - โฟลเดอร์ที่ระบุจะถูกสร้างใหม่หากยังไม่มีอยู่ และข้อมูลจะถูกบันทึกลงในโฟลเดอร์นี้.

การเขียนข้อมูลในลักษณะนี้ช่วยให้คุณสามารถเก็บผลลัพธ์การประมวลผลและการวิเคราะห์ข้อมูลที่ทำใน Spark ได้ในรูปแบบที่สามารถนำไปใช้ต่อในกระบวนการอื่นๆ ได้.
'''
