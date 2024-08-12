from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.appName("ExampleApp").getOrCreate()

read_file = spark.read.format("csv")\
    .option("header", "true")\
    .load("fb_live_thailand.csv")


sqlDF = read_file.select("num_reactions", "num_comments", "num_loves")\
    .where(read_file["num_reactions"].cast(IntegerType()) > 100)\
    .withColumnRenamed("num_loves", "new_num_loves")\
    .orderBy("num_reactions")  # Order by num_reactions


sqlDF.show(10) 

'''
โค้ดที่ให้มานั้นเป็นการใช้ PySpark เพื่อทำงานกับข้อมูลในไฟล์ CSV โดยขั้นตอนหลักๆ ของโค้ดจะมีดังนี้:

1. **สร้าง Spark Session**:
   ```python
   spark = SparkSession.builder.appName("ExampleApp").getOrCreate()
   ```
   - การสร้าง Spark Session เป็นการเริ่มต้นการทำงานกับ Apache Spark ซึ่งเป็นขั้นตอนที่จำเป็นในการใช้ฟังก์ชันต่างๆ ของ PySpark.
   - `appName("ExampleApp")` กำหนดชื่อของแอพพลิเคชัน, ซึ่งสามารถเปลี่ยนเป็นชื่อที่ต้องการได้.

2. **อ่านไฟล์ CSV**:
   ```python
   read_file = spark.read.format("csv")\
       .option("header", "true")\
       .load("fb_live_thailand.csv")
   ```
   - `spark.read.format("csv")` กำหนดให้ PySpark ใช้รูปแบบไฟล์ CSV.
   - `.option("header", "true")` กำหนดให้ PySpark อ่านแถวแรกของไฟล์ CSV เป็นชื่อคอลัมน์.
   - `.load("fb_live_thailand.csv")` กำหนดเส้นทางของไฟล์ CSV ที่ต้องการอ่าน.

3. **เลือกคอลัมน์และกรองข้อมูล**:
   ```python
   sqlDF = read_file.select("num_reactions", "num_comments")\
       .where(read_file["num_reactions"].cast(IntegerType()) > 100)\
       .orderBy("num_reactions")
   ```
   - `select("num_reactions", "num_comments")` เลือกเฉพาะคอลัมน์ `num_reactions` และ `num_comments` จาก DataFrame.
   - `where(read_file["num_reactions"].cast(IntegerType()) > 100)` กรองข้อมูลโดยเลือกเฉพาะแถวที่มีค่าในคอลัมน์ `num_reactions` มากกว่า 100.
     - `.cast(IntegerType())` แปลงค่าของคอลัมน์ `num_reactions` ให้เป็นชนิดข้อมูล `Integer` เพื่อให้สามารถเปรียบเทียบค่าได้.
   - `orderBy("num_reactions")` จัดเรียงข้อมูลตามค่าในคอลัมน์ `num_reactions`.

4. **แสดงผลลัพธ์**:
   ```python
   sqlDF.show(10)
   ```
   - `show(10)` แสดงผลลัพธ์ของ DataFrame โดยแสดงแถวแรก 10 แถว (สามารถปรับจำนวนแถวที่ต้องการได้).

### สรุป
โค้ดนี้ใช้ PySpark เพื่ออ่านข้อมูลจากไฟล์ CSV, เลือกคอลัมน์ที่ต้องการ, กรองข้อมูลตามเงื่อนไขที่กำหนด, และจัดเรียงข้อมูลตามคอลัมน์ที่ระบุ จากนั้นจึงแสดงผลลัพธ์ที่ได้บนคอนโซล.
'''