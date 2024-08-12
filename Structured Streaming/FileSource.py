from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split
from pyspark.sql.types import StructType, StringType, StructField

spark = SparkSession.builder.appName("FileSourceStreamingExample").getOrCreate()

# Define the schema for the CSV files
schema = StructType([
StructField("status_id", StringType(), True),
StructField("status_type", StringType(), True),
StructField("status_published", StringType(), True)
])

# Read from the CSV file source in a streaming fashion
lines = spark.readStream.format("csv") \
.option("maxFilesPerTrigger", 1) \
.option("header", True) \
.option("path", "data") \
.schema(schema) \
.load()

# Extract the date from the 'status_published' column
words = lines.withColumn("date", split(lines["status_published"], " ").getItem(0))

# Group by 'date' and 'status_type', then count the occurrences
wordCounts = words.groupBy("date", "status_type").count()

# Start running the query that prints the word counts to the console
query = wordCounts.writeStream \
.outputMode("complete") \
.format("console") \
.start()

# Await termination of the query
query.awaitTermination()


'''
โค้ดนี้ใช้ Apache Spark สำหรับการประมวลผลข้อมูลแบบสตรีมมิ่งจากไฟล์ CSV โดยจะอธิบายแต่ละขั้นตอนของโค้ดและการทำงานของมันอย่างละเอียด:

### 1. **การนำเข้าไลบรารี**:
   ```python
   from pyspark.sql import SparkSession
   from pyspark.sql.functions import explode, split
   from pyspark.sql.types import StructType, StringType, StructField
   ```
   - `SparkSession` ใช้สำหรับสร้างเซสชันการทำงานกับ Spark
   - `explode`, `split` เป็นฟังก์ชันที่ใช้ในการประมวลผลข้อมูล
   - `StructType`, `StringType`, `StructField` ใช้สำหรับกำหนด schema ของข้อมูล

### 2. **การสร้าง SparkSession**:
   ```python
   spark = SparkSession.builder.appName("FileSourceStreamingExample").getOrCreate()
   ```
   - `SparkSession.builder` ใช้สำหรับสร้าง SparkSession ใหม่
   - `.appName("FileSourceStreamingExample")` ตั้งชื่อให้กับแอปพลิเคชัน Spark
   - `.getOrCreate()` ใช้เพื่อรับเซสชันที่มีอยู่แล้วหรือสร้างใหม่หากไม่มี

### 3. **การกำหนด Schema**:
   ```python
   schema = StructType([
       StructField("status_id", StringType(), True),
       StructField("status_type", StringType(), True),
       StructField("status_published", StringType(), True)
       # Add more StructField definitions as necessary
   ])
   ```
   - `StructType` ใช้สำหรับกำหนด schema ของ DataFrame
   - `StructField` ใช้สำหรับกำหนดชื่อและประเภทของคอลัมน์
   - `StringType()` หมายถึงข้อมูลที่เป็นประเภทสตริง
   - `True` หมายถึงคอลัมน์ที่เป็นค่า null ได้

### 4. **การอ่านข้อมูลจากไฟล์ CSV แบบสตรีม**:
   ```python
   lines = spark.readStream.format("csv") \
       .option("maxFilesPerTrigger", 1) \
       .option("header", True) \
       .option("path", "data") \
       .schema(schema) \
       .load()
   ```
   - `spark.readStream.format("csv")` กำหนดให้ Spark อ่านข้อมูลในโหมดสตรีมจากไฟล์ CSV
   - `.option("maxFilesPerTrigger", 1)` จำกัดให้ Spark อ่านไฟล์สูงสุด 1 ไฟล์ในแต่ละ trigger
   - `.option("header", True)` ระบุว่าไฟล์ CSV มีแถวหัวข้อ
   - `.option("path", "data")` ระบุพาธของไดเรกทอรีที่มีไฟล์ CSV
   - `.schema(schema)` ใช้ schema ที่กำหนดในการอ่านข้อมูล
   - `.load()` เริ่มโหลดข้อมูลจากพาธที่ระบุ

### 5. **การประมวลผลข้อมูล**:
   ```python
   words = lines.withColumn("date", split(lines["status_published"], " ").getItem(0))
   ```
   - `withColumn("date", split(lines["status_published"], " ").getItem(0))` สร้างคอลัมน์ใหม่ชื่อ "date" โดยการแยกข้อมูลในคอลัมน์ `status_published` ตามช่องว่างและเลือกเฉพาะวันที่ (ส่วนแรก)

### 6. **การจัดกลุ่มและนับจำนวน**:
   ```python
   wordCounts = words.groupBy("date", "status_type").count()
   ```
   - `groupBy("date", "status_type")` จัดกลุ่มข้อมูลตามวันที่และประเภทสถานะ
   - `.count()` นับจำนวนรายการในแต่ละกลุ่ม

### 7. **การเขียนผลลัพธ์ไปยังคอนโซล**:
   ```python
   query = wordCounts.writeStream \
       .outputMode("complete") \
       .format("console") \
       .start()
   ```
   - `writeStream` ใช้สำหรับเขียนข้อมูลที่ประมวลผลแล้ว
   - `.outputMode("complete")` หมายถึง การเขียนผลลัพธ์ทั้งหมดในแต่ละครั้งของการประมวลผล
   - `.format("console")` กำหนดให้แสดงผลลัพธ์ในคอนโซล
   - `.start()` เริ่มต้นการประมวลผลสตรีม

### 8. **รอการสิ้นสุดของการประมวลผล**:
   ```python
   query.awaitTermination()
   ```
   - รอจนกว่าการประมวลผลสตรีมมิ่งจะสิ้นสุด

### สรุป

- โค้ดนี้ใช้ Apache Spark เพื่อประมวลผลข้อมูลแบบสตรีมมิ่งจากไฟล์ CSV
- กำหนด schema สำหรับไฟล์ CSV เพื่อให้การอ่านข้อมูลมีความถูกต้อง
- ข้อมูลจะถูกประมวลผลโดยการแยกวันที่จากคอลัมน์ `status_published` และจัดกลุ่มตามวันที่และประเภทสถานะ
- ผลลัพธ์จะถูกเขียนไปยังคอนโซลในโหมด "complete" ซึ่งหมายถึงการแสดงผลลัพธ์ทั้งหมดในแต่ละครั้งของการประมวลผล

การทำงานร่วมกันของโค้ดจะช่วยให้สามารถประมวลผลข้อมูลแบบสตรีมมิ่งจากไฟล์ CSV และดูผลลัพธ์ในเวลาเรียลไทม์
'''