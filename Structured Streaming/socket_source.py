from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

# Create Spark Session
spark = SparkSession.builder \
    .appName("WordCountStreaming") \
    .getOrCreate()

# Create DataFrame representing the stream of input lines from connection to localhost: 7000
lines = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 7000) \
    .load()

# Split the lines into words and explode them
words = lines.select(
    explode(
        split(lines.value, " ")
    ).alias("word")
)

# Group by each word and count occurrences
wordCounts = words.groupBy("word").count()

# Write the results to the console
query = wordCounts.writeStream \
    .outputMode("update") \
    .format("console") \
    .start()
# Use "append", "update", or "complete"


# Await termination of the query
query.awaitTermination()



'''
โค้ดที่คุณมีคือการตั้งค่าการประมวลผลข้อมูลแบบสตรีมมิ่งด้วย Apache Spark และเชื่อมต่อกับ `ncat` เพื่อรับข้อมูลเข้ามา ในที่นี้เราจะอธิบายรายละเอียดของการทำงานของโค้ดและการทำงานร่วมกันของ `ncat` กับ Spark Streaming

### การทำงานของโค้ด

1. **การสร้าง Spark Session**:
   ```python
   from pyspark.sql import SparkSession

   spark = SparkSession.builder \
       .appName("WordCountStreaming") \
       .getOrCreate()
   ```
   - **SparkSession** คือจุดเริ่มต้นในการใช้งาน Spark API และจะจัดการการเชื่อมต่อกับ Spark cluster
   - `.appName("WordCountStreaming")` ตั้งชื่อให้กับแอปพลิเคชัน Spark

2. **การสร้าง DataFrame สำหรับรับข้อมูลสตรีม**:
   ```python
   lines = spark.readStream \
       .format("socket") \
       .option("host", "localhost") \
       .option("port", 7000) \
       .load()
   ```
   - `spark.readStream` ใช้สำหรับอ่านข้อมูลในโหมดสตรีมมิ่ง
   - `.format("socket")` กำหนดให้ Spark อ่านข้อมูลจากการเชื่อมต่อ socket
   - `.option("host", "localhost")` และ `.option("port", 7000)` ตั้งค่าที่อยู่และพอร์ตที่ Spark จะรับข้อมูล

3. **การประมวลผลข้อมูล**:
   ```python
   from pyspark.sql.functions import explode, split

   words = lines.select(
       explode(
           split(lines.value, " ")
       ).alias("word")
   )
   ```
   - `split(lines.value, " ")` แยกข้อความออกเป็นคำ โดยใช้ช่องว่างเป็นตัวคั่น
   - `explode()` เปลี่ยนแต่ละคำในรายการคำให้เป็นแถวแยกใน DataFrame

4. **การนับจำนวนคำ**:
   ```python
   wordCounts = words.groupBy("word").count()
   ```
   - `groupBy("word")` จัดกลุ่มข้อมูลตามคำ
   - `.count()` นับจำนวนครั้งที่แต่ละคำปรากฏ

5. **การเขียนผลลัพธ์ไปยังคอนโซล**:
   ```python
   query = wordCounts.writeStream \
       .outputMode("update") \
       .format("console") \
       .start()
   ```
   - `writeStream` ใช้สำหรับเขียนข้อมูลที่ประมวลผลแล้ว
   - `.outputMode("update")` หมายถึง การเขียนเฉพาะข้อมูลที่อัปเดต (ที่มีการเปลี่ยนแปลง) ลงในคอนโซล
   - `.format("console")` กำหนดให้ออกผลลัพธ์ไปยังคอนโซล

6. **รอการสิ้นสุดของการประมวลผล**:
   ```python
   query.awaitTermination()
   ```
   - รอจนกว่าการประมวลผลสตรีมมิ่งจะสิ้นสุด

### การทำงานร่วมกันกับ `ncat`

- **การทำงานของ `ncat`**:
  ```bash
  ncat -l 7000
  ```
  - `ncat` (NetCat) เป็นเครื่องมือที่สามารถใช้เปิด socket และรับส่งข้อมูล
  - คำสั่งนี้จะเปิดการเชื่อมต่อบนพอร์ต 7000 และรอรับข้อมูลที่ส่งเข้ามา

- **การเชื่อมโยงกับ Spark Streaming**:
  - โค้ด Spark Streaming ของคุณเชื่อมต่อกับ `localhost:7000` ซึ่งหมายความว่ามันจะรับข้อมูลที่ส่งมาทางพอร์ต 7000
  - ข้อมูลที่คุณส่งผ่าน `ncat` จะถูกส่งไปยังพอร์ตนี้
  - Spark จะอ่านข้อมูลที่ได้รับจาก socket, แยกเป็นคำ, นับจำนวนคำ, และแสดงผลลัพธ์ในคอนโซล

### การซิงค์กัน

- เมื่อคุณรัน `ncat -l 7000` และส่งข้อมูล เช่น `supawit saelim love python` ข้อมูลเหล่านี้จะถูกส่งผ่าน socket ไปยัง Spark Streaming
- Spark Streaming จะรับข้อมูลนี้, ประมวลผลมัน (แยกคำและนับจำนวนคำ), และแสดงผลลัพธ์ในคอนโซลตามการตั้งค่า `writeStream`

### สรุป

- `ncat` ใช้สำหรับส่งข้อมูลเข้าไปยัง Spark Streaming ผ่าน socket
- Spark Streaming รับข้อมูล, ประมวลผลข้อมูล, และแสดงผลลัพธ์
- ความซิงค์กันเกิดจากการที่ Spark Streaming รับข้อมูลจาก `ncat` ผ่านการเชื่อมต่อ socket ที่กำหนดไว้
'''