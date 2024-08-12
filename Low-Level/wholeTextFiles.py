from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
rdd = spark.sparkContext.wholeTextFiles('fb_live_thailand.csv')
print("Number of partitions: " + str(rdd.getNumPartitions()))


"""
การใช้ `wholeTextFiles()` แทน `textFile()` ใน PySpark จะมีความแตกต่างกันบ้างในการทำงาน:

- **`textFile(path)`**: ใช้เพื่ออ่านไฟล์หลายไฟล์ (หรือไฟล์เดียว) เป็น RDD ของบรรทัดข้อความ (lines) 
และสามารถแบ่งข้อมูลเป็นหลาย partitions ตามที่กำหนดหรือตามการตั้งค่าเริ่มต้นของ Spark
- **`wholeTextFiles(path)`**: ใช้เพื่ออ่านไฟล์หลายไฟล์ (หรือไฟล์เดียว) โดยจะโหลดทั้งไฟล์เป็นแต่ละ record ใน
 RDD และแต่ละ record จะเป็นคู่ (key-value) โดยที่ key คือ path ของไฟล์และ value คือเนื้อหาทั้งหมดของไฟล์

ถ้าคุณต้องการทดสอบการใช้ `wholeTextFiles()` และตรวจสอบจำนวน partitions, คุณสามารถทำได้ดังนี้:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
rdd = spark.sparkContext.wholeTextFiles('fb_live_thailand.csv')
print("Number of partitions: " + str(rdd.getNumPartitions()))
```

ในกรณีที่คุณใช้ `wholeTextFiles()`, Spark จะอ่านไฟล์ทั้งหมดและโหลดไฟล์เป็น
 record เดียวต่อไฟล์ ซึ่งหมายความว่า:

1. ถ้าไฟล์ `fb_live_thailand.csv` มีไฟล์เดียว, RDD ของคุณจะมี partition 
เดียวหรือมากกว่านั้นขึ้นอยู่กับการตั้งค่าของ Spark
2. ถ้าใช้หลายไฟล์, จะมี partition ตามจำนวนไฟล์และการตั้งค่าของ Spark

คุณอาจเห็นผลลัพธ์ที่ต่างกันตามจำนวนไฟล์ที่อ่านเข้ามาและการตั้งค่าพื้นฐานของ Spark ในการแบ่ง partitions.
"""