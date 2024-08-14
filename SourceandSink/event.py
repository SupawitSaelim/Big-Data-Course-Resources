from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, current_timestamp, window

# สร้าง Spark Session
spark = SparkSession.builder \
    .appName("WindowedWordCount") \
    .getOrCreate()

# กำหนด DataFrame Streaming จาก socket source
# เปลี่ยน "localhost" และ "9999" เป็นโฮสต์และพอร์ตของคุณ
lines = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# ประมวลผลข้อมูล: แยกคำจากคอลัมน์ value, เพิ่มคอลัมน์ timestamp, และตั้ง watermark
words = lines.withColumn(
    "date", split(col("value"), " ").getItem(1)  # แยกวันที่จากค่าที่สองในคอลัมน์ value
).withColumn(
    "timestamp", current_timestamp()  # เพิ่มคอลัมน์ timestamp
).withWatermark(
    "timestamp", "10 seconds"  # ตั้ง watermark เพื่อจัดการข้อมูลที่มาช้า
).selectExpr("explode(split(value, ' ')) as word", "timestamp")

# ใช้ windowed aggregation: นับจำนวนคำภายในหน้าต่างเวลา 10 วินาที และเลื่อนทุก 5 วินาที
windowed_counts = words.groupBy(
    window(col("timestamp"), "10 seconds", "5 seconds"),
    col("word")
).count()

# เขียนผลลัพธ์ไปที่คอนโซล
query = windowed_counts.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# รอการสิ้นสุดของ streaming query
query.awaitTermination()