from pyspark.sql import SparkSession
from pyspark.sql.functions import min, max
from pyspark.sql.types import IntegerType

# สร้าง Spark Session
spark = SparkSession.builder.appName("MinMaxExample").getOrCreate()

# อ่านข้อมูลจากไฟล์ CSV หลายไฟล์
read_file = spark.read.format("csv")\
    .option("header", "true")\
    .load("data/*.csv")

# สร้าง Temporary View
read_file.createOrReplaceTempView("tempTable")

# ใช้ SQL Query เพื่อเลือกข้อมูลจาก Temporary View
sqlDF = spark.sql("SELECT * FROM tempTable")

# แปลงคอลัมน์เป็นประเภท Integer และเพิ่มเป็นคอลัมน์ใหม่
sqlDF = sqlDF.withColumn('new_num_comments', 
                         sqlDF['num_comments'].cast(IntegerType()))

# คำนวณค่า min และ max ของคอลัมน์ใหม่
result_df = sqlDF.select(
    min('new_num_comments').alias('min_value'),
    max('new_num_comments').alias('max_value')
)

# แสดงผลลัพธ์
result_df.show()
