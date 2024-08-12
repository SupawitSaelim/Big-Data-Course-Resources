from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
rdd = spark.sparkContext.textFile('fb_live_thailand.csv')

# ใช้ flatMap เพื่อแยกข้อมูลในแต่ละบรรทัดด้วยคอมมา
flatmap_rdd = rdd.flatMap(lambda x: x.split(','))

# ใช้ map เพื่อสร้างคู่ของ (คำ, 1)
pair_rdd = flatmap_rdd.map(lambda x: (x, 1))

# ใช้ reduceByKey เพื่อรวมค่าของคู่ที่มีคีย์เดียวกันโดยการบวกค่าของมันเข้าด้วยกัน
reduce_key_rdd = pair_rdd.reduceByKey(lambda x, y: x + y)

# ดึงผลลัพธ์บางส่วนมาแสดง
result = reduce_key_rdd.take(10)
print('Reduced by key:')
for record in result:
    print(record)
