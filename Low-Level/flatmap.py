from pyspark.sql import SparkSession

# สร้าง SparkSession
spark = SparkSession.builder.getOrCreate()

# อ่านไฟล์ CSV เป็น RDD
rdd = spark.sparkContext.textFile('fb_live_thailand.csv')

# ใช้ flatMap เพื่อแยกข้อมูลในแต่ละบรรทัดด้วยคอมมา
flatmap_rdd = rdd.flatMap(lambda x: x.split(','))

# ใช้ map เพื่อสร้างคู่ของ (คำ, 1)
pair_rdd = flatmap_rdd.map(lambda x: (x, 1))

# แสดงตัวอย่างคู่ (คำ, 1) จำนวน 200 คู่
take_pair = pair_rdd.take(200)

# กรองและแสดงผลเฉพาะคู่ที่มีคำว่า 'photo' หรือ 'video'
print('Filtered pairs:')
for f in take_pair:
    if str(f[0]) == 'photo' or str(f[0]) == 'video':
        print(str(f[0]), str(f[1]))
