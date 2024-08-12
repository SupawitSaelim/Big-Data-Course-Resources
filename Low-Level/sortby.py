from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
rdd = spark.sparkContext.textFile('fb_live_thailand.csv')

# ใช้ flatMap เพื่อแยกข้อมูลในแต่ละบรรทัดด้วยคอมมา
flatmap_rdd = rdd.flatMap(lambda x: x.split(','))

# ใช้ map เพื่อสร้างคู่ของ (คำ, 1)
pair_rdd = flatmap_rdd.map(lambda x: (x, 1))

# ใช้ sortBy เพื่อจัดเรียงคู่ (key, value) ตาม key ในลำดับจากมากไปน้อย และแบ่งเป็น 5 partitions
sort_data = pair_rdd.sortBy(lambda x: x[0], ascending=False, numPartitions=5).collect()

# แสดงผลลัพธ์ที่จัดเรียงแล้ว
print('Sorted pairs:')
for f in sort_data:
    print(str(f[0]), str(f[1]))
