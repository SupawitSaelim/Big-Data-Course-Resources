from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# สร้าง RDD จากรายการที่มีการใช้คีย์-ค่า
alphabet = [('a', 1), ('b', 2), ('c', 3), ('a', 1), ('b', 2)]
rdd = spark.sparkContext.parallelize(alphabet, 4)

# ค่าเริ่มต้นสำหรับการรวมค่า
zero_val = (0, 0)

# ฟังก์ชันสำหรับการรวมค่าภายในแต่ละ partition
par_agg = lambda x, y: (x[0] + y, x[1] + 1)

# ฟังก์ชันสำหรับการรวมค่าระหว่าง partitions
allpar_agg = lambda x, y: (x[0] + y[0], x[1] + y[1])

# ใช้ aggregateByKey เพื่อรวมข้อมูล
agg = rdd.aggregateByKey(zero_val, par_agg, allpar_agg).take(10)

# แสดงผลลัพธ์
print(agg)


"""
อธิบายเพิ่มเติม:
aggregateByKey: ฟังก์ชันนี้ทำการรวมข้อมูลใน RDD ตามคีย์
 โดยใช้ฟังก์ชันที่กำหนดในการรวมค่าภายในแต่ละ partition (par_agg) และฟังก์ชันในการรวมค่าระหว่าง
   partitions (allpar_agg).
par_agg (partial aggregation) จะทำการรวมค่าภายใน partition และคืนค่าที่รวมกัน
allpar_agg (aggregate) จะรวมค่าที่ได้จากแต่ละ partition
โค้ดนี้จะใช้ aggregateByKey เพื่อรวมข้อมูลตามคีย์ โดยการคำนวณผลรวมของค่าและจำนวนค่าที่พบในแต่ละ
 partition และในทั้งหมด partitions.
"""