from pyspark.sql import SparkSession

# สร้าง SparkSession
spark = SparkSession.builder.getOrCreate()

# สร้าง RDD จากรายการที่มีการใช้คีย์-ค่า
alphabet = [('a', 1), ('b', 2), ('c', 3), ('a', 1), ('b', 2)]
rdd = spark.sparkContext.parallelize(alphabet, 4)

# ฟังก์ชันเริ่มต้น: แปลงแต่ละคีย์เป็นลิสต์ที่มีค่านั้น
def tolist(x):
    return [x]

# ฟังก์ชันการรวมภายใน partition: เพิ่มค่าลงในลิสต์
def append(x, y):
    x.append(y)
    return x

# ฟังก์ชันการรวมระหว่าง partitions: ขยายลิสต์โดยรวมค่าจากลิสต์อื่น
def extend(x, y):
    x.extend(y)
    return x

# ใช้ combineByKey เพื่อรวมค่าตามคีย์
combine = sorted(rdd.combineByKey(tolist, append, extend).collect())

# แสดงผลลัพธ์
print(combine)
