from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
alphabet = [('a', 1), ('b', 2), ('c', 3), ('a', 1), ('b', 2)]
rdd = spark.sparkContext.parallelize(alphabet, 4)

# ใช้ groupByKey เพื่อจัดกลุ่มข้อมูลตามคีย์และนับจำนวนค่าในแต่ละคีย์
group1 = sorted(rdd.groupByKey().mapValues(len).collect())
print("Group 1 (Count of values for each key):")
print(group1)

# ใช้ groupByKey เพื่อจัดกลุ่มข้อมูลตามคีย์และเก็บค่าทั้งหมดในลิสต์สำหรับแต่ละคีย์
group2 = sorted(rdd.groupByKey().mapValues(list).collect())
print("Group 2 (List of values for each key):")
print(group2)


"""
ใน PySpark, `groupByKey` และ `mapValues` เป็นฟังก์ชันที่ใช้ในการจัดกลุ่มข้อมูลและแปลงค่าของข้อมูลตามคีย์ที่ต้องการ

### การทำงานของโค้ด:

1. **สร้าง SparkSession**:
   ```python
   from pyspark.sql import SparkSession
   spark = SparkSession.builder.getOrCreate()
   ```

2. **สร้าง RDD จากรายการที่มีการใช้คีย์-ค่า**:
   ```python
   alphabet = [('a', 1), ('b', 2), ('c', 3), ('a', 1), ('b', 2)]
   rdd = spark.sparkContext.parallelize(alphabet, 4)
   ```

3. **ใช้ `groupByKey` และ `mapValues` เพื่อจัดกลุ่มข้อมูลและนับจำนวนค่าต่อคีย์**:
   ```python
   group1 = sorted(rdd.groupByKey().mapValues(len).collect())
   print("Group 1 (Count of values for each key):")
   print(group1)
   ```
   - `groupByKey()` จะจัดกลุ่มค่าตามคีย์
   - `mapValues(len)` จะนับจำนวนค่าที่เกี่ยวข้องกับแต่ละคีย์
   - `sorted()` และ `collect()` จะจัดเรียงผลลัพธ์และนำมารวมไว้ในลิสต์

4. **ใช้ `groupByKey` และ `mapValues` เพื่อจัดกลุ่มข้อมูลและเก็บค่าทั้งหมดในลิสต์สำหรับแต่ละคีย์**:
   ```python
   group2 = sorted(rdd.groupByKey().mapValues(list).collect())
   print("Group 2 (List of values for each key):")
   print(group2)
   ```
   - `groupByKey()` จะจัดกลุ่มค่าตามคีย์
   - `mapValues(list)` จะเก็บค่าทั้งหมดที่เกี่ยวข้องกับแต่ละคีย์ในลิสต์
   - `sorted()` และ `collect()` จะจัดเรียงผลลัพธ์และนำมารวมไว้ในลิสต์

### ผลลัพธ์ที่คาดหวัง

- **Group 1**: จะมีผลลัพธ์เป็นคู่ (key, count) ที่แสดงจำนวนค่าที่มีอยู่สำหรับแต่ละคีย์

  ```
  Group 1 (Count of values for each key):
  [('a', 2), ('b', 2), ('c', 1)]
  ```

- **Group 2**: จะมีผลลัพธ์เป็นคู่ (key, list of values) ที่แสดงค่าทั้งหมดสำหรับแต่ละคีย์

  ```
  Group 2 (List of values for each key):
  [('a', [1, 1]), ('b', [2, 2]), ('c', [3])]
  ```

การใช้ `groupByKey` ทำให้ค่าทั้งหมดที่มีคีย์เดียวกันถูกจัดกลุ่มเข้าด้วยกัน, และการใช้ `mapValues` ช่วยให้สามารถแปลงค่าภายในกลุ่มนั้นได้ตามที่ต้องการ.
"""