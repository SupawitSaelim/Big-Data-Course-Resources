from pyspark.sql import SparkSession

# สร้าง SparkSession
spark = SparkSession.builder.getOrCreate()

# สร้าง RDD แรก
alphabet1 = [('a', 1), ('b', 2), ('c', 3)]
rdd1 = spark.sparkContext.parallelize(alphabet1)

# สร้าง RDD ที่สอง
alphabet2 = [('a', 1), ('b', 2), ('a', 1), ('b', 2)]
rdd2 = spark.sparkContext.parallelize(alphabet2)

# ใช้ join เพื่อรวมข้อมูลจากทั้งสอง RDD ตามคีย์ที่ตรงกัน
joinRDD = rdd1.join(rdd2).collect()
print("Join:")
print(joinRDD)

# ใช้ leftOuterJoin เพื่อรวมข้อมูลจาก RDD แรก (ซ้าย) กับ RDD ที่สอง (ขวา)
left = rdd1.leftOuterJoin(rdd2).collect()
print("Left Outer Join:")
print(left)

# ใช้ rightOuterJoin เพื่อรวมข้อมูลจาก RDD ที่สอง (ขวา) กับ RDD แรก (ซ้าย)
right = rdd1.rightOuterJoin(rdd2).collect()
print("Right Outer Join:")
print(right)

"""
การใช้ `join`, `leftOuterJoin`, และ `rightOuterJoin` ใน PySpark จะช่วยในการรวมข้อมูลจากสอง RDD ตามคีย์ที่ตรงกัน โดยแต่ละฟังก์ชันมีพฤติกรรมที่แตกต่างกัน:

1. **`join`**: จะรวมข้อมูลจากสอง RDD โดยรวมเฉพาะคีย์ที่มีในทั้งสอง RDD
2. **`leftOuterJoin`**: จะรวมข้อมูลจาก RDD แรก (ซ้าย) กับข้อมูลใน RDD ที่สอง (ขวา) รวมถึงค่าจาก RDD แรกที่ไม่มีใน RDD ที่สอง โดยให้ค่าเป็น `None` สำหรับคีย์ที่ไม่มีใน RDD ที่สอง
3. **`rightOuterJoin`**: จะรวมข้อมูลจาก RDD ที่สอง (ขวา) กับข้อมูลใน RDD แรก (ซ้าย) รวมถึงค่าจาก RDD ที่สองที่ไม่มีใน RDD แรก โดยให้ค่าเป็น `None` สำหรับคีย์ที่ไม่มีใน RDD แรก


### การทำงานของโค้ด:

1. **สร้าง SparkSession**:
   ```python
   from pyspark.sql import SparkSession
   spark = SparkSession.builder.getOrCreate()
   ```

2. **สร้าง RDD แรก**:
   ```python
   alphabet1 = [('a', 1), ('b', 2), ('c', 3)]
   rdd1 = spark.sparkContext.parallelize(alphabet1)
   ```

3. **สร้าง RDD ที่สอง**:
   ```python
   alphabet2 = [('a', 1), ('b', 2), ('a', 1), ('b', 2)]
   rdd2 = spark.sparkContext.parallelize(alphabet2)
   ```

4. **ใช้ `join` เพื่อรวมข้อมูลตามคีย์ที่ตรงกัน**:
   ```python
   joinRDD = rdd1.join(rdd2).collect()
   print("Join:")
   print(joinRDD)
   ```

   - **ผลลัพธ์**: จะเป็นคู่ (key, (value1, value2)) สำหรับคีย์ที่ตรงกันในทั้งสอง RDD

5. **ใช้ `leftOuterJoin` เพื่อรวมข้อมูลจาก RDD แรกกับ RDD ที่สอง**:
   ```python
   left = rdd1.leftOuterJoin(rdd2).collect()
   print("Left Outer Join:")
   print(left)
   ```

   - **ผลลัพธ์**: จะเป็นคู่ (key, (value1, value2)) สำหรับคีย์ที่มีใน RDD แรก และ `None` สำหรับคีย์ที่ไม่มีใน RDD ที่สอง

6. **ใช้ `rightOuterJoin` เพื่อรวมข้อมูลจาก RDD ที่สองกับ RDD แรก**:
   ```python
   right = rdd1.rightOuterJoin(rdd2).collect()
   print("Right Outer Join:")
   print(right)
   ```

   - **ผลลัพธ์**: จะเป็นคู่ (key, (value1, value2)) สำหรับคีย์ที่มีใน RDD ที่สอง และ `None` สำหรับคีย์ที่ไม่มีใน RDD แรก

### ผลลัพธ์ที่คาดหวัง:

- **Join**:
  ```
  [('a', (1, 1)), ('a', (1, 1)), ('b', (2, 2)), ('b', (2, 2))]
  ```

- **Left Outer Join**:
  ```
  [('a', (1, 1)), ('a', (1, 1)), ('b', (2, 2)), ('b', (2, 2)), ('c', (3, None))]
  ```

- **Right Outer Join**:
  ```
  [('a', (1, 1)), ('a', (1, 1)), ('b', (2, 2)), ('b', (2, 2)), ('c', (None, 3))]
  ```

โค้ดนี้จะแสดงผลลัพธ์จากการรวมข้อมูลตามประเภทการรวมที่ต่างกัน.
"""