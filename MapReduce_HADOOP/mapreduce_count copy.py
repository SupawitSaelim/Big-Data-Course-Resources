from mrjob.job import MRJob

class MapReduceCount(MRJob):
    def mapper(self, _, line):
        data  = line.split(',')
        status_type = data[1].strip()
        if status_type == 'status':
            yield 'status', 1
        elif  status_type == 'video':
            yield 'video',1
        elif status_type == 'link':
            yield 'link', 1
        elif status_type == 'photo':
            yield 'photo', 1

    def reducer(self, key, value):
        yield  key, sum(value)

if (__name__ == '__main__'):
    MapReduceCount.run()


'''
โค้ดที่คุณให้มาคือโปรแกรม MapReduce ที่เขียนโดยใช้ `mrjob` ไลบรารีใน Python เพื่อประมวลผลข้อมูลประเภทต่าง ๆ และนับจำนวนแต่ละประเภท นี่คือการอธิบายแต่ละส่วนของโค้ด:

### โครงสร้างของโค้ด

```python
from mrjob.job import MRJob

class MapReduceCount(MRJob):
    def mapper(self, _, line):
        data = line.split(',')
        status_type = data[1].strip()
        if status_type == 'status':
            yield 'status', 1
        elif status_type == 'video':
            yield 'video', 1
        elif status_type == 'link':
            yield 'link', 1
        elif status_type == 'photo':
            yield 'photo', 1

    def reducer(self, key, value):
        yield key, sum(value)

if __name__ == '__main__':
    MapReduceCount.run()
```

### อธิบายส่วนต่าง ๆ ของโค้ด

1. **การนำเข้าไลบรารี (`import` statement)**:
   ```python
   from mrjob.job import MRJob
   ```
   - นำเข้า `MRJob` จากไลบรารี `mrjob` ซึ่งเป็นคลาสหลักที่ใช้ในการสร้างงาน MapReduce

2. **การสร้างคลาส `MapReduceCount`**:
   ```python
   class MapReduceCount(MRJob):
   ```
   - สร้างคลาส `MapReduceCount` ที่สืบทอดจาก `MRJob` ซึ่งเป็นคลาสพื้นฐานในการสร้างงาน MapReduce ด้วย `mrjob`

3. **การกำหนดฟังก์ชัน `mapper`**:
   ```python
   def mapper(self, _, line):
       data = line.split(',')
       status_type = data[1].strip()
       if status_type == 'status':
           yield 'status', 1
       elif status_type == 'video':
           yield 'video', 1
       elif status_type == 'link':
           yield 'link', 1
       elif status_type == 'photo':
           yield 'photo', 1
   ```
   - `mapper` คือฟังก์ชันที่ประมวลผลข้อมูลแถวละบรรทัด
   - `line.split(',')` แยกบรรทัดข้อมูลออกเป็นรายการโดยใช้เครื่องหมายจุลภาค (`,` ) เป็นตัวแบ่ง
   - `status_type = data[1].strip()` ดึงข้อมูลประเภทสถานะจากคอลัมน์ที่ 2 (ซึ่ง index คือ 1) และลบช่องว่างที่ไม่ต้องการ
   - `yield` จะส่งคืนคู่ค่าที่ประกอบด้วย `status_type` และค่า 1 (สำหรับการนับ)

4. **การกำหนดฟังก์ชัน `reducer`**:
   ```python
   def reducer(self, key, value):
       yield key, sum(value)
   ```
   - `reducer` จะรวมค่าที่ส่งมาจาก `mapper`
   - `key` คือประเภทของสถานะ เช่น 'status', 'video', 'link', 'photo'
   - `value` คือรายการของค่าที่ส่งมาจาก `mapper`
   - `sum(value)` จะรวมค่าทั้งหมดที่เกี่ยวข้องกับแต่ละ `key` เพื่อให้ได้จำนวนรวม

5. **การรันงาน MapReduce**:
   ```python
   if __name__ == '__main__':
       MapReduceCount.run()
   ```
   - ตรวจสอบว่าโค้ดกำลังรันในสคริปต์หลัก (`__main__`)
   - เรียกใช้ `MapReduceCount.run()` ซึ่งจะเริ่มต้นกระบวนการ MapReduce

### การทำงานของโปรแกรม

1. **Mapper**:
   - อ่านแต่ละบรรทัดจากไฟล์ CSV
   - แยกข้อมูลโดยใช้คอมมา (`,`) และดึงข้อมูลประเภทสถานะจากคอลัมน์ที่ 2
   - ส่งคู่ค่า (key-value) ไปยัง Reducer โดยที่ key คือประเภทของสถานะ และ value คือ 1

2. **Reducer**:
   - รับคู่ค่าจาก Mapper และรวมค่า 1 สำหรับแต่ละประเภทสถานะ
   - ส่งออกผลลัพธ์ที่เป็นคู่ค่า (key, total count) สำหรับแต่ละประเภทสถานะ

โปรแกรมนี้จะช่วยให้คุณสามารถนับจำนวนบรรทัดในไฟล์ CSV ที่มีประเภทสถานะต่าง ๆ เช่น 'status', 'video', 'link', และ 'photo' และให้ผลลัพธ์เป็นจำนวนรวมสำหรับแต่ละประเภทสถานะ
'''