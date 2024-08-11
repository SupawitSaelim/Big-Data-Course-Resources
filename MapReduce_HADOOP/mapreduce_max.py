from mrjob.job import MRJob
from mrjob.step import MRStep
class MapReduceMax(MRJob):
    def mapper(self, _, line):
        if 'status_id' not in line:
            data = line.split(',')  # Data is a list of values in each line of a file
            date = data[2].strip()
            year = date.split(' ')[0].split('/')[2]
            status_type = data[1].strip()  # Get the status type
            if status_type in ['video', 'photo', 'link', 'status']:
                yield (year, status_type), 1

    def reducer_count(self, key, values):
        yield key[0], (sum(values), key[1])

    def reducer_max(self, key, values):
        max_count = 0
        status_type = None
        for count, st in values:
            if count > max_count:
                max_count = count
                status_type = st
        yield key, (status_type, max_count)

    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer_count),
            MRStep(reducer=self.reducer_max)
        ]

if __name__ == '__main__':
    MapReduceMax.run()


"""
โค้ดที่คุณเขียนใช้ `mrjob` เพื่อคำนวณค่าสูงสุดของจำนวนสถานะแต่ละประเภท (video, photo, link, status) สำหรับแต่ละปีจากข้อมูลที่มีคอลัมน์ต่าง ๆ โดยโค้ดมีดังนี้:

### การทำงานของโค้ด

1. **Mapper Function:**
   ```python
   def mapper(self, _, line):
       if 'status_id' not in line:
           data = line.split(',')  # แยกข้อมูลแต่ละบรรทัดตามเครื่องหมายคอมม่า
           date = data[2].strip()  # ดึงวันที่
           year = date.split(' ')[0].split('/')[2]  # ดึงปีจากวันที่ (dd/mm/yyyy)
           status_type = data[1].strip()  # ดึงประเภทสถานะ
           if status_type in ['video', 'photo', 'link', 'status']:
               yield (year, status_type), 1  # ส่งออกเป็นคู่ (ปี, ประเภทสถานะ) และค่า 1
   ```
   - **`if 'status_id' not in line:`**: ข้ามบรรทัดหัวข้อ
   - **`data = line.split(',')`**: แยกบรรทัดข้อมูลเป็นรายการ
   - **`year = date.split(' ')[0].split('/')[2]`**: ดึงปีจากวันที่
   - **`yield (year, status_type), 1`**: ส่งออกเป็นคู่ (ปี, ประเภทสถานะ) และค่า 1 เพื่อใช้ในการนับ

2. **Reducer Function (`reducer_count`):**
   ```python
   def reducer_count(self, key, values):
       yield key[0], (sum(values), key[1])
   ```
   - **`yield key[0], (sum(values), key[1])`**: นับจำนวนของแต่ละ (ปี, ประเภทสถานะ) และส่งออกเป็นปีและจำนวนรวมพร้อมประเภทสถานะ

3. **Reducer Function (`reducer_max`):**
   ```python
   def reducer_max(self, key, values):
       max_count = 0
       status_type = None
       for count, st in values:
           if count > max_count:
               max_count = count
               status_type = st
       yield key, (status_type, max_count)
   ```
   - **`for count, st in values:`**: ตรวจสอบค่าทั้งหมดที่ได้รับจาก `reducer_count`
   - **`if count > max_count:`**: เปรียบเทียบเพื่อหาค่าสูงสุด
   - **`yield key, (status_type, max_count)`**: ส่งออกปีและประเภทสถานะที่มีจำนวนสูงสุด

4. **Steps Function:**
   ```python
   def steps(self):
       return [
           MRStep(mapper=self.mapper, reducer=self.reducer_count),
           MRStep(reducer=self.reducer_max)
       ]
   ```
   - **`steps()`**: กำหนดลำดับขั้นตอนของ MapReduce job โดยมีสองขั้นตอนคือการ map และการลด (reduction)

5. **Main Section:**
   ```python
   if __name__ == '__main__':
       MapReduceMax.run()
   ```
   - **`MapReduceMax.run()`**: เรียกใช้ MapReduce job เมื่อสคริปต์นี้ถูกเรียกใช้งานโดยตรง

### สรุป
โค้ดนี้จะคำนวณจำนวนสูงสุดของแต่ละประเภทสถานะในแต่ละปี โดยใช้ขั้นตอน mapper เพื่อแยกและนับจำนวน, reducer เพื่อคำนวณจำนวนรวม, และ reducer อีกตัวเพื่อหาค่าสูงสุดจากจำนวนที่นับได้.
"""