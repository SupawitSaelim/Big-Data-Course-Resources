from mrjob.job import MRJob
from mrjob.step import MRStep

class MapReduceDistinct(MRJob):
    def mapper(self, _, line):
        if 'status_id' not in line:
            data = line.split(',')
            datetime = data[2].strip()
            year = datetime.split(' ')[0].split('/')[2]
            date = datetime.split(' ')[0].split('/')[0]

            if year == '2018':
                yield date, None

    def reducer(self, key, _):
        yield key, None

if (__name__ == '__main__'):
    MapReduceDistinct.run()


"""
โค้ดนี้เป็นโปรแกรม MapReduce ใช้ `mrjob` เพื่อดึงและแสดงวันในปี 2018 จากข้อมูล. นี่คืออธิบายสั้นๆ ของแต่ละฟังก์ชันและการทำงานของโค้ด:

### ฟังก์ชันและการทำงาน

1. **นำเข้าโมดูล:**
   ```python
   from mrjob.job import MRJob
   from mrjob.step import MRStep
   ```
   - นำเข้า `MRJob` และ `MRStep` จากไลบรารี `mrjob`.

2. **การสร้างคลาส `MapReduceDistinct`:**
   ```python
   class MapReduceDistinct(MRJob):
   ```
   - คลาสนี้สืบทอดจาก `MRJob` และใช้สำหรับกำหนดโปรแกรม MapReduce.

3. **ฟังก์ชัน `mapper`:**
   ```python
   def mapper(self, _, line):
       if 'status_id' not in line:
           data = line.split(',')
           datetime = data[2].strip()
           year = datetime.split(' ')[0].split('/')[2]
           date = datetime.split(' ')[0].split('/')[0]

           if year == '2018':
               yield date, None
   ```
   - **`if 'status_id' not in line:`**: ตรวจสอบว่าแถวไม่ใช่หัวข้อ.
   - **`data = line.split(',')`**: แยกข้อมูลแต่ละบรรทัดด้วยคอมมา.
   - **`datetime = data[2].strip()`**: ดึงวันที่และเวลา.
   - **`year = datetime.split(' ')[0].split('/')[2]`**: แยกปีจากวันที่.
   - **`date = datetime.split(' ')[0].split('/')[0]`**: แยกวันจากวันที่.
   - **`if year == '2018':`**: ตรวจสอบปี.
   - **`yield date, None`**: ส่งออกวัน (date) สำหรับปี 2018.

4. **ฟังก์ชัน `reducer`:**
   ```python
   def reducer(self, key, _):
       yield key, None
   ```
   - **`yield key, None`**: ส่งออกวันที่ได้รับจาก mapper โดยไม่มีการเปลี่ยนแปลง.

5. **การรันโปรแกรม:**
   ```python
   if __name__ == '__main__':
       MapReduceDistinct.run()
   ```
   - รันโปรแกรม MapReduce ถ้าไฟล์นี้ถูกเรียกใช้งานโดยตรง.

### สรุป
- **Mapper**: ดึงวันจากวันที่ในปี 2018 และส่งออกวัน.
- **Reducer**: ส่งออกวันที่ได้จาก mapper โดยไม่มีการกรองค่าซ้ำ.
- **การทำงาน**: โปรแกรมนี้จะให้ผลลัพธ์เป็นวันที่ในปี 2018 โดยแสดงวันที่จากคอลัมน์วันที่ในข้อมูลที่ให้มา.
"""