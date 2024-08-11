from mrjob.job import MRJob
from mrjob.step import MRStep

class MapReducefilter(MRJob):
    def mapper(self, _, line):
        if 'status_id' not in line:
            data = line.split(',') # Data is a list of values in each line of a file
            date = data[2].strip()
            year = date.split(' ')[0].split('/')[2]
            status_type = data[1].strip() # Get the status type
            num_reactions = data[3].strip()
            if year == '2018' and int(num_reactions) > 2000:
                yield status_type,num_reactions
    
if (__name__ == '__main__'):
    MapReducefilter.run()


"""
โค้ดที่คุณให้มาเป็นตัวอย่างการใช้ `MRJob` เพื่อประมวลผลข้อมูลด้วย MapReduce โดยมีการกรองข้อมูลตามเงื่อนไขที่กำหนด นี่คือลักษณะการทำงานของโค้ด:

### อธิบายโค้ด

1. **การนำเข้าโมดูล:**
   ```python
   from mrjob.job import MRJob
   from mrjob.step import MRStep
   ```
   - นำเข้า `MRJob` จาก `mrjob` สำหรับสร้างและรันโปรแกรม MapReduce.
   - `MRStep` ใช้ในการกำหนดขั้นตอนต่างๆ ของ MapReduce, แต่ในโค้ดนี้ไม่ใช้ `MRStep` เลย.

2. **การสร้างคลาส `MapReducefilter`:**
   ```python
   class MapReducefilter(MRJob):
   ```
   - คลาสนี้สืบทอดมาจาก `MRJob` และเป็นการกำหนดโปรแกรม MapReduce ของคุณ.

3. **ฟังก์ชัน `mapper`:**
   ```python
   def mapper(self, _, line):
       if 'status_id' not in line:
           data = line.split(',')  # Data is a list of values in each line of a file
           date = data[2].strip()
           year = date.split(' ')[0].split('/')[2]
           status_type = data[1].strip()  # Get the status type
           num_reactions = data[3].strip()
           if year == '2018' and int(num_reactions) > 2000:
               yield status_type, num_reactions
   ```
   - **`mapper(self, _, line)`**: ฟังก์ชันที่ทำหน้าที่ในการแมพข้อมูล.
   - **`if 'status_id' not in line`**: ตรวจสอบว่าแถวข้อมูลไม่ได้เป็นแถวหัวข้อ.
   - **`data = line.split(',')`**: แยกข้อมูลแต่ละบรรทัดโดยการใช้เครื่องหมายคอมมาเป็นตัวแบ่ง.
   - **`date.split(' ')[0].split('/')[2]`**: แยกปีจากวันที่ (รูปแบบ `MM/DD/YYYY`).
   - **`if year == '2018' and int(num_reactions) > 2000:`**: ตรวจสอบว่าปีคือ 2018 และจำนวนปฏิกิริยามากกว่า 2000 หรือไม่.
   - **`yield status_type, num_reactions`**: ส่งออกประเภทสถานะและจำนวนปฏิกิริยา.

4. **การรันโปรแกรม:**
   ```python
   if __name__ == '__main__':
       MapReducefilter.run()
   ```
   - รันโปรแกรม MapReduce ถ้าไฟล์นี้ถูกเรียกใช้งานโดยตรง.

### สรุป

- **Mapper**: ฟังก์ชันนี้ทำการกรองข้อมูลตามปีและจำนวนปฏิกิริยา ก่อนที่จะส่งข้อมูลที่ตรงตามเงื่อนไขออกไป
- **Data Processing**: ข้อมูลจะถูกแยกและตรวจสอบเพื่อให้ได้ข้อมูลที่ตรงตามเงื่อนไขที่กำหนด

โค้ดนี้ใช้สำหรับกรองข้อมูลจากไฟล์ CSV โดยมองหาข้อมูลในปี 2018 ที่มีจำนวนปฏิกิริยา (reactions) มากกว่า 2000 และส่งออกประเภทสถานะ (status_type) และจำนวนปฏิกิริยา.
"""