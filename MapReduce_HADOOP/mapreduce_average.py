from mrjob.job import MRJob

class MapReduceAverage(MRJob): 
    def mapper(self, _, line):
        if 'status_id' not in line:
            data = line.split(',') # Data is a list of values in each line of a file
            status_type = data[1].strip() # Get the status type
            num_reactions = data[3].strip() # Get the number of reactions
            yield status_type, float(num_reactions) # Keep key and value in memory

    def reducer(self, key, values):
        lval = list(values) # Create a list
        yield key, round(sum(lval)/len(lval),2) # Calculate average

if (__name__ == '__main__'):
    MapReduceAverage.run() 


"""
โค้ดที่คุณเขียนเป็น MapReduce job โดยใช้ `mrjob` เพื่อคำนวณค่าเฉลี่ยของ `num_reactions` สำหรับแต่ละ `status_type` ต่อไปนี้คือคำอธิบายของแต่ละส่วน:

### การทำงานของโค้ด

1. **Mapper Function:**
   - **`if 'status_id' not in line:`**: ข้ามบรรทัดที่มีหัวข้อ (header) ในข้อมูล
   - **`data = line.split(',')`**: แยกข้อมูลแต่ละบรรทัดตามเครื่องหมายคอมม่า
   - **`status_type = data[1].strip()`**: ดึงประเภทสถานะจากคอลัมน์ที่ 2 (index 1) และลบช่องว่าง
   - **`num_reactions = data[3].strip()`**: ดึงจำนวนการตอบสนองจากคอลัมน์ที่ 4 (index 3) และลบช่องว่าง
   - **`yield status_type, float(num_reactions)`**: ส่งออกคีย์และค่าของ `status_type` และ `num_reactions` ที่แปลงเป็น float

2. **Reducer Function:**
   - **`lval = list(values)`**: เปลี่ยนค่าทั้งหมดที่เกี่ยวข้องกับคีย์นั้น ๆ เป็นรายการ
   - **`yield key, round(sum(lval)/len(lval),2)`**: คำนวณค่าเฉลี่ยของ `num_reactions` สำหรับแต่ละ `status_type` และปัดเศษผลลัพธ์เป็นทศนิยม 2 ตำแหน่ง

3. **Main Section:**
   - **`if (__name__ == '__main__'):`**: ตรวจสอบให้แน่ใจว่าโค้ดจะรันเฉพาะเมื่อสคริปต์นี้ถูกเรียกใช้โดยตรง
   - **`MapreduceAverage.run()`**: เรียกใช้งาน MapReduce job

### การตรวจสอบข้อผิดพลาด

1. **การแปลง `num_reactions`:**
   ตรวจสอบให้แน่ใจว่าคอลัมน์ `num_reactions` ในข้อมูลของคุณมีค่าที่เป็นตัวเลขทั้งหมด การแปลงเป็น float จะล้มเหลวหากมีค่าที่ไม่สามารถแปลงได้ เช่น ข้อความหรือค่าว่าง

2. **การจัดการข้อมูลที่ไม่สามารถแปลงได้:**
   ถ้าค่าของ `num_reactions` อาจมีค่าที่ไม่ใช่ตัวเลข (เช่น ช่องว่างหรือข้อความ) คุณอาจต้องเพิ่มการตรวจสอบหรือจัดการข้อผิดพลาด:

   ```python
   def mapper(self, _, line):
       if 'status_id' not in line:
           data = line.split(',')
           status_type = data[1].strip()
           num_reactions = data[3].strip()
           try:
               yield status_type, float(num_reactions)
           except ValueError:
               pass  # ข้ามค่าที่ไม่สามารถแปลงเป็น float ได้
   ```

### วิธีการรันโค้ด

- **ตรวจสอบให้แน่ใจว่า `mrjob` ติดตั้งอยู่ในสภาพแวดล้อม Python ของคุณ**:
  ```bash
  pip install mrjob
  ```

- **รันโค้ดโดยใช้คำสั่งใน terminal**:
  ```bash
  python <your_script_name>.py <input_file>
  ```

  ตัวอย่าง:
  ```bash
  python mapreduce_average.py input_data.txt
  ```

**หมายเหตุ:** แทนที่ `<your_script_name>.py` ด้วยชื่อไฟล์สคริปต์ของคุณและ `<input_file>` ด้วยไฟล์ข้อมูลที่คุณต้องการใช้

หากคุณยังพบปัญหาหรือข้อผิดพลาดในการรันโค้ด กรุณาให้ข้อมูลเพิ่มเติมเกี่ยวกับข้อผิดพลาดที่เกิดขึ้นเพื่อให้ฉันสามารถช่วยคุณได้มากขึ้น
"""