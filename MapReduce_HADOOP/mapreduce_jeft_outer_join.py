from mrjob.job import MRJob
from mrjob.step import MRStep

class MapReduceleftoutterjoin(MRJob):
    def mapper(self, _, line):
        if 'status_id' not in line:
            data = line.split(',')
            id = data[1]
            yield id, data

    def reducer(self, key, values):
        file_1 = []
        file_2 = []
        
        # Separate values into file_1 and file_2 lists based on the first element of each value
        for v in values:
            if v[0] == 'FB2':
                file_1.append(v)
            elif v[0] == 'FB3':
                file_2.append(v)
        
        # Yield combinations of values from file_1 and file_2
        for value1 in file_1:
            if file_2:  # Check if file_2 is not empty
                for value2 in file_2:
                    yield None, (value1 + value2)
            else:  # If file_2 is empty, yield only value from file_1
                yield None, value1

if __name__ == '__main__':
    MapReduceleftoutterjoin.run()



"""
### คำอธิบายโค้ด

โค้ดที่ให้มาคือโค้ด `MapReduce` โดยใช้ `MRJob` ที่มีการทำงานหลักเพื่อสร้าง inverted index และจับคู่ข้อมูลระหว่างสองประเภท (`FB2` และ `FB3`) ของข้อมูลที่มีคีย์เดียวกัน. นี่คือการอธิบายแต่ละบรรทัด:

```python
from mrjob.job import MRJob
from mrjob.step import MRStep

class MapReduceInvertedIndex(MRJob):
    def mapper(self, _, line):
        # ข้ามแถวหัวข้อ (header) ที่เริ่มต้นด้วย 'status_id'
        if 'status_id' not in line:
            # แยกแต่ละบรรทัดที่มีคอมม่าเป็นตัวคั่น
            data = line.split(',')
            # ใช้ค่าจากคอลัมน์ที่สองเป็นคีย์
            id = data[1]
            # ส่งออก ID เป็นคีย์และข้อมูลทั้งหมดเป็นค่า
            yield id, data

    def reducer(self, key, values):
        file_1 = []  # รายการเก็บข้อมูลจาก FB2
        file_2 = []  # รายการเก็บข้อมูลจาก FB3
        
        # แยกข้อมูลตามประเภท FB2 และ FB3
        for v in values:
            if v[0] == 'FB2':
                file_1.append(v)  # เก็บข้อมูล FB2 ลงใน file_1
            elif v[0] == 'FB3':
                file_2.append(v)  # เก็บข้อมูล FB3 ลงใน file_2
        
        # ตรวจสอบว่ามีข้อมูลใน file_2 หรือไม่
        if file_2:
            # จับคู่ข้อมูลจาก file_1 และ file_2
            for value1 in file_1:
                for value2 in file_2:
                    yield None, (value1 + value2)  # ส่งออกการรวมข้อมูล
        else:
            # ถ้า file_2 ไม่มีข้อมูล ให้ส่งออกเฉพาะข้อมูลจาก file_1
            for value1 in file_1:
                yield None, value1

if __name__ == '__main__':
    MapReduceInvertedIndex.run()
```

### การทำงานของโค้ด:

1. **Mapper:**
   - รับข้อมูลจากแต่ละบรรทัด
   - แยกข้อมูลโดยใช้คอมม่า (`,`) เป็นตัวคั่น
   - ใช้ค่าจากคอลัมน์ที่สอง (ID) เป็นคีย์
   - ส่งออก `ID` เป็นคีย์และข้อมูลทั้งหมด (รายการ) เป็นค่า

2. **Reducer:**
   - แยกข้อมูลออกเป็นสองกลุ่ม (`file_1` สำหรับ `FB2` และ `file_2` สำหรับ `FB3`)
   - ถ้ามีข้อมูลใน `file_2`:
     - จับคู่ข้อมูลจาก `file_1` และ `file_2` โดยการรวมข้อมูลทั้งสองกลุ่ม
   - ถ้าไม่มีข้อมูลใน `file_2`:
     - ส่งออกข้อมูลจาก `file_1` เท่านั้น

### ตัวอย่างข้อมูล:

#### ข้อมูลนำเข้า:

```
FB2,246675545449582_1649696485147474,video,4/22/2018 6:00,529,512
FB3,246675545449582_1649696485147474,video,4/22/2018 6:00,529,512
FB2,246675545449582_1649426988507757,photo,4/21/2018 22:45,150,0
```

#### ข้อมูลที่ส่งออกจาก Mapper:

```
(246675545449582_1649696485147474, ['FB2', '246675545449582_1649696485147474', 'video', '4/22/2018 6:00', '529', '512'])
(246675545449582_1649696485147474, ['FB3', '246675545449582_1649696485147474', 'video', '4/22/2018 6:00', '529', '512'])
```

#### ข้อมูลที่ส่งออกจาก Reducer:

```
(None, ['FB2', '246675545449582_1649696485147474', 'video', '4/22/2018 6:00', '529', '512', 'FB3', '246675545449582_1649696485147474', 'video', '4/22/2018 6:00', '529', '512'])
```

### คำอธิบายของ Output:
- **Output**: ข้อมูลที่ส่งออกจาก Reducer จะเป็นการรวมข้อมูลจาก `file_1` และ `file_2` ที่มีคีย์เดียวกัน (ในกรณีนี้คือ `246675545449582_1649696485147474`).
"""