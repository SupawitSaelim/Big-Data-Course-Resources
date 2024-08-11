from mrjob.job import MRJob
from mrjob.step import MRStep

class MapReduceInverted(MRJob):
    def mapper(self, _, line):
        if 'status_id' not in line:
            data = line.split(',')
            status_type = data[1].strip()
            num_reactions = data[3].strip()
            
            yield status_type,num_reactions

    def reducer(self, key, values):
        lval = []
        for react in values:
            lval.append(react)
        yield key, lval
                    
if (__name__ == '__main__'):
    MapReduceInverted.run()

    """
    สรุป
Mapper: แยกข้อมูลจากแต่ละบรรทัดในไฟล์ CSV และส่งออกคู่ค่า (status_type, num_reactions).
Reducer: รวบรวมค่าของ num_reactions สำหรับแต่ละ status_type และส่งออกเป็นรายการ (list) ของ num_reactions.
โค้ดนี้สร้าง Inverted Index ที่แสดงรายการจำนวนการตอบสนอง (num_reactions) ที่เกี่ยวข้องกับแต่ละประเภทสถานะ (status_type)
 เช่น "photo" หรือ "video" ซึ่งทำให้เราสามารถดูจำนวนการตอบสนองที่เกี่ยวข้องกับแต่ละประเภทสถานะได้อย่างรวดเร็ว.
    """