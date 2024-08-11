from mrjob.job import MRJob
from mrjob.step import MRStep

class MapReduceSorting(MRJob):
    def mapper(self, _, line):
        if 'status_id' not in line:
            data = line.split(',')
            datatime = data[2].strip()
            num_reactions = data[3].strip()
            year = datatime.split(' ')[0].split('/')[2]
            
            if int(num_reactions) > 3000:
                yield year,num_reactions

    def reducer(self, key, values):
        yield key, sorted(values, reverse=True)
        
if (__name__ == '__main__'):
    MapReduceSorting.run()


"""
โค้ดนี้คือโปรแกรม MapReduce ที่ใช้ไลบรารี MRJob เพื่อกรองและจัดเรียงข้อมูลจากไฟล์ CSV ที่มีข้อมูลการตอบสนองของผู้ใช้ในโพสต์ Facebook:

Mapper: อ่านบรรทัดของไฟล์ CSV และกรองบรรทัดที่มีการตอบสนองมากกว่า 3000 จากนั้นส่งออกปีและจำนวนการตอบสนอง
Reducer: รับข้อมูลที่ถูกกรองจาก Mapper และจัดเรียงจำนวนการตอบสนองในแต่ละปีในลำดับจากมากไปน้อย
"""