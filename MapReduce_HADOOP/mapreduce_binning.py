from mrjob.job import MRJob
from mrjob.step import MRStep

class MapReduceBinning(MRJob):
    def mapper(self, _, line):
        if 'status_id' not in line:
            data = line.split(',')
            datetime = data[2].strip()
            status_type = data[1].strip()
            year = datetime.split(' ')[0].split('/')[2]

            if year == '2018':
                if status_type == 'photo':
                    yield 'Photo', data
                elif status_type == 'video':
                    yield 'Video', data
                elif status_type == 'link':
                    yield 'Link', data
                elif status_type == 'status':
                    yield 'Status', data
                
if (__name__ == '__main__'):
    MapReduceBinning.run()


"""
การใช้ Binning
ในโค้ดนี้, mapper จะใช้การ binning โดยการจัดกลุ่มข้อมูลตามประเภทสถานะ (status_type) ดังนี้:

'Photo' สำหรับประเภทสถานะ 'photo'
'Video' สำหรับประเภทสถานะ 'video'
'Link' สำหรับประเภทสถานะ 'link'
'Status' สำหรับประเภทสถานะ 'status'
สรุป
Mapper: แยกข้อมูลและจัดกลุ่มตามประเภทสถานะ ('Photo', 'Video', 'Link', 'Status') สำหรับปี 2018.
Reducer: ไม่มีการกำหนด reducer ในโค้ดนี้ ดังนั้นข้อมูลที่จัดกลุ่มจะถูกส่งออกจาก mapper ตามกลุ่มที่กำหนด.
การ binning ในที่นี้หมายถึงการจัดกลุ่มข้อมูลตามประเภทสถานะที่กำหนด ซึ่งทำให้สามารถวิเคราะห์ข้อมูลตามหมวดหมู่ที่แตกต่างกันได้.
"""