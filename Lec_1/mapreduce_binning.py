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
                # if status_type == 'photo':
                #     yield 'Photo', data
                # elif status_type == 'video':
                #     yield 'Video', data
                # elif status_type == 'link':
                #     yield 'Link', data
                # elif status_type == 'status':
                #     yield 'Status', data
                
                yield status_type, data

    # def reducer(self, key, _):
    #     yield key, None

if (__name__ == '__main__'):
    MapReduceBinning.run()