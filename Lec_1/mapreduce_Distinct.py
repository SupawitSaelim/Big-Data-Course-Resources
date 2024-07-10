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