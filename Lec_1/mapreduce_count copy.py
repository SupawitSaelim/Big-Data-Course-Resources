from mrjob.job import MRJob

class MapReduceCount(MRJob):
    def mapper(self, _, line):
        data = line.split(',')
        status_type = data[1].strip()
        if status_type == 'link':
            yield 'link', 1
        elif status_type == 'video':
            yield 'video', 1
    def reducer(self, key, value):
        yield key, sum(value)

if (__name__ == '__main__'):
    MapReduceCount.run() 