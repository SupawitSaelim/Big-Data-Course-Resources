from mrjob.job import MRJob
from mrjob.step import MRStep

class MapReduceAverage(MRJob):
    def mapper(self, _, line):
        if 'status_id' not in line:
            data = line.split(',') # Data is a list of values in each line of a file
            date = data[2].strip()
            year = date.split(' ')[0].split('/')[2]
            status_type = data[1].strip() # Get the status type
            if status_type =='video':
                yield (year,'video'), 1
            elif status_type =='photo':
                yield (year,'photo'), 1
            elif status_type =='link':
                yield (year,'link'), 1
            elif status_type =='status':
                yield (year,'status'), 1

    def reducer_count(self, key, value):
        yield key[0], (sum(value), key[1])

    def reducer_max(self, key, value):
        yield key,max(value)

    def steps(self):
        return [MRStep(mapper = self.mapper, \
                       reducer = self.reducer_count), \
                        MRStep(reducer = self.reducer_max)]
    
if (__name__ == '__main__'):
    MapReduceAverage.run()