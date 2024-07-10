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