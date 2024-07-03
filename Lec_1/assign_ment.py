from mrjob.job import MRJob
from mrjob.step import MRStep
from heapq import nlargest

class MapReduceAverage(MRJob):

    def mapper(self, _, line):
        if 'status_id' not in line:
            data = line.split(',')
            date = data[2].strip()
            year = date.split(' ')[0].split('/')[2]
            status_type = data[1].strip()
            if status_type == 'video' or status_type == 'photo' or status_type == 'link' or status_type == 'status':
                yield (year, status_type), 1

    def reducer_count(self, key, values):
        yield key[0], (sum(values), key[1])

    def reducer_max(self, key, values):
        top_n = nlargest(2, values)  # Get the top 2 values
        yield key, top_n

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer_count),
            MRStep(reducer=self.reducer_max)
        ]

if __name__ == '__main__':
    MapReduceAverage.run()
