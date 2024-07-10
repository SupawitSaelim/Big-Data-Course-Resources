from mrjob.job import MRJob
from mrjob.step import MRStep

class MapReduceInvertedIndex(MRJob):
    def mapper(self, _, line):
        if 'status_id' not in line:
            data = line.split(',') # Data is a list of values in each line of a file
            
            id = data[1]

            yield id, data



    def reducer(self, key, values):
        file_1 = []
        file_2 = []
        for v in values:
            if v[0] == 'FB2':
                file_1.append(v)
            elif v[0] == 'FB3':
                file_2.append(v)

        for i in file_1:
            for j in file_2:
                yield None, (i+j)

        
    
if (__name__ == '__main__'):
    MapReduceInvertedIndex.run()