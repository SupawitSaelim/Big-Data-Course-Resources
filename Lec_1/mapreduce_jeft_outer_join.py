from mrjob.job import MRJob
from mrjob.step import MRStep

class MapReduceInvertedIndex(MRJob):
    def mapper(self, _, line):
        if 'status_id' not in line:
            data = line.split(',')
            id = data[1]
            yield id, data

    def reducer(self, key, values):
        file_1 = []
        file_2 = []
        
        # Separate values into file_1 and file_2 lists based on the first element of each value
        for v in values:
            if v[0] == 'FB2':
                file_1.append(v)
            elif v[0] == 'FB3':
                file_2.append(v)
        
        # Yield combinations of values from file_1 and file_2
        for value1 in file_1:
            if file_2:  # Check if file_2 is not empty
                for value2 in file_2:
                    yield None, (value1 + value2)
            else:  # If file_2 is empty, yield only value from file_1
                yield None, value1

if __name__ == '__main__':
    MapReduceInvertedIndex.run()