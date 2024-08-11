from mrjob.job import MRJob

class MapReduceJoin(MRJob):

    def mapper(self, _, line):
        if 'status_id' not in line:
            data = line.split(',')
            key = data[1]  # status_id as key
            yield key, data

    def reducer(self, key, values):
        first_list = []
        second_list = []

        # Separate values into two lists based on the first column value
        for value in values:
            if value[0] == 'FB2':
                first_list.append(value)
            elif value[0] == 'FB3':
                second_list.append(value)

        # Generate combinations of values from the two lists
        if first_list:
            for value1 in first_list:
                if second_list:
                    for value2 in second_list:
                        yield None, value1 + value2
                else:
                    yield None, value1
        elif second_list:
            for value2 in second_list:
                yield None, value2

if __name__ == '__main__':
    MapReduceJoin.run()