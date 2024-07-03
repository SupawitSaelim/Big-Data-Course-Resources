from mrjob.job import MRJob

class MapReduceAverage(MRJob):
    def mapper(self, _, line):
        if 'status_id' not in line:
            data = line.split(',') # Data is a list of values in each line of a file
            status_type = data[1].strip() # Get the status type
            num_reactions = data[3].strip() # Get the number of reactions
            yield status_type, float(num_reactions) # Keep key and value in memory

    def reducer(self, key, values):
        lval = list(values) # Create a list
        yield key, round(sum(lval)/len(lval),2) # Calculate average

if (__name__ == '__main__'):
    MapReduceAverage.run() 