from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CSV_Example").getOrCreate()
alphabet = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h']
rdd = spark.sparkContext.parallelize(alphabet, 4)
print("Number of partitions for alphabet RDD: " + str(rdd.getNumPartitions()))

csv_file_path = 'Low-Level/fb_live_thailand.csv'
rdd2 = spark.sparkContext.textFile(csv_file_path, 5)
print("Number of partitions for CSV RDD: " + str(rdd2.getNumPartitions()))

header = rdd2.first()  # Extract the header
data = rdd2.filter(lambda line: line != header)  # Filter out the header

print("Header: " + header)
print("Data: ")
data.take(10)

columns = header.split(",")
data_rdd = data.map(lambda line: line.split(","))

row_count = data_rdd.count()
print("Number of rows in CSV: " + str(row_count))

def extract_num_reactions(row):
    try:
        return int(row[3])
    except ValueError:
        return 0

total_reactions = data_rdd.map(extract_num_reactions).sum()
print("Total number of reactions: " + str(total_reactions))

spark.stop()
