from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("PySpark Test") \
    .getOrCreate()

data = [("Alice", 29), ("Bob", 31), ("Cathy", 27)]
columns = ["Name", "Age"]

df = spark.createDataFrame(data, columns)

df.show()

row_count = df.count()
print(f"Number of rows in DataFrame: {row_count}")

spark.stop()
