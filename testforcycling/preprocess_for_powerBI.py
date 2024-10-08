from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, col, lit

spark = SparkSession.builder \
    .appName("Cycling Routes Analysis") \
    .getOrCreate()

file_path = './testforcycling/cycling.csv'
edges = spark.read.csv(file_path, header=True, inferSchema=True)

edges = edges.withColumnRenamed("FromStationName", "src").withColumnRenamed("ToStationName", "dst")

vertices = edges.select("src").union(edges.select("dst")).distinct().withColumnRenamed("src", "id")

print("Vertices DataFrame:")
vertices.show()

print("Edges DataFrame:")
edges.show()

edges_grouped = edges.groupBy("src", "dst") \
    .count() \
    .filter("count > 5") \
    .orderBy(desc("count")) \
    .withColumn("source_color", lit("#3358FF")) \
    .withColumn("destination_color", lit("#FF3F33"))

print("Grouped Edges DataFrame:")
edges_grouped.show()

edges_grouped.write.mode("overwrite").option("header", True).csv('./testforcycling/new_cycling_data.csv')

spark.stop()
