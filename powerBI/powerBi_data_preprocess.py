# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, col, lit
spark = SparkSession.builder \
    .appName("Airline Routes Analysis") \
    .getOrCreate()

df = spark.read.csv('./PowerBI/airline_routes.csv', header=True, inferSchema=True)
vertices = df.select("source_airport").withColumnRenamed("source_airport", "id").distinct()

edges = df.select("source_airport", "destination_airport") \
    .withColumnRenamed("source_airport", "src") \
    .withColumnRenamed("destination_airport", "dst")

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

new_data = edges_grouped.select("src","dst","source_color","destination_color")

new_data.write.mode("overwrite").option("header", True).csv('./PowerBI/new_data.csv')
spark.stop()
