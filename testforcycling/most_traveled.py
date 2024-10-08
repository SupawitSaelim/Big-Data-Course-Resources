from pyspark.sql import SparkSession
from graphframes import GraphFrame

spark = SparkSession.builder \
    .appName("Graph Analytics") \
    .config("spark.jars.packages", "graphframes:graphframes:0.8.2-spark3.0-s_2.12") \
    .getOrCreate()

file_path = './testforcycling/cycling.csv'
edges = spark.read.csv(file_path, header=True, inferSchema=True)

edges = edges.withColumnRenamed("FromStationName", "src").withColumnRenamed("ToStationName", "dst")

stations = edges.selectExpr("src as id").union(edges.selectExpr("dst as id")).distinct()

g = GraphFrame(stations, edges)

degree_df = g.degrees

most_connected_station = degree_df.orderBy("degree", ascending=False).first()
print(f"สถานีที่มีการเชื่อมต่อมากที่สุด: {most_connected_station['id']} (Degree: {most_connected_station['degree']})")

route_count_df = edges.groupBy("src", "dst").count()

most_frequent_route = route_count_df.orderBy("count", ascending=False).first()
print(f"เส้นทางที่มีการใช้งานบ่อยที่สุด: {most_frequent_route['src']} -> {most_frequent_route['dst']} (Count: {most_frequent_route['count']})")
