from pyspark.sql import SparkSession
from graphframes import GraphFrame
import matplotlib.pyplot as plt
import networkx as nx

spark = SparkSession.builder \
    .appName("Graph Analytics") \
    .config("spark.jars.packages", "graphframes:graphframes:0.8.2-spark3.0-s_2.12") \
    .getOrCreate()
vertices = spark.createDataFrame([
    ("Alice", 45),
    ("Jacob", 43),
    ("Roy", 21),
    ("Ryan", 49),
    ("Emily", 24),
    ("Sheldon", 52)
], ["id", "age"])
edges = spark.createDataFrame([
    ("Sheldon", "Alice", "Sister"),
    ("Alice", "Jacob", "Husband"),
    ("Emily", "Jacob", "Father"),
    ("Ryan", "Alice", "Friend"),
    ("Alice", "Emily", "Daughter"),
    ("Jacob", "Roy", "Son"),
    ("Roy", "Ryan", "Son")
], ["src", "dst", "relation"])

graph = GraphFrame(vertices, edges)

# สร้าง NetworkX graph
G = nx.DiGraph()  # หรือใช้ nx.Graph() สำหรับกราฟที่ไม่มีทิศทาง
# เพิ่มโหนด (nodes) จาก vertices
for row in vertices.collect():
    G.add_node(row['id'], age=row['age'])
# เพิ่มขอบ (edges) จาก edges
for row in edges.collect():
    G.add_edge(row['src'], row['dst'], relation=row['relation'])

plt.figure(figsize=(10, 8))  # ขนาดของกราฟ
pos = nx.spring_layout(G)  # กำหนด layout
nx.draw(G, pos, with_labels=True, node_color='lightblue', node_size=2000, font_size=16, font_weight='bold')
edge_labels = nx.get_edge_attributes(G, 'relation')
nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels, font_color='red')
plt.title("Graph Representation")
plt.show()
spark.stop()
