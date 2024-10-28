from pyspark.sql import SparkSession  
from graphframes import GraphFrame  
from pyspark.sql.functions import desc, count, col, expr, avg

# สร้าง Spark Session
spark = SparkSession.builder \
    .appName("Graph Analytics") \
    .config("spark.jars.packages", "graphframes:graphframes:0.8.2-spark3.0-s_2.12") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .getOrCreate()

# สร้าง DataFrame สำหรับ vertices (โหนด) โดยมีข้อมูลเกี่ยวกับชื่อและอายุ
vertices = spark.createDataFrame([
    ("Alice", 45),
    ("Jacob", 43),
    ("Roy", 21),
    ("Ryan", 49),
    ("Emily", 24),
    ("Sheldon", 52),
    ("Liam", 30),    # เพิ่มสมาชิกใหม่
    ("Emma", 35)     # เพิ่มสมาชิกใหม่
], ["id", "age"])  # กำหนดชื่อคอลัมน์เป็น "id" และ "age"

# สร้าง DataFrame สำหรับ edges (ขอบ) โดยมีข้อมูลเกี่ยวกับความสัมพันธ์ระหว่างโหนด
edges = spark.createDataFrame([
    ("Sheldon", "Alice", "Sister"),
    ("Alice", "Jacob", "Husband"),
    ("Emily", "Jacob", "Father"),
    ("Ryan", "Alice", "Friend"),
    ("Alice", "Emily", "Daughter"),
    ("Jacob", "Roy", "Son"),
    ("Roy", "Ryan", "Son"),
    ("Ryan", "Liam", "Colleague"),   # เพิ่มความสัมพันธ์ใหม่
    ("Emma", "Roy", "Cousin")         # เพิ่มความสัมพันธ์ใหม่
], ["src", "dst", "relation"])  # กำหนดชื่อคอลัมน์เป็น "src", "dst" และ "relation"

graph = GraphFrame(vertices, edges)

# 1. จัดกลุ่ม edges ตาม src และ dst และนับจำนวน จากนั้นเรียงลำดับ
print("Grouped and ordered edges:")  
graph.edges.groupBy("src", "dst").count().orderBy(desc("count")).show()

# 2. Filter
print("Filtered edges where src or dst is 'Alice':")  
graph.edges.where("src = 'Alice' OR dst = 'Alice'").groupBy("src", "dst").count().orderBy(desc("count")).show()  # กรอง edges ที่มี 'Alice' และนับจำนวน

# 3. Subgraph
print("Subgraph where src or dst is 'Alice':") 
subgraph_edges = graph.edges.where("src = 'Alice' OR dst = 'Alice'")  # กรอง edges สำหรับ subgraph
subgraph = GraphFrame(graph.vertices, subgraph_edges)  # สร้าง subgraph โดยใช้ vertices เดิมและ edges ที่กรอง
subgraph.edges.show()  # แสดง edges ของ subgraph

# 4. motifs
print("Finding motifs in the graph:")  
motifs = graph.find("(a) - [ab] -> (b)")  # ค้นหามอทิฟในกราฟที่กำหนด
motifs.show()

print("Finding complex motifs in the graph:")  
motifs = graph.find("(a) - [ab] -> (b)") \
               .filter("ab.relation = 'Husband' OR ab.relation = 'Father'")  # กรองมอทิฟที่มี 'Husband' หรือ 'Father'
motifs.show()

# 5. PageRank
print("Calculating PageRank:")  
rank = graph.pageRank(resetProbability=0.15, maxIter=10)  # คำนวณ PageRank โดยใช้ค่า resetProbability และ maxIter
rank.vertices.orderBy(desc("pagerank")).show()

# 6. In-Degree
print("In-Degree of nodes:") 
in_degree = graph.inDegrees  # คำนวณ In-Degree ของโหนด
in_degree.orderBy(desc("inDegree")).show()  # แสดง In-Degree และเรียงลำดับ

# 7. Out-Degree
print("Out-Degree of nodes:")  
out_degree = graph.outDegrees  # คำนวณ Out-Degree ของโหนด
out_degree.orderBy(desc("outDegree")).show()  # แสดง Out-Degree และเรียงลำดับ

# 8. Connected Components
print("Finding connected components:")  
spark.sparkContext.setCheckpointDir("./tmp/checkpoints")  # ตั้งค่า directory สำหรับ checkpoint (จำเป็น)
cc = graph.connectedComponents()  # ค้นหาส่วนเชื่อมต่อในกราฟ
cc.show()  

print("Finding strongly connected components:") 
scc = graph.stronglyConnectedComponents(maxIter=5)  # ค้นหาส่วนเชื่อมต่อที่แข็งแกร่งโดยกำหนดจำนวนรอบ
scc.show() 

# 9. BFS
print("Performing BFS from node 'Alice' to node 'Roy':")  
bfs_result = graph.bfs(fromExpr="id = 'Alice'", toExpr="id = 'Roy'", maxPathLength=3)  # เพิ่ม maxPathLength
bfs_result.show()  

# 10. Average Age of Nodes in Connected Components
print("Average Age of Nodes in Connected Components:")
vertices_with_cc = cc.select("id", "component")
avg_age = vertices_with_cc.join(vertices, "id") \
                            .groupBy("component") \
                            .agg(avg("age").alias("average_age"))  # คำนวณอายุเฉลี่ยของโหนดในแต่ละส่วนเชื่อมต่อ
avg_age.show()

# 11. Count Relations
print("Count of Relations in the Graph:")
relation_counts = graph.edges.groupBy("relation").agg(count("relation").alias("count"))  # นับความสัมพันธ์ต่างๆ
relation_counts.orderBy(desc("count")).show()

# 12. Degree Distribution
print("Degree Distribution:")
degree_distribution = in_degree.join(out_degree, "id", "outer").fillna(0) \
                                 .select(col("id"), (col("inDegree") + col("outDegree")).alias("totalDegree"))  # คำนวณการกระจายของ Degree
degree_distribution.show()


spark.stop()
