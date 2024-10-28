from pyspark.sql import SparkSession  
from graphframes import GraphFrame  
from pyspark.sql.functions import desc 

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
    ("Sheldon", 52)
], ["id", "age"])  # กำหนดชื่อคอลัมน์เป็น "id" และ "age"

# สร้าง DataFrame สำหรับ edges (ขอบ) โดยมีข้อมูลเกี่ยวกับความสัมพันธ์ระหว่างโหนด
edges = spark.createDataFrame([
    ("Sheldon", "Alice", "Sister"),
    ("Alice", "Jacob", "Husband"),
    ("Emily", "Jacob", "Father"),
    ("Ryan", "Alice", "Friend"),
    ("Alice", "Emily", "Daughter"),
    ("Jacob", "Roy", "Son"),
    ("Roy", "Ryan", "Son")
], ["src", "dst", "relation"])  # กำหนดชื่อคอลัมน์เป็น "src", "dst" และ "relation"

graph = GraphFrame(vertices, edges)

 # จัดกลุ่ม edges ตาม src และ dst และนับจำนวน จากนั้นเรียงลำดับ################################
print("Grouped and ordered edges:")  
graph.edges.groupBy("src", "dst").count().orderBy(desc("count")).show()

############## Filter ##################################################################
print("Filtered edges where src or dst is 'Alice':")  
graph.edges.where("src = 'Alice' OR dst = 'Alice'").groupBy("src", "dst").count().orderBy(desc("count")).show()  # กรอง edges ที่มี 'Alice' และนับจำนวน

############## Subgraph ##################################################################
print("Subgraph where src or dst is 'Alice':") 
subgraph_edges = graph.edges.where("src = 'Alice' OR dst = 'Alice'")  # กรอง edges สำหรับ subgraph
subgraph = GraphFrame(graph.vertices, subgraph_edges)  # สร้าง subgraph โดยใช้ vertices เดิมและ edges ที่กรอง
subgraph.edges.show()  # แสดง edges ของ subgraph

############## motifs ##################################################################
print("Finding motifs in the graph:")  
motifs = graph.find("(a) - [ab] -> (b)")  # ค้นหามอทิฟในกราฟที่กำหนด
motifs.show()
print("Finding complex motifs in the graph:")  
motifs = graph.find("(a) - [ab] -> (b)") \
               .filter("ab.relation = 'Husband' OR ab.relation = 'Father'")  # กรองมอทิฟที่มี 'Husband' หรือ 'Father'
motifs.show()

############## PageRank ##################################################################
print("Calculating PageRank:")  
rank = graph.pageRank(resetProbability=0.15, maxIter=5)  # คำนวณ PageRank โดยใช้ค่า resetProbability และ maxIter
rank.vertices.orderBy(desc("pagerank")).show()

# rank = graph.pageRank(resetProbability=0.15, maxIter=5)  # คำนวณ PageRank
# rank_vertices = rank.vertices
# filtered_rank = rank_vertices.where("age > 30")  # กรองเฉพาะโหนดที่มีอายุมากกว่า 30
# filtered_rank.orderBy(desc("pagerank")).show()  # แสดงผลลัพธ์ที่กรองและเรียงตาม PageRank

############## In-Degree ##################################################################
print("In-Degree of nodes:") 
in_degree = graph.inDegrees  # คำนวณ In-Degree ของโหนด
in_degree.orderBy(desc("inDegree")).show()  # แสดง In-Degree และเรียงลำดับ

############## Out-Degree ##################################################################
print("Out-Degree of nodes:")  
out_degree = graph.outDegrees  # คำนวณ Out-Degree ของโหนด
out_degree.orderBy(desc("outDegree")).show()  # แสดง Out-Degree และเรียงลำดับ

############## Connected ##################################################################
print("Finding connected components:")  
spark.sparkContext.setCheckpointDir("./tmp/checkpoints")  # ตั้งค่า directory สำหรับ checkpoint (จำเป็น)
cc = graph.connectedComponents()  # ค้นหาส่วนเชื่อมต่อในกราฟ
cc.show()  
print("Finding strongly connected components:") 
scc = graph.stronglyConnectedComponents(maxIter=5)  # ค้นหาส่วนเชื่อมต่อที่แข็งแกร่งโดยกำหนดจำนวนรอบ
scc.show() 

############## BFS ##################################################################
print("Performing BFS from node 'Alice' to node 'Roy':")  
bfs_result = graph.bfs(fromExpr="id = 'Alice'", toExpr="id = 'Roy'", maxPathLength=2)  
bfs_result.show()  

spark.stop()  