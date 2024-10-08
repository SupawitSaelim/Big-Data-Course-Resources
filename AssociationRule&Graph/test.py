# Step 3: Create SparkSession
from pyspark.sql import SparkSession
from graphframes import GraphFrame
from pyspark.sql.functions import desc

# Create SparkSession with GraphFrames
spark = SparkSession.builder \
    .appName("Graph Analytics") \
    .config("spark.jars.packages", "graphframes:graphframes:0.8.2-spark3.2-s_2.12") \
    .getOrCreate()

# Step 4: Create Vertices DataFrame
vertices = spark.createDataFrame([
    ("Alice", 45),
    ("Jacob", 43),
    ("Roy", 21),
    ("Ryan", 49),
    ("Emily", 24),
    ("Sheldon", 52)
], ["id", "age"])

# Step 5: Create Edges DataFrame
edges = spark.createDataFrame([
    ("Sheldon", "Alice", "Sister"),
    ("Alice", "Jacob", "Husband"),
    ("Emily", "Jacob", "Father"),
    ("Ryan", "Alice", "Friend"),
    ("Alice", "Emily", "Daughter"),
    ("Jacob", "Roy", "Son"),
    ("Roy", "Ryan", "Son")
], ["src", "dst", "relation"])

# Step 6: Create a GraphFrame
graph = GraphFrame(vertices, edges)

# Step 7: Graph Analytics
print("Grouped and ordered edges:")
graph.edges.groupBy("src", "dst").count().orderBy(desc("count")).show()

print("Filtered edges where src or dst is 'Alice':")
graph.edges.where("src = 'Alice' OR dst = 'Alice'").groupBy("src", "dst").count().orderBy(desc("count")).show()

print("Subgraph where src or dst is 'Alice':")
subgraph_edges = graph.edges.where("src = 'Alice' OR dst = 'Alice'")
subgraph = GraphFrame(graph.vertices, subgraph_edges)
subgraph.edges.show()

print("Finding motifs in the graph:")
motifs = graph.find("(a) - [ab] -> (b)")
motifs.show()

print("Calculating PageRank:")
rank = graph.pageRank(resetProbability=0.15, maxIter=5)
rank.vertices.orderBy(desc("pagerank")).show()

print("In-Degree of nodes:")
in_degree = graph.inDegrees
in_degree.orderBy(desc("inDegree")).show()

print("Out-Degree of nodes:")
out_degree = graph.outDegrees
out_degree.orderBy(desc("outDegree")).show()

print("Finding connected components:")
spark.sparkContext.setCheckpointDir("/tmp/checkpoints")  # Required for connected components
cc = graph.connectedComponents()
cc.show()

print("Finding strongly connected components:")
scc = graph.stronglyConnectedComponents(maxIter=5)
scc.show()

print("Performing BFS from node 'Alice' to node 'Roy':")
bfs_result = graph.bfs(fromExpr="id = 'Alice'", toExpr="id = 'Roy'", maxPathLength=2)
bfs_result.show()

# Stop the Spark session
spark.stop()
