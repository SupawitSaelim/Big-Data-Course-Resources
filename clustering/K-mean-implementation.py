from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import ClusteringEvaluator
import matplotlib.pyplot as plt
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("KMeans Clustering") \
    .getOrCreate()

df = spark.read.csv("./fb_live_thailand.csv",\
                    header=True, inferSchema=True)
df = df.select(df.num_sads.cast(DoubleType()),df.num_reactions.cast(DoubleType()))

vec_assembler = VectorAssembler(inputCols=["num_sads", "num_reactions"], outputCol="features")

# Scaling for making columns comparable
scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures", withStd=True, withMean=False)

# Initialize k values list
k_values = []

# Loop for finding the optimal k in range 2 to 5
for i in range(2, 7):
    kmeans = KMeans(featuresCol="scaledFeatures", predictionCol="prediction_col", k=i)
    pipeline = Pipeline(stages=[vec_assembler, scaler, kmeans])
    model = pipeline.fit(df)
    output = model.transform(df)
    evaluator = ClusteringEvaluator(predictionCol="prediction_col", \
                                    featuresCol="scaledFeatures", \
                                    metricName="silhouette", \
                                    distanceMeasure="squaredEuclidean")
    score = evaluator.evaluate(output)
    k_values.append(score)
    print("Silhouette Score for k =", i, ":", score)

# Get the best k
best_k = k_values.index(max(k_values)) + 2
print("The best k:", best_k, "with Silhouette Score:", max(k_values))

# Initialize KMeans with the best k
kmeans = KMeans(featuresCol="scaledFeatures", predictionCol="prediction_col", k=best_k)

# Create pipeline
pipeline = Pipeline(stages=[vec_assembler, scaler, kmeans])

# Fit the model
model = pipeline.fit(df)

# Prediction
predictions = model.transform(df)

# Evaluate
evaluator = ClusteringEvaluator(predictionCol="prediction_col", \
                                featuresCol="scaledFeatures", \
                                metricName="silhouette", \
                                distanceMeasure="squaredEuclidean")
silhouette = evaluator.evaluate(predictions)
print("Silhouette with squared euclidean distance =", str(silhouette))

# Converting to Pandas DataFrame
clustered_data_pd = predictions.toPandas()

# Visualizing the results
# plt.scatter(clustered_data_pd["num_reactions"], \
#             clustered_data_pd["num_sads"], \
#             c = clustered_data_pd["prediction_col"])
# plt.xlabel("num_reactions")
# plt.ylabel("num_sads")
# plt.title("K-means Clustering")
# plt.colorbar().set_label("Cluster")
# plt.show()

# Count the number of data points in each cluster
cluster_counts = clustered_data_pd["prediction_col"].value_counts().sort_index()
# Plotting the bar chart
plt.bar(cluster_counts.index, cluster_counts.values, color='skyblue')
plt.xlabel("Cluster")
plt.ylabel("Number of Points")
plt.title("Number of Points per Cluster")
plt.xticks(cluster_counts.index)  # To ensure the x-ticks match the cluster numbers
plt.show()

