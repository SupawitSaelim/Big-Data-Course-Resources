'''
โจทย์ Clustering แบบใช้ PCA ,K-mean แล้วพลอตทั้งสอง cluster
Using Facebook post interaction data, perform clustering with K-means on the features
"num_reactions," "num_comments," "num_shares," "num_likes," "num_loves," and "num_angrys" 
to categorize posts based on user engagement patterns. Apply PCA for dimensionality reduction, 
if necessary, to make the clustering process more efficient. Identify the optimal number of
clusters (k) by maximizing the Silhouette Score. Conduct time-based clustering analyses (e.g., 
monthly or quarterly) to examine any variations in engagement patterns over time. Additionally, 
analyze the relationship between clusters and post types (e.g., photo, video). Lastly, display
posts within each cluster that have the highest and lowest engagement levels across the selected features
'''
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StandardScaler, PCA
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import ClusteringEvaluator
import matplotlib.pyplot as plt
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("KMeans Clustering") \
    .getOrCreate()

df = spark.read.csv("./Practices/Clustering/fb_live_thailand.csv",\
                    header=True, inferSchema=True)
df = df.select(df.num_comments.cast(DoubleType()), 
               df.num_reactions.cast(DoubleType()), 
               df.num_shares.cast(DoubleType()),
               df.num_likes.cast(DoubleType()),
               df.num_loves.cast(DoubleType()),
               df.num_angrys.cast(DoubleType()),
              )

vec_assembler = VectorAssembler(inputCols=["num_comments", "num_reactions", \
                                           "num_shares", "num_likes", "num_loves", \
                                           "num_angrys"], outputCol="features")

# Scaling for making columns comparable
scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures", withStd=True, withMean=False)

pca = PCA(k=2, inputCol="scaledFeatures", outputCol="pcaFeatures")

# Initialize k values list
k_values = []

# Loop for finding the optimal k in range 2 to 5
for i in range(2, 7):
    kmeans = KMeans(featuresCol="pcaFeatures", predictionCol="prediction_col", k=i)
    pipeline = Pipeline(stages=[vec_assembler, scaler, pca, kmeans])
    model = pipeline.fit(df)
    output = model.transform(df)
    evaluator = ClusteringEvaluator(predictionCol="prediction_col", \
                                    featuresCol="pcaFeatures", \
                                    metricName="silhouette", \
                                    distanceMeasure="squaredEuclidean")
    score = evaluator.evaluate(output)
    k_values.append(score)
    print("Silhouette Score for k =", i, ":", score)

# Get the best k
best_k = k_values.index(max(k_values)) + 2
print("The best k:", best_k, "with Silhouette Score:", max(k_values))

# Initialize KMeans with the best k
kmeans = KMeans(featuresCol="pcaFeatures", predictionCol="prediction_col", k=best_k)

# Create pipeline
pipeline = Pipeline(stages=[vec_assembler, scaler, pca,  kmeans])

# Fit the model
model = pipeline.fit(df)

# Prediction
predictions = model.transform(df)

# Evaluate
evaluator = ClusteringEvaluator(predictionCol="prediction_col", \
                                featuresCol="pcaFeatures", \
                                metricName="silhouette", \
                                distanceMeasure="squaredEuclidean")
silhouette = evaluator.evaluate(predictions)
print("Silhouette with squared euclidean distance =", str(silhouette))

# Converting to Pandas DataFrame
clustered_data_pd = predictions.toPandas()

# Visualizing the results
plt.scatter(clustered_data_pd['pcaFeatures'].apply(lambda x: x[0]), 
            clustered_data_pd['pcaFeatures'].apply(lambda x: x[1]),
            c=clustered_data_pd["prediction_col"])
plt.xlabel("PCA Feature 1")
plt.ylabel("PCA Feature 2")
plt.title("K-means Clustering with PCA")
plt.colorbar().set_label("Cluster")
plt.show()