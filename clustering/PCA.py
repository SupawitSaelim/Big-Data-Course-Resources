from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StandardScaler, PCA
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import ClusteringEvaluator
import matplotlib.pyplot as plt
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("PCA KMeans Clustering") \
    .getOrCreate()

df = spark.read.csv("./Clustering/fb_live_thailand.csv", header=True, inferSchema=True)
df = df.select(df.num_sads.cast(DoubleType()), df.num_reactions.cast(DoubleType()))

vec_assembler = VectorAssembler(inputCols=["num_sads", "num_reactions"], outputCol="features")

scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures", withStd=True, withMean=False)

# PCA
pca = PCA(k=1, inputCol="scaledFeatures", outputCol="pcaFeatures")  # ลดมิติเป็น 1 สำหรับการใช้งาน KMeans

k_values = []

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

best_k = k_values.index(max(k_values)) + 2
print("The best k:", best_k, "with Silhouette Score:", max(k_values))

kmeans = KMeans(featuresCol="pcaFeatures", predictionCol="prediction_col", k=best_k)

pipeline = Pipeline(stages=[vec_assembler, scaler, pca, kmeans])

model = pipeline.fit(df)

predictions = model.transform(df)

evaluator = ClusteringEvaluator(predictionCol="prediction_col", \
                                featuresCol="pcaFeatures", \
                                metricName="silhouette", \
                                distanceMeasure="squaredEuclidean")
silhouette = evaluator.evaluate(predictions)
print("Silhouette with squared euclidean distance =", str(silhouette))

clustered_data_pd = predictions.toPandas()

plt.scatter(clustered_data_pd["num_reactions"], \
            clustered_data_pd["num_sads"], \
            c=clustered_data_pd["prediction_col"])
plt.xlabel("num_reactions")
plt.ylabel("num_sads")
plt.title("PCA K-means Clustering")
plt.colorbar().set_label("Cluster")
plt.show()
