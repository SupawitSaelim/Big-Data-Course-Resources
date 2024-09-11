# Concatenate input columns to the output "features"
vec_assembler = VectorAssembler(inputCols=["num_sads", "num_reactions"], outputCol="features")

# Scaling for making columns comparable
scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures", withStd=True, withMean=False)

# Initialize k values list
k_values = []

# Loop for finding the optimal k in range 2 to 5
for i in range(2, 5):
    kmeans = KMeans(featuresCol="scaledFeatures", predictionCol="prediction_col", k=i)
    pipeline = Pipeline(stages=[vec_assembler, scaler, kmeans])
    model = pipeline.fit(df)
    output = model.transform(df)
    evaluator = ClusteringEvaluator(predictionCol="prediction_col", featuresCol="scaledFeatures", metricName="silhouette", distanceMeasure="squaredEuclidean")
    score = evaluator.evaluate(output)
    k_values.append(score)
    print("Silhouette Score:", score)

# Get the best k
best_k = k_values.index(max(k_values)) + 2
print("The best k:", best_k, max(k_values))

# Concatenate input columns to the output "features"
vec_assembler = VectorAssembler(inputCols=["num_sads", "num_reactions"], outputCol="features")

# Scaling for making columns comparable
scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures", withStd=True, withMean=False)

# Initialize k values list
k_values = []

# Loop for finding the optimal k in range 2 to 5
for i in range(2, 5):
    kmeans = KMeans(featuresCol="scaledFeatures", predictionCol="prediction_col", k=i)
    pipeline = Pipeline(stages=[vec_assembler, scaler, kmeans])

    # Fit the model and transform the data
    model = pipeline.fit(df)
    output = model.transform(df)

    # Evaluate the clustering using Silhouette score
    evaluator = ClusteringEvaluator(predictionCol="prediction_col", featuresCol="scaledFeatures", metricName="silhouette", distanceMeasure="squaredEuclidean")
    score = evaluator.evaluate(output)
    k_values.append(score)
    print("Silhouette Score:", score)

# Get the best k
best_k = k_values.index(max(k_values)) + 2
print("The best k:", best_k, max(k_values))

# Initialize KMeans with the best k
kmeans = KMeans(featuresCol="scaledFeatures", predictionCol="prediction_col", k=best_k)

# Create pipeline
pipeline = Pipeline(stages=[vec_assembler, scaler, kmeans])

# Fit the model
model = pipeline.fit(df)

# Prediction
predictions = model.transform(df)

# Evaluate
evaluator = ClusteringEvaluator(predictionCol="prediction_col", featuresCol="scaledFeatures", metricName="silhouette", distanceMeasure="squaredEuclidean")
silhouette = evaluator.evaluate(predictions)
print("Silhouette with squared euclidean distance =", str(silhouette))

# Converting to Pandas DataFrame
clustered_data_pd = predictions.toPandas()

# Visualizing the results
plt.scatter(clustered_data_pd["num_reactions"], clustered_data_pd["num_sads"], c=clustered_data_pd["prediction_col"])
plt.xlabel("num_reactions")
plt.ylabel("num_sads")
plt.title("K-means Clustering")
plt.colorbar().set_label("Cluster")
plt.show()