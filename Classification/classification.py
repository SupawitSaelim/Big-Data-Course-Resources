from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline

spark = SparkSession.builder.appName("LogisticRegressionFBLive").getOrCreate()

data = spark.read.csv("fb_live_thailand.csv", header=True, inferSchema=True)

indexer_status_type = StringIndexer(inputCol="status_type", outputCol="status_type_ind")
indexer_status_published = StringIndexer(inputCol="status_published", outputCol="status_published_ind")

indexed_data = indexer_status_type.fit(data).transform(data)
indexed_data = indexer_status_published.fit(indexed_data).transform(indexed_data)

assembler = VectorAssembler(inputCols=["status_type_ind", "status_published_ind"], outputCol="features")

lr = LogisticRegression(featuresCol="features", labelCol="status_type_ind", maxIter=10, regParam=0.3, elasticNetParam=0.8)

pipeline = Pipeline(stages=[assembler, lr])

train_data, test_data = indexed_data.randomSplit([0.8, 0.2], seed=1234)

lr_model = pipeline.fit(train_data)

predictions = lr_model.transform(test_data)

predictions.select("status_type_ind", "prediction").show(5)

evaluator = MulticlassClassificationEvaluator(labelCol="status_type_ind", predictionCol="prediction")

accuracy = evaluator.setMetricName("accuracy").evaluate(predictions)
print(f"Accuracy: {accuracy}")

precision = evaluator.setMetricName("weightedPrecision").evaluate(predictions)
print(f"Weighted Precision: {precision}")

recall = evaluator.setMetricName("weightedRecall").evaluate(predictions)
print(f"Weighted Recall: {recall}")

f1 = evaluator.setMetricName("f1").evaluate(predictions)
print(f"F1 Score: {f1}")
