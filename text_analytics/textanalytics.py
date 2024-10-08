from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import trim, col
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

spark = SparkSession.builder \
    .appName("Text Analysis Implementation") \
    .getOrCreate()

data = spark.read.csv("./text_analytics/reviews_rated.csv", header=True, inferSchema=True)

data = data.select(trim(data['Review Text']).alias('ReviewText'),
                   data['Rating'].cast(IntegerType()).alias('Rating'))

data = data.filter((col("ReviewText").isNotNull()) & (col("Rating").isNotNull()))

data.show(5)

tokenizer = Tokenizer(inputCol="ReviewText", outputCol="ReviewTextWords")

stop_word_remover = StopWordsRemover(inputCol=tokenizer.getOutputCol(), outputCol="MeaningfulWords")

hashing_tf = HashingTF(inputCol=stop_word_remover.getOutputCol(), outputCol="features")

pipeline = Pipeline(stages=[tokenizer, stop_word_remover, hashing_tf])

train_data, test_data = data.randomSplit([0.8, 0.2], seed=123)

train_data.show(5)

pipeline_model = pipeline.fit(train_data)

train_transformed = pipeline_model.transform(train_data)
test_transformed = pipeline_model.transform(test_data)

train_transformed.show(5)

lr = LogisticRegression(labelCol="Rating", featuresCol="features")

lr_model = lr.fit(train_transformed)

predictions = lr_model.transform(test_transformed)

predictions.select("MeaningfulWords", "Rating", "prediction").show(5)

evaluator = MulticlassClassificationEvaluator(labelCol="Rating", predictionCol="prediction", metricName="accuracy")

accuracy = evaluator.evaluate(predictions)

print(f"Test Accuracy = {accuracy}")
