from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

spark = SparkSession.builder \
    .appName("LinearRegressionPipelineExample") \
    .getOrCreate()

data = spark.read.csv('./RegressionandDecision/fb_live_thailand.csv', header=True, inferSchema=True)

indexer_reactions = StringIndexer(inputCol="num_reactions", outputCol="num_reactions_ind")
indexer_loves = StringIndexer(inputCol="num_loves", outputCol="num_loves_ind")

assembler = VectorAssembler(
    inputCols=["num_reactions_ind", "num_loves_ind"],
    outputCol="features"
)

lr = LinearRegression(
    featuresCol="features",
    labelCol="num_loves_ind",
    maxIter=10,
    regParam=0.1,
    elasticNetParam=0.8
)

pipeline = Pipeline(stages=[assembler, lr])

train_data, test_data = data.randomSplit([0.8, 0.2], seed=1234)

train_data = indexer_reactions.fit(train_data).transform(train_data)
train_data = indexer_loves.fit(train_data).transform(train_data)
pipeline_model = pipeline.fit(train_data)

test_data = indexer_reactions.fit(test_data).transform(test_data)
test_data = indexer_loves.fit(test_data).transform(test_data)
predictions = pipeline_model.transform(test_data)

evaluator = RegressionEvaluator(
    labelCol="num_loves_ind",
    predictionCol="prediction"
)

evaluator.setMetricName("mse")
mse = evaluator.evaluate(predictions)
print(f"Mean Squared Error (MSE): {mse}")

evaluator.setMetricName("r2")
r2 = evaluator.evaluate(predictions)
print(f"R-squared (R2): {r2}")

selected_data = predictions.select(
    col("num_loves_ind").cast(IntegerType()).alias("num_loves"),
    col("prediction").cast(IntegerType()).alias("prediction")
).orderBy(col("prediction").desc())

selected_data_pd = selected_data.toPandas()

plt.figure(figsize=(12, 6))
sns.lmplot(
    data=selected_data_pd,
    x='num_loves',
    y='prediction',
    aspect=1.5,
    scatter_kws={'s': 50, 'alpha': 0.5},
    line_kws={'color': 'red'}
)

plt.title('Linear Regression: Actual vs Predicted Values')
plt.xlabel('Actual num_loves')
plt.ylabel('Predicted num_loves')
plt.show()

spark.stop()

'''
โค้ดนี้ใช้ PySpark เพื่อสร้างโมเดลการถดถอยเชิงเส้น (Linear Regression) สำหรับพยากรณ์จำนวนการตอบสนองของโพสต์ Facebook
'''
