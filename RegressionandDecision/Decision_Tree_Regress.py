from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler, OneHotEncoder
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
from pyspark.sql.types import IntegerType
import seaborn as sns
import matplotlib.pyplot as plt

spark = SparkSession.builder.appName("DecisionTreeRegressionFBLive").getOrCreate()
data = spark.read.csv("./RegressionandDecision/fb_live_thailand.csv", header=True, inferSchema=True)

indexer_reactions = StringIndexer(inputCol="num_reactions", outputCol="num_reactions_ind")
indexer_loves = StringIndexer(inputCol="num_loves", outputCol="num_loves_ind")

indexed_data = indexer_reactions.fit(data).transform(data)
indexed_data = indexer_loves.fit(indexed_data).transform(indexed_data)

encoder_reactions = OneHotEncoder(inputCol="num_reactions_ind", outputCol="num_reactions_encoded")
encoder_loves = OneHotEncoder(inputCol="num_loves_ind", outputCol="num_loves_encoded")

assembler = VectorAssembler(inputCols=["num_reactions_encoded", "num_loves_encoded"], outputCol="features")
dt = DecisionTreeRegressor(featuresCol="features", labelCol="num_loves_ind")
pipeline = Pipeline(stages=[encoder_reactions, encoder_loves, assembler, dt])

train_data, test_data = indexed_data.randomSplit([0.8, 0.2], seed=1234)
dt_model = pipeline.fit(train_data)

predictions = dt_model.transform(test_data)
predictions.select("num_loves_ind", "prediction").show(5)
evaluator = RegressionEvaluator(labelCol="num_loves_ind", predictionCol="prediction")

r2 = evaluator.setMetricName("r2").evaluate(predictions)
print(f"R2 Score: {r2}")

selected_data = predictions.select("num_loves_ind", "prediction").toPandas()

sns.lmplot(x="num_loves_ind", y="prediction", data=selected_data)
plt.title("Decision Tree Regression: num_loves_ind vs Prediction")
plt.xlabel("Actual num_loves_ind")
plt.ylabel("Predicted num_loves_ind")
plt.show()