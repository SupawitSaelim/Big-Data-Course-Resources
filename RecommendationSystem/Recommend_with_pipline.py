from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS

spark = SparkSession.builder.appName('BookRecommendationSystem').getOrCreate()

df = spark.read.csv("./RecommendationSystem/book_ratings.csv", header=True, inferSchema=True)
df.printSchema()

train, test = df.randomSplit([0.8, 0.2])

als = ALS(
    maxIter=10,
    regParam=0.1,
    userCol="user_id",
    itemCol="book_id",
    ratingCol="rating",
    coldStartStrategy="drop"
)

pipeline = Pipeline(stages=[als])

model = pipeline.fit(train)

predictions = model.transform(test)

evaluator = RegressionEvaluator(
    metricName="rmse",
    labelCol="rating",
    predictionCol="prediction"
)

rmse = evaluator.evaluate(predictions)
print(f"Root Mean Square Error (RMSE): {rmse}")

user53_df = df.filter(df['user_id'] == 53)
user53_predictions = model.transform(user53_df)
user53_predictions.orderBy("prediction", ascending=False).show(truncate=False)

user_recommendations = model.stages[0].recommendForAllUsers(5)  
user_recommendations.show(truncate=False)

book_recommendations = model.stages[0].recommendForAllItems(5) 
book_recommendations.show(truncate=False)

spark.stop()
