from pyspark.sql import SparkSession
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS

spark = SparkSession.builder.appName('BookRecommendationSystem').getOrCreate()

df = spark.read.csv("./RecommendationSystem/book_ratings.csv", header=True, inferSchema=True)

df.printSchema()

als = ALS(
    maxIter=10,
    regParam=0.1,
    userCol="user_id",
    itemCol="book_id",
    ratingCol="rating",
    coldStartStrategy="drop"
)

model = als.fit(df)

evaluator = RegressionEvaluator(
    metricName="rmse",
    labelCol="rating",
    predictionCol="prediction"
)

train, test = df.randomSplit([0.8, 0.2])
predictions = model.transform(test)

rmse = evaluator.evaluate(predictions)
print(f"Root Mean Square Error (RMSE): {rmse}")

user53_df = df.filter(df['user_id'] == 53)
user53_predictions = model.transform(user53_df)
user53_predictions.orderBy("prediction", ascending=False).show(truncate=False)

user_recommendations = model.recommendForAllUsers(5)
user_recommendations.show(truncate=False)

book_recommendations = model.recommendForAllItems(5)
book_recommendations.show(truncate=False)