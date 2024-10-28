from pyspark.sql import SparkSession
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import functions as F
from pyspark.sql.functions import col,format_number


spark = SparkSession.builder.appName('BookRecommendationSystem').getOrCreate()

# โหลดข้อมูล
df = spark.read.csv("./book_ratings.csv", header=True, inferSchema=True)

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

evaluator_mae = RegressionEvaluator(
    metricName="mae",
    labelCol="rating",
    predictionCol="prediction"
)
mae = evaluator_mae.evaluate(predictions)
print(f"Mean Absolute Error (MAE): {mae}")

confusion_matrix = predictions.groupBy('rating', 'prediction').count()
confusion_matrix = predictions.select('rating', format_number(col("prediction"),2))
print("Confusion Matrix:")
confusion_matrix.show()
# แสดง confusion matrix ด้วยการนับ
print("Detailed Confusion Matrix:")
confusion_matrix.agg(F.count('*').alias('count')).show()

# เพิ่มส่วนนี้เพื่อแสดงรายการที่มี rating 5
rating_5_books = df.filter(df['rating'] == 5).select("book_id").distinct().limit(5)
print("Books with Rating 5:")
rating_5_books.show(truncate=False)

######### Union ########################
# หนังสือที่มี rating 3
rating_3_books = df.filter(df['rating'] == 3).select("book_id").distinct().withColumn("rating_value", F.lit(3)).limit(5)
# หนังสือที่มี rating 4
rating_4_books = df.filter(df['rating'] == 4).select("book_id").distinct().withColumn("rating_value", F.lit(4)).limit(5)
combined_books = rating_3_books.union(rating_4_books)
result = combined_books.limit(10)
print("Books with Rating 3 and 4:")
result.show(truncate=False)
