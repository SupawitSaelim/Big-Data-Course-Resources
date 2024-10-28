from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import trim, col, size, explode, desc, count, when, collect_list, avg
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# สร้าง Spark Session
spark = SparkSession.builder \
    .appName("Text Analysis Implementation") \
    .getOrCreate()

# อ่านข้อมูลจากไฟล์ CSV
data = spark.read.csv("./reviews_rated.csv", header=True, inferSchema=True)

# กรองข้อมูลและปรับคอลัมน์
data = data.select(trim(data['Review Text']).alias('ReviewText'),
                   data['Rating'].cast(IntegerType()).alias('Rating')) \
           .filter((col("ReviewText").isNotNull()) & (col("Rating").isNotNull()))

# แสดงข้อมูล 5 แถวแรก
print("Initial Data Sample:")
data.show(5)

# สร้าง Tokenizer, StopWordsRemover, HashingTF
tokenizer = Tokenizer(inputCol="ReviewText", outputCol="ReviewTextWords")
stop_word_remover = StopWordsRemover(inputCol=tokenizer.getOutputCol(), outputCol="MeaningfulWords")
hashing_tf = HashingTF(inputCol=stop_word_remover.getOutputCol(), outputCol="features")

# สร้าง Pipeline
pipeline = Pipeline(stages=[tokenizer, stop_word_remover, hashing_tf])

# แบ่งข้อมูลเป็นชุดฝึกสอนและชุดทดสอบ
train_data, test_data = data.randomSplit([0.8, 0.2], seed=123)

# แสดงข้อมูลชุดฝึกสอน 5 แถวแรก
print("Training Data Sample:")
train_data.show(5)

# สร้างและฝึกโมเดล Pipeline
pipeline_model = pipeline.fit(train_data)

# แปลงข้อมูล
train_transformed = pipeline_model.transform(train_data)
test_transformed = pipeline_model.transform(test_data)

# แสดงข้อมูลชุดที่แปลงแล้ว
print("Transformed Training Data Sample:")
train_transformed.show(5)

# สร้างโมเดล Logistic Regression
lr = LogisticRegression(labelCol="Rating", featuresCol="features")
lr_model = lr.fit(train_transformed)

# ทำนายข้อมูลชุดทดสอบ
predictions = lr_model.transform(test_transformed)

# แสดงการทำนายผล
print("Sample Predictions:")
predictions.select("MeaningfulWords", "Rating", "prediction").show(5)

# ประเมินโมเดล
evaluator = MulticlassClassificationEvaluator(labelCol="Rating", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print(f"Test Accuracy = {accuracy}")

# 1. นับจำนวนคำในแต่ละรีวิวจาก DataFrame ที่ถูกแปลง
print("Count of Words in Each Review:")
data_with_word_count = train_transformed.withColumn("WordCount", size(col("MeaningfulWords")))  # ใช้ MeaningfulWords แทน ReviewTextWords
data_with_word_count.select("ReviewText", "WordCount").show(5)

# 2. สถิติเกี่ยวกับเรตติ้ง
print("Rating Statistics:")
rating_stats = data.select("Rating").describe()  # คำนวณสถิติเกี่ยวกับเรตติ้ง
rating_stats.show()

# 3. แสดงตัวอย่างรีวิวที่มีการทำนายผิดพลาด
print("Incorrect Predictions:")
incorrect_predictions = predictions.filter(col("Rating") != col("prediction"))
incorrect_predictions.select("ReviewText", "Rating", "prediction").show(5)

# 4. แสดงค่าสถิติของคำที่มีความถี่สูงที่สุดในรีวิว
print("Most Frequent Words in Reviews:")
word_frequencies = train_transformed.select(explode("MeaningfulWords").alias("Word")) \
                                     .groupBy("Word").count() \
                                     .orderBy(desc("count"))
word_frequencies.show(10)

# 5. แสดงการกระจายของเรตติ้ง
print("Distribution of Ratings:")
rating_distribution = data.groupBy("Rating").count().orderBy("Rating")
rating_distribution.show()

# 6. แสดงค่าสถิติเกี่ยวกับความแม่นยำในการทำนายตามเรตติ้ง
print("Accuracy by Rating:")
accuracy_by_rating = predictions.groupBy("Rating").agg(
    (count(when(col("Rating") == col("prediction"), 1)) / count("Rating")).alias("Accuracy")
)
accuracy_by_rating.show()

# 7. คำที่มีความถี่สูงในรีวิวที่มีเรตติ้ง 5
print("Most Frequent Words for Rating 5:")
most_frequent_rating_5 = train_transformed.filter(col("Rating") == 5) \
                                           .select(explode("MeaningfulWords").alias("Word")) \
                                           .groupBy("Word").count() \
                                           .orderBy(desc("count"))
most_frequent_rating_5.show(10)

# 10. ความยาวเฉลี่ยของรีวิว
print("Average Length of Reviews:")
average_length = data_with_word_count.agg(avg('WordCount')).collect()[0][0]
print(f"Average length of reviews (in words): {average_length}")

# 11. จำนวนรีวิวต่อเรตติ้ง
print("Count of Reviews per Rating:")
count_reviews_per_rating = data.groupBy("Rating").count().orderBy("Rating")
count_reviews_per_rating.show()

# 12. คำที่มีความถี่สูงในรีวิวที่มีเรตติ้ง 2
print("Most Frequent Words for Rating 2:")
most_frequent_rating_2 = train_transformed.filter(col("Rating") == 2) \
                                           .select(explode("MeaningfulWords").alias("Word")) \
                                           .groupBy("Word").count() \
                                           .orderBy(desc("count"))
most_frequent_rating_2.show(10)

# 16. การกระจายของจำนวนคำในรีวิวแต่ละเรตติ้ง
print("Word Count Distribution by Rating:")
word_count_distribution = data_with_word_count.groupBy("WordCount").count().orderBy("WordCount")
word_count_distribution.show()

# 17. จำนวนรีวิวที่ไม่ซ้ำกันตามเรตติ้ง
print("Unique Review Count per Rating:")
unique_review_count_per_rating = data.select("Rating").distinct().groupBy("Rating").count()
unique_review_count_per_rating.show()

# 18. ความสัมพันธ์ระหว่างความยาวของรีวิวกับเรตติ้ง
print("Length of Reviews vs. Rating:")
length_vs_rating = data_with_word_count.select("WordCount", "Rating").groupBy("WordCount", "Rating").count().orderBy(desc("WordCount"))
length_vs_rating.show()

# 19. คำที่มีความถี่สูงในรีวิวที่ไม่ถูกต้อง
print("Most Frequent Words in Incorrect Predictions:")
incorrect_word_frequencies = incorrect_predictions.select(explode("MeaningfulWords").alias("Word")) \
                                                .groupBy("Word").count() \
                                                .orderBy(desc("count"))
incorrect_word_frequencies.show(10)


spark.stop()
