from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import trim, col
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# สร้าง SparkSession สำหรับการทำงานกับ Apache Spark
spark = SparkSession.builder \
    .appName("Text Analysis Implementation") \
    .getOrCreate()

# อ่านข้อมูลจากไฟล์ CSV และกำหนด schema
data = spark.read.csv("./reviews_rated.csv", header=True, inferSchema=True)

# เลือกคอลัมน์ที่ต้องการและทำการ trim ค่าที่ว่างเปล่า
data = data.select(trim(data['Review Text']).alias('ReviewText'),
                   data['Rating'].cast(IntegerType()).alias('Rating'))

# กรองข้อมูลที่มีค่า ReviewText และ Rating ไม่เป็น null
data = data.filter((col("ReviewText").isNotNull()) & (col("Rating").isNotNull()))

# แสดงผล 5 แถวแรกของข้อมูล
data.show(5)

# ขั้นตอนการแปลงข้อมูลข้อความ
tokenizer = Tokenizer(inputCol="ReviewText", outputCol="ReviewTextWords")
stop_word_remover = StopWordsRemover(inputCol=tokenizer.getOutputCol(), outputCol="MeaningfulWords")
hashing_tf = HashingTF(inputCol=stop_word_remover.getOutputCol(), outputCol="features")

# สร้าง Pipeline เพื่อรวมขั้นตอนการประมวลผล
pipeline = Pipeline(stages=[tokenizer, stop_word_remover, hashing_tf])

# แบ่งข้อมูลเป็นชุดการฝึกและชุดทดสอบ
train_data, test_data = data.randomSplit([0.8, 0.2], seed=123)

# แสดงผล 5 แถวแรกของชุดการฝึก
train_data.show(5)

# ฟิต Pipeline กับข้อมูลการฝึก
pipeline_model = pipeline.fit(train_data)

# แปลงข้อมูลการฝึกและข้อมูลทดสอบ
train_transformed = pipeline_model.transform(train_data)
test_transformed = pipeline_model.transform(test_data)

# แสดงผล 5 แถวแรกของข้อมูลที่แปลงแล้ว
train_transformed.show(5)

# สร้างโมเดล Logistic Regression
lr = LogisticRegression(labelCol="Rating", featuresCol="features")

# ฟิตโมเดลกับข้อมูลที่แปลงแล้ว
lr_model = lr.fit(train_transformed)

# ทำการทำนายกับข้อมูลทดสอบ
predictions = lr_model.transform(test_transformed)

# แสดงผลการทำนาย
predictions.select("MeaningfulWords", "Rating", "prediction").show(5)

# ประเมินความถูกต้องของโมเดล
evaluator = MulticlassClassificationEvaluator(labelCol="Rating", predictionCol="prediction", metricName="accuracy")

# คำนวณและแสดงความแม่นยำ
accuracy = evaluator.evaluate(predictions)
print(f"Test Accuracy = {accuracy}")

'''
โค้ดนี้ทำการวิเคราะห์และจำแนกประเภทความคิดเห็น (reviews) โดยใช้วิธีการทำงานของโมเดล Machine Learning
โดยเฉพาะ Logistic Regression พร้อมทั้งมีการแปลงและเตรียมข้อมูลก่อนการฝึกโมเดล โดยมีการใช้ Pipeline 
เพื่อจัดการกับขั้นตอนต่างๆ ในการประมวลผลข้อมูล
'''