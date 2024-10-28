'''
บริษัทแห่งหนึ่งต้องการวิเคราะห์ข้อมูลการโพสต์บนแพลตฟอร์ม Facebook Live 
เพื่อทำความเข้าใจปัจจัยที่ส่งผลต่อประเภทของโพสต์ 
โดยข้อมูลที่มีประกอบด้วยประเภทของโพสต์ (เช่น วิดีโอ, รูปภาพ), 
วันและเวลาที่โพสต์ถูกเผยแพร่, จำนวนการตอบสนอง (reactions), 
และจำนวนการแสดงความรู้สึกต่างๆ เช่น ความชอบ ความรัก และความเห็นอื่นๆ 
ข้อมูลนี้จะถูกนำมาใช้เพื่อสร้างโมเดลการจำแนกประเภทโพสต์ โดยใช้ 
Decision Tree Classifier เพื่อคาดการณ์ว่าประเภทโพสต์ใดจะได้รับการตอบรับ
ที่ดีที่สุดจากผู้ใช้ในอนาคต โดยเฉพาะอย่างยิ่งในการวิเคราะห์ปฏิสัมพันธ์และการมีส่วน
ร่วมของผู้ใช้กับโพสต์ในแต่ละประเภท
'''
from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler, OneHotEncoder
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline

spark = SparkSession.builder.appName("DecisionTreeClassificationExample").getOrCreate()
df = spark.read.csv("./Classification/fb_live_thailand.csv", \
                    header=True, inferSchema=True)

df.printSchema()
status_type_indexer = StringIndexer(inputCol="status_type",outputCol="status_type_ind")

df_indexed = status_type_indexer.fit(df).transform(df)
df_indexed.groupBy("status_type_ind").count().show()
 
encoder = OneHotEncoder(inputCols=["status_type_ind"],\
                        outputCols=["status_type_vec"])

assembler = VectorAssembler(inputCols=["num_reactions", "num_comments", "num_likes", \
                                       "num_shares", "num_wows"], outputCol="features")

dt = DecisionTreeClassifier(labelCol="status_type_ind", featuresCol="features")

pipeline = Pipeline(stages=[encoder, assembler, dt])

train_data, test_data = df_indexed.randomSplit([0.7, 0.3], seed=42)
pipeline_model = pipeline.fit(train_data)
predictions = pipeline_model.transform(test_data)
predictions.select("status_type_ind", "prediction").show(5)

evaluator = MulticlassClassificationEvaluator(labelCol="status_type_ind", predictionCol="prediction")

accuracy = evaluator.evaluate(predictions, {evaluator.metricName: "accuracy"})
precision = evaluator.evaluate(predictions, {evaluator.metricName: "weightedPrecision"})
recall = evaluator.evaluate(predictions, {evaluator.metricName: "weightedRecall"})
f1 = evaluator.evaluate(predictions, {evaluator.metricName: "f1"})

print(f"Accuracy: {accuracy}")
print(f"Precision: {precision}")
print(f"Recall: {recall}")
print(f"F1 Score: {f1}")
test_error = 1.0 - accuracy
print(f"Test Error: {test_error}")

# confusion_matrix
# confusion_matrix = predictions.groupBy("status_type_ind", "prediction").count()
# confusion_matrix.show()
spark.stop()