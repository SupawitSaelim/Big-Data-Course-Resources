##### เหมือนโค้ดต้นฉบับ แต่ใช้หลาย feature ในการเทรน และทดสอบ ##########################
from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline

# สร้าง SparkSession
spark = SparkSession.builder.appName("LogisticRegressionFBLive").getOrCreate()

# อ่านข้อมูล
data = spark.read.csv("./Practices/Classification/fb_live_thailand.csv", header=True, inferSchema=True)

# ขั้นตอน 1: การแปลงข้อมูล
indexer_status_type = StringIndexer(inputCol="status_type", outputCol="status_type_ind")

indexed_data = indexer_status_type.fit(data).transform(data)

# ขั้นตอน 2: สร้าง VectorAssembler
assembler = VectorAssembler(inputCols=["num_reactions", "num_comments", "num_likes", "num_shares", "num_wows"], outputCol="features")

# สร้างโมเดล Logistic Regression
lr = LogisticRegression(featuresCol="features", labelCol="status_type_ind", maxIter=10, regParam=0.3, elasticNetParam=0.8)

# สร้าง Pipeline
pipeline = Pipeline(stages=[assembler, lr])

# แบ่งข้อมูลเป็น train และ test
train_data, test_data = indexed_data.randomSplit([0.8, 0.2], seed=1234)

# ฝึกโมเดล
lr_model = pipeline.fit(train_data)

# ทำการพยากรณ์
predictions = lr_model.transform(test_data)

# ขั้นตอน 3: การประเมินผล
evaluator = MulticlassClassificationEvaluator(labelCol="status_type_ind", predictionCol="prediction")

accuracy = evaluator.setMetricName("accuracy").evaluate(predictions)
print(f"Accuracy: {accuracy}")

precision = evaluator.setMetricName("weightedPrecision").evaluate(predictions)
print(f"Weighted Precision: {precision}")

recall = evaluator.setMetricName("weightedRecall").evaluate(predictions)
print(f"Weighted Recall: {recall}")

f1 = evaluator.setMetricName("f1").evaluate(predictions)
print(f"F1 Score: {f1}")

# หยุด SparkSession
spark.stop()
