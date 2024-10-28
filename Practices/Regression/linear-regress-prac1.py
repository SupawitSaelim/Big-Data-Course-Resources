'''
บริษัทแห่งหนึ่งต้องการวิเคราะห์ข้อมูลเพื่อทำความเข้าใจปฏิสัมพันธ์ที่เกิดขึ้นกับโพสต์บนแพลตฟอร์มโซเชียลมีเดีย 
(Facebook) โดยมีข้อมูลต่างๆ เช่น จำนวนการแสดงความรู้สึกต่างๆ (reactions) ประเภทของโพสต์ (เช่น ภาพ วิดีโอ) 
และเวลาที่โพสต์ถูกเผยแพร่ ข้อมูลนี้จะถูกนำมาใช้เพื่อคาดการณ์การตอบรับของผู้ใช้ในอนาคตโดยเฉพาะในส่วนของ 
"num_loves" หรือจำนวนคนที่แสดงความรู้สึก "รัก"

จงใช้ข้อมูลที่ให้มาพร้อมกับโค้ด PySpark ที่เกี่ยวข้อง แล้วทำตามขั้นตอนดังนี้:

1.เพิ่มการวิเคราะห์ประเภทโพสต์ (status_type):

ให้เพิ่มการแปลงประเภทโพสต์ (status_type) เป็นตัวแปรเชิงตัวเลขเพื่อใช้ในการพยากรณ์ 
โดยใช้เทคนิคการจัดการข้อมูล (เช่น การเข้ารหัสแบบ StringIndexer) เพิ่มเป็นตัวแปรใหม่ 
และรวมเข้ากับตัวแปร "features" สำหรับใช้ในการพยากรณ์

2.สร้างแบบจำลองที่มีคุณภาพสูงขึ้น:
ใช้เทคนิคการพยากรณ์ที่แม่นยำมากขึ้น โดยการปรับพารามิเตอร์ของ LinearRegression ได้แก่ maxIter, 
regParam, และ elasticNetParam ให้เหมาะสม เพื่อให้ได้ค่า Mean Squared Error (MSE) 
และค่า R-squared (R2) ที่ดีขึ้น

3.วิเคราะห์การกระจายตัวของโพสต์ตามช่วงเวลา:
สร้างฟีเจอร์ใหม่โดยแปลงเวลาที่โพสต์ (status_published) ให้เป็นช่วงเวลา เช่น ช่วงเช้า 
กลางวัน เย็น และกลางคืน แล้วรวมเป็นตัวแปรฟีเจอร์ เพื่อพิจารณาว่าช่วงเวลาใดที่โพสต์มีแนวโน้มได้รับการตอบรับสูงสุด

4.สร้างกราฟที่แสดงการพยากรณ์แบบรายกลุ่ม:
สร้างกราฟการพยากรณ์ (Actual vs. Predicted) สำหรับแต่ละประเภทของโพสต์ เช่น วิดีโอ ภาพ 
และอื่น ๆ เพื่อเปรียบเทียบว่าประเภทของโพสต์ใดมีผลต่อจำนวน num_loves มากที่สุด

5.ปรับปรุงการวัดผล (Evaluation):
ให้เพิ่มการวัดผล Mean Absolute Error (MAE) เพิ่มเติม เพื่อประเมินความแม่นยำของโมเดล
'''
from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import col,format_number
from pyspark.sql.types import IntegerType
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler, OneHotEncoder
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import col, hour, when
from pyspark.sql.types import IntegerType
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# สร้าง SparkSession
spark = SparkSession.builder \
    .appName("FacebookPostAnalysis") \
    .getOrCreate()

# อ่านข้อมูลจาก CSV
data = spark.read.csv('./Practices/Regression/fb_live_thailand.csv', header=True, inferSchema=True)

# ขั้นตอน 1: แปลงประเภทโพสต์ (status_type) เป็นตัวแปรเชิงตัวเลข
indexer_types = StringIndexer(inputCol="status_type", outputCol="status_types_ind")

# สร้างฟีเจอร์ใหม่จากเวลาโพสต์ (status_published)
data = data.withColumn("post_hour", hour(col("status_published")))  # สร้างคอลัมน์ชั่วโมง

# แปลงเวลาที่โพสต์เป็นช่วงเวลา
data = data.withColumn("time_of_day",
                       when(col("post_hour").between(5, 11), "morning")
                       .when(col("post_hour").between(12, 17), "afternoon")
                       .when(col("post_hour").between(18, 21), "evening")
                       .otherwise("night"))

# แปลงช่วงเวลาเป็นตัวแปรเชิงตัวเลข
indexer_time = StringIndexer(inputCol="time_of_day", outputCol="time_of_day_ind")

# ขั้นตอน 2: สร้าง VectorAssembler เพื่อใช้ในการพยากรณ์
assembler = VectorAssembler(
    inputCols=["num_reactions", "num_loves", "status_types_ind", "time_of_day_ind"],
    outputCol="features"
)

# สร้างโมเดล Linear Regression
lr = LinearRegression(
    featuresCol="features",
    labelCol="num_loves",
    maxIter=50,  # ปรับเพิ่มค่า maxIter
    regParam=0.01,  # ปรับลดค่า regParam
    elasticNetParam=0.5  # ปรับค่า elasticNetParam
)

# สร้าง Pipeline
pipeline = Pipeline(stages=[indexer_types, indexer_time, assembler, lr])

# แบ่งข้อมูลเป็น train และ test
train_data, test_data = data.randomSplit([0.8, 0.2], seed=1234)

# ฝึกโมเดล
pipeline_model = pipeline.fit(train_data)

# ทำการพยากรณ์
predictions = pipeline_model.transform(test_data)

# ขั้นตอน 5: สร้างตัววัดผล
evaluator = RegressionEvaluator(
    labelCol="num_loves",
    predictionCol="prediction"
)

# คำนวณ MSE
evaluator.setMetricName("mse")
mse = evaluator.evaluate(predictions)
print(f"Mean Squared Error (MSE): {mse}")

# คำนวณ R2
evaluator.setMetricName("r2")
r2 = evaluator.evaluate(predictions)
print(f"R-squared (R2): {r2}")

# คำนวณ MAE
evaluator.setMetricName("mae")
mae = evaluator.evaluate(predictions)
print(f"Mean Absolute Error (MAE): {mae}")

# ขั้นตอน 4: สร้างกราฟที่แสดงการพยากรณ์แบบรายกลุ่ม
selected_data = predictions.select(
    col("num_loves").alias("actual_num_loves"),
    col("prediction").alias("predicted_num_loves"),
    col("status_types_ind").alias("status_type")
).orderBy(col("predicted_num_loves").desc())

# แปลงข้อมูลเป็น DataFrame ของ Pandas
selected_data_pd = selected_data.toPandas()

# สร้างกราฟ Actual vs Predicted
plt.figure(figsize=(12, 6))
sns.lmplot(
    data=selected_data_pd,
    x='actual_num_loves',
    y='predicted_num_loves',
    aspect=1.5,
    scatter_kws={'s': 50, 'alpha': 0.5},
    line_kws={'color': 'red'}
)

# ตั้งชื่อกราฟ
plt.title('Linear Regression: Actual vs Predicted Values')
plt.xlabel('Actual num_loves')
plt.ylabel('Predicted num_loves')
plt.show()

# หยุด SparkSession
spark.stop()