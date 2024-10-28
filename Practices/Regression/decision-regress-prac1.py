"""
ใช้ข้อมูลการมีส่วนร่วมของโพสต์บน Facebook เพื่อทำการวิเคราะห์การถดถอยด้วย Decision Tree 
Regression เพื่อพยากรณ์จำนวน "num_shares" โดยแปลงฟีเจอร์และสร้างโมเดล 
จากนั้นประเมินผลและแสดงความสัมพันธ์ระหว่างค่าจริงและค่าที่พยากรณ์ด้วยกราฟกระจาย (scatter plot)
Using Facebook post interaction data, perform regression analysis using Decision 
Tree Regression on the features "num_reactions," "num_comments," "num_shares," 
"num_likes," and "num_wows" to predict the number of "num_shares" for each post. 
Begin by creating a SparkSession and reading the dataset from a CSV file. Use 
StringIndexer to convert the categorical feature "status_type" into numerical 
indices for model training. OneHotEncoder can be applied to the indexed features 
to create binary indicator variables. Assemble the features into a single vector using 
VectorAssembler. Build a machine learning pipeline that includes the preprocessing steps 
and the Decision Tree model. Split the data into training and testing sets and fit the model on the training data. Evaluate the model's performance using metrics such as R² and Mean Absolute Error (MAE). Finally, visualize the relationship between actual and predicted values for "num_shares" using a scatter plot to assess the model's predictive capabilities.
"""
from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler, OneHotEncoder
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
import seaborn as sns
import matplotlib.pyplot as plt

# สร้าง SparkSession
spark = SparkSession.builder.appName("DecisionTreeRegressionFBLive").getOrCreate()

# อ่านข้อมูล
data = spark.read.csv("./RegressionandDecision/fb_live_thailand.csv", header=True, inferSchema=True)

# ขั้นตอน 1: การแปลงข้อมูล
indexer_types = StringIndexer(inputCol="status_type", outputCol="status_types_ind", handleInvalid="skip")

# การแปลงข้อมูล
indexed_data = indexer_types.fit(data).transform(data)

# OneHot Encoding
encoder_types = OneHotEncoder(inputCol="status_types_ind", outputCol="status_types_encoded")

# ขั้นตอน 2: สร้าง VectorAssembler และ Decision Tree
assembler = VectorAssembler(inputCols=["num_reactions", "num_comments", "num_shares", "num_likes", "num_wows", "status_types_encoded"], outputCol="features")
dt = DecisionTreeRegressor(featuresCol="features", labelCol="num_shares")  # คาดการณ์ num_shares โดยตรง

# สร้าง Pipeline
pipeline = Pipeline(stages=[encoder_types, assembler, dt])

# แบ่งข้อมูลเป็น train และ test
train_data, test_data = indexed_data.randomSplit([0.8, 0.2], seed=1234)

# ฝึกโมเดล
dt_model = pipeline.fit(train_data)

# ทำการพยากรณ์
predictions = dt_model.transform(test_data)

# ขั้นตอน 3: การประเมินผล
evaluator = RegressionEvaluator(labelCol="num_shares", predictionCol="prediction")
r2 = evaluator.setMetricName("r2").evaluate(predictions)
mae = evaluator.setMetricName("mae").evaluate(predictions)

print(f"R2 Score: {r2}")
print(f"Mean Absolute Error (MAE): {mae}")

# ขั้นตอน 4: สร้างกราฟ
selected_data = predictions.select("num_shares", "prediction").toPandas()

# สร้างกราฟการเปรียบเทียบ
plt.figure(figsize=(12, 6))
sns.lmplot(x="num_shares", y="prediction", data=selected_data)
plt.title("Decision Tree Regression: Actual vs Predicted num_shares")
plt.xlabel("Actual num_shares")
plt.ylabel("Predicted num_shares")
plt.show()

# หยุด SparkSession
spark.stop()
