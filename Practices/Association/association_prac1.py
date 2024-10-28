from pyspark.sql import SparkSession  
from pyspark.ml.fpm import FPGrowth  
from pyspark.sql.functions import collect_list, array_distinct, explode, split, col, count

# สร้าง Spark Session
spark = SparkSession.builder.appName("FPGrowthExample").getOrCreate()

# อ่านข้อมูลจากไฟล์ CSV
data = spark.read.csv("./groceries_data.csv", header=True, inferSchema=True)

# กรองข้อมูลเฉพาะปี 2015
data_filtered = data.filter(data.Date.substr(1, 4) == "2015")

# สร้างกลุ่มข้อมูลตามสมาชิก
grouped_data = data_filtered.groupBy("Member_number").agg(collect_list("itemDescription").alias("Items"))
grouped_data.show(truncate=False)  

# กำจัดรายการที่ซ้ำกันในแต่ละสมาชิก
grouped_data = grouped_data.withColumn("basket", array_distinct(grouped_data["Items"]))
grouped_data.show(truncate=False)  

# แยกข้อมูลที่ถูกทำให้เป็นรายการ
exploded_data = grouped_data.select("Member_number", explode("Items").alias("item"))
separated_data = exploded_data.withColumn("item", explode(split("item", "/")))
separated_data.show(10)

# สร้าง DataFrame สุดท้ายโดยกลุ่มข้อมูลสมาชิก
final_data = separated_data.groupBy("Member_number").agg(collect_list("item").alias("Items"))
final_data = final_data.withColumn("Items", array_distinct(col("Items")))
final_data.show(truncate=False)  

# แสดงจำนวนสมาชิกที่ซื้อสินค้าประเภทต่าง ๆ
item_counts = final_data.select(explode("Items").alias("item")).groupBy("item").agg(count("item").alias("count"))
item_counts.orderBy(col("count").desc()).show(truncate=False)

# กำหนดค่า minSupport และ minConfidence
minSupport = 0.1  # คุณสามารถปรับค่านี้ได้
minConfidence = 0.5  # คุณสามารถปรับค่านี้ได้

# สร้างโมเดล FPGrowth
fp = FPGrowth(minSupport=minSupport, minConfidence=minConfidence, itemsCol='Items', predictionCol='prediction')

# ฝึกโมเดล
model = fp.fit(final_data)

# แสดงผลรายการที่มีความถี่
model.freqItemsets.show(10)

# กรองกฎที่มีความมั่นใจมากกว่า 0.4
filtered_rules = model.associationRules.filter(model.associationRules.confidence > 0.4)

# แสดงผลกฎที่กรองแล้ว
filtered_rules.show(truncate=False)

# ดึงข้อมูลรายการที่สมาชิก 1808 เคยซื้อ
member_1808_items = final_data.filter(final_data.Member_number == 1808).select("Items")
member_1808_items.show(truncate=False)

# ระบุรายการที่ต้องการ (เช่น "candy")
user_input_item = ["candy"]

# สร้าง DataFrame ใหม่สำหรับการทำนาย โดยใช้รายการที่สมาชิก 1808 เคยซื้อรวมกับรายการที่ระบุ
combined_items = member_1808_items.collect()[0].Items + user_input_item  # รวมรายการที่ซื้อและรายการที่ระบุ

# สร้าง DataFrame ใหม่สำหรับการทำนาย
new_data = spark.createDataFrame(
    [
        (combined_items,)  # ใช้รายการที่รวมกัน
    ],
    ["Items"]  # ตั้งชื่อคอลัมน์ว่า "Items"
)

new_data.show(truncate=False)  
predictions = model.transform(new_data)
predictions.show(truncate=False)  

# ปิด Spark Session
spark.stop()  
