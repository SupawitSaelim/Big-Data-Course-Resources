from pyspark.sql import SparkSession  
from pyspark.ml.fpm import FPGrowth  
from pyspark.sql.functions import collect_list, array_distinct, explode, split, col  

# สร้าง Spark Session และตั้งชื่อให้กับแอพพลิเคชัน
spark = SparkSession.builder.appName("FPGrowthExample").getOrCreate()
# .config("spark.driver.port", "4040") \ .getOrCreate()  # สามารถกำหนดพอร์ตของ Spark Driver ได้ที่นี่ (ถ้าจำเป็น)

data = spark.read.csv("./groceries_data.csv", header=True, inferSchema=True)

# จัดกลุ่มข้อมูลตาม Member_number และรวบรวมรายการสินค้าในรูปแบบลิสต์
grouped_data = data.groupBy("Member_number").agg(collect_list("itemDescription").alias("Items"))
grouped_data.show(truncate=False)  # แสดงข้อมูลที่จัดกลุ่มแล้ว

# เพิ่มคอลัมน์ basket ที่เก็บรายการสินค้าแบบไม่ซ้ำกัน เช่น [soda, canned beer, sausage, sausage,] -> [soda, canned beer, sausage,]
grouped_data = grouped_data.withColumn("basket", array_distinct(grouped_data["Items"]))
grouped_data.show(truncate=False)  # แสดงข้อมูลที่มีคอลัมน์ basket

# แยกรายการสินค้าแต่ละรายการออกจากลิสต์โดยใช้ explode  1000: [soda, canned beer, sausage,] -> (แนวตั้ง)
exploded_data = grouped_data.select("Member_number", explode("Items").alias("item"))
# แยกรายการที่มีการแบ่งตามเครื่องหมาย "/" ออกเป็นหลายแถว
separated_data = exploded_data.withColumn("item", explode(split("item", "/")))
separated_data.show(10)


# จัดกลุ่มข้อมูลใหม่ตาม Member_number และรวบรวมรายการสินค้าอีกครั้ง ก็คือเอา รายการซ้ำและเครื่องหมาย / ออก
final_data = separated_data.groupBy("Member_number").agg(collect_list("item").alias("Items"))
# ทำให้รายการสินค้าในคอลัมน์ Items ไม่มีรายการซ้ำ
final_data = final_data.withColumn("Items", array_distinct(col("Items")))
final_data.show(truncate=False)  # แสดงข้อมูลสุดท้ายที่เตรียมไว้สำหรับการทำ FPGrowth


minSupport = 0.1  
minConfidence = 0.2  

# สร้างโมเดล FPGrowth โดยระบุคอลัมน์ที่ใช้และพารามิเตอร์
fp = FPGrowth(minSupport=minSupport, minConfidence=minConfidence, \
              itemsCol='Items', predictionCol='prediction')

# ฝึกโมเดลด้วยข้อมูลที่เตรียมไว้
model = fp.fit(final_data)

# แสดงชุดรายการที่เกิดขึ้นบ่อยที่สุด 10 อันดับ
model.freqItemsets.show(10)

# กรองกฎที่มีความเชื่อมั่นสูงกว่า 0.4
filtered_rules = model.associationRules.filter(model.associationRules.confidence > 0.4)

# แสดงกฎที่ถูกกรอง
filtered_rules.show(truncate=False)

# สร้าง DataFrame ใหม่สำหรับการทดสอบโมเดล
new_data = spark.createDataFrame(
    [
        (["vegetable juice", "frozen fruits", "packaged fruit"],),
        (["mayonnaise", "butter", "buns"],)
    ],
    ["Items"]  # ตั้งชื่อคอลัมน์ว่า "Items"
)

new_data.show(truncate=False)  
predictions = model.transform(new_data)
predictions.show(truncate=False)  

spark.stop()

'''
โค้ดนี้สร้างและใช้โมเดล FPGrowth ในการวิเคราะห์การซื้อขายสินค้าในรูปแบบ Association Rule Mining 
เพื่อตรวจจับชุดของรายการที่ถูกซื้อร่วมกันบ่อยครั้ง และสร้างกฎที่สามารถช่วยในการทำการตลาดหรือการจัดโปรโมชั่น
'''
