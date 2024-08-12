from pyspark.sql import SparkSession
from pyspark.sql.functions import min, max
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.appName("MinMaxExample").getOrCreate()
read_file = spark.read.format("csv")\
    .option("header", "true")\
    .load("data/*.csv")

read_file.createOrReplaceTempView("tempTable")
sqlDF = spark.sql("SELECT * FROM tempTable")

# แปลงคอลัมน์เป็นประเภท Integer และเพิ่มเป็นคอลัมน์ใหม่
sqlDF = sqlDF.withColumn('new_num_comments', 
                         sqlDF['num_comments'].cast(IntegerType()))

# คำนวณค่า min และ max ของคอลัมน์ใหม่
result_df = sqlDF.select(
    min('new_num_comments').alias('min_value'),
    max('new_num_comments').alias('max_value')
)

result_df.show()
