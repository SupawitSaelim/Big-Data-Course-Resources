from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("AggregationExample").getOrCreate()

read_file = spark.read.format("csv")\
    .option("header", "true")\
    .load("data/*.csv")

read_file.createOrReplaceTempView("temp_view")

# คิวรีข้อมูลเพื่อทำ Aggregation
aggregation_query = """
SELECT
    COUNT(*) AS total_records,
    MIN(num_reactions) AS min_reactions,
    MAX(num_reactions) AS max_reactions,
    SUM(num_reactions) AS total_reactions
FROM temp_view
"""

# ประมวลผลคิวรีและรวมข้อมูล
aggregated_result = spark.sql(aggregation_query)

# แสดงผลลัพธ์
aggregated_result.show()
