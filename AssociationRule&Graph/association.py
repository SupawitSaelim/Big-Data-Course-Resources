from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.ml.fpm import FPGrowth
from pyspark.sql.functions import collect_list, array_distinct, explode, split, col

spark = SparkSession.builder.appName("FPGrowthExample").getOrCreate()

data = spark.read.csv("./AssociationRule&Graph/groceries_data.csv", header=True, inferSchema=True)

grouped_data = data.groupBy("Member_number").agg(collect_list("itemDescription").alias("Items"))

grouped_data.show(truncate=False)

grouped_data = grouped_data.withColumn("basket", array_distinct(grouped_data["Items"]))

grouped_data.show(truncate=False)

exploded_data = grouped_data.select("Member_number", explode("Items").alias("item"))


separated_data = exploded_data.withColumn("item", explode(split("item", "/")))

final_data = separated_data.groupBy("Member_number").agg(collect_list("item").alias("Items"))

final_data = final_data.withColumn("Items", array_distinct(col("Items")))

final_data.show(truncate=False)

minSupport = 0.1
minConfidence = 0.2

fp = FPGrowth(minSupport=minSupport, minConfidence=minConfidence, itemsCol='Items', predictionCol='prediction')

model = fp.fit(final_data)

model.freqItemsets.show(10)  # Show top 10 frequent itemsets

filtered_rules = model.associationRules.filter(model.associationRules.confidence > 0.4)

filtered_rules.show(truncate=False)

new_data = spark.createDataFrame(
    [
        (["vegetable juice", "frozen fruits", "packaged fruit"],),
        (["mayonnaise", "butter", "buns"],)
    ],
    ["Items"]  # Changed to "Items"
)

new_data.show(truncate=False)

predictions = model.transform(new_data)

predictions.show(truncate=False)

spark.stop()