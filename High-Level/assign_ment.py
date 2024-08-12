from pyspark.sql import SparkSession
from pyspark.sql.functions import count, countDistinct, first, last, min, max, sum, sumDistinct, avg
from pyspark.sql.types import IntegerType

# Create Spark Session
spark = SparkSession.builder.appName("SparkExample").getOrCreate()

# Read data from CSV files
read_file = spark.read.format("csv")\
    .option("header", "true")\
    .load("data/fb_live_thailand.csv")  # Replace with your file path

# Print the schema of the DataFrame
read_file.printSchema()

# Perform SQL query: Select specific columns and show results
sqlDF = read_file.select("status_published", "num_reactions")  # Replace with actual column names
sqlDF.show(10)  # Replace 10 with the number of rows you want to display

# Perform SQL query with a where clause
sqlDF = read_file.select("num_reactions", "num_comments", "num_loves")\
    .where(read_file["num_reactions"].cast(IntegerType()) > 100)\
    .withColumnRenamed("num_loves", "new_num_loves")\
    .orderBy("num_reactions")  # Order by num_reactions
sqlDF.show(10)  # Replace 10 with the number of rows you want to display

# Create a temporary view
read_file.createOrReplaceTempView("tempTable")

# Perform SQL query from the temporary view
sqlDF = spark.sql("SELECT * FROM tempTable")
sqlDF.show(10)  # Replace 10 with the number of rows you want to display

# Save the DataFrame to a CSV file
sqlDF.write.mode("overwrite").csv("output_folder")  # Replace with your output folder

# Split data into two sets
split = sqlDF.randomSplit([0.7, 0.3])  # Example split ratio, adjust as needed
split[0].show()  # Show data in the first set
split[1].show()  # Show data in the second set

# Read multiple CSV files
read_file = spark.read.format("csv")\
    .option("header", "true")\
    .load("data/*.csv")  # Replace with your file path

# Create a temporary view for the new DataFrame
read_file.createOrReplaceTempView("tempTable")

# Count the number of rows in a column
count_df = spark.sql("SELECT COUNT(status_published) AS count_status_published FROM tempTable")  # Replace column_name
count_df.show()

# Count the number of rows where status_type is 'photo'
sqlDF = spark.sql("SELECT * FROM tempTable WHERE status_type = 'photo'")
count_df = sqlDF.select(count("status_published"))
count_df.show()

# Count distinct values in a column
countDistinct_df = sqlDF.select(countDistinct("num_reactions"))  # Replace column_name
countDistinct_df.show()

# Select first and last values in a column
first_last_df = sqlDF.select(first("num_shares"), last("num_shares"))  # Replace column_name
first_last_df.show()

# Calculate min and max values of a column after converting to IntegerType
sqlDF = sqlDF.withColumn('new_num_comments', sqlDF['num_comments'].cast(IntegerType()))  # Replace column names
min_max_df = sqlDF.select(min('new_num_comments').alias('min_value'), max('new_num_comments').alias('max_value'))
min_max_df.show()

# Calculate sum and distinct sum of a column
sum_df = sqlDF.select(sum('num_likes').alias('total_sum'))  # Replace column_name
sumDistinct_df = sqlDF.select(sumDistinct('num_likes').alias('distinct_sum'))  # Replace column_name
sum_df.show()
sumDistinct_df.show()

# Calculate the average of a column
avg_df = sqlDF.select(avg('num_likes').alias('average_value'))  # Replace column_name
avg_df.show()

# Perform join operations
df1 = spark.read.format("csv").option("header", "true").load("data/fb_live_thailand.csv")  # Replace with your file path
df2 = spark.read.format("csv").option("header", "true").load("data/fb_live_thailand2.csv")  # Replace with your file path

# Create temporary views for joins
df1.createOrReplaceTempView("table1")
df2.createOrReplaceTempView("table2")

# Identify the join column
join_column = df1["status_id"] == df2["status_id"]

# Perform inner join
inner_join_df = df1.join(df2, join_column, "inner")
inner_join_df.show()

# Perform left outer join
left_outer_join_df = df1.join(df2, join_column, "left_outer")
left_outer_join_df.show()

# Perform right outer join
right_outer_join_df = df1.join(df2, join_column, "right_outer")
right_outer_join_df.show()

# Perform outer join
right_outer_join_df = df1.join(df2, join_column, "outer")
right_outer_join_df.show()