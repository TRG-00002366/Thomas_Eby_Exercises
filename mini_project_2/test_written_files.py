from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('test files').getOrCreate()

df = spark.read.parquet("mini_project_2/Ride_Analytics_Results/")
df_total_fares = spark.read.csv("mini_project_2/Ride_Analytics_Results/total_fares/", header=True, inferSchema=True)
df_avg_ratings = spark.read.csv("mini_project_2/Ride_Analytics_Results/avg_ratings/", header=True, inferSchema=True)

df.show()
df_total_fares.show()
df_avg_ratings.show()
