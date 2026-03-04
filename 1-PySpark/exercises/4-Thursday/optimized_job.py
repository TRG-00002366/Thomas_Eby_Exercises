from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum

spark = SparkSession.builder \
    .appName("SlowJob") \
    .master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext

# Generate sample data (simulating large dataset)
data = [(f"product_{i % 100}", f"category_{i % 10}", i * 1.5, i % 50) 
        for i in range(100000)]

rdd = sc.parallelize(data)
rdd.cache()

# Problematic Pattern 1: Using groupByKey
grouped = rdd.map(lambda x: (x[1], x[2])) \
             .reduceByKey(lambda a, b: a + b)

# Problematic Pattern 2: Collecting large data
all_data = rdd.take(5)

# Problematic Pattern 3: Multiple actions on same RDD
count1 = rdd.count()
count2 = rdd.filter(lambda x: x[3] > 25).count()
count3 = rdd.map(lambda x: x[2]).reduce(lambda a, b: a + b)

print(f"Grouped: {grouped.take(5)}")
print(f"Counts: {count1}, {count2}, {count3}")

spark.stop()