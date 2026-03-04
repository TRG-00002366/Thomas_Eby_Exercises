from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SlowJob") \
    .master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext

# Generate sample data (simulating large dataset)
data = [(f"product_{i % 100}", f"category_{i % 10}", i * 1.5, i % 50) 
        for i in range(100000)]

rdd = sc.parallelize(data)

# Problematic Pattern 1: Using groupByKey
# costly shuffle
grouped = rdd.map(lambda x: (x[1], x[2])) \
             .groupByKey() \
             .mapValues(lambda values: sum(values))

# Problematic Pattern 2: Collecting large data
# could crash if too big
all_data = rdd.collect()

# Problematic Pattern 3: Multiple actions on same RDD
# redoing work
count1 = rdd.count()
count2 = rdd.filter(lambda x: x[3] > 25).count()
count3 = rdd.map(lambda x: x[2]).reduce(lambda a, b: a + b)

print(f"Grouped: {grouped.take(5)}")
print(f"Counts: {count1}, {count2}, {count3}")

spark.stop()