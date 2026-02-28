from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    collect_list, collect_set, first, last,
    stddev, variance, expr, sum, count
)

spark = SparkSession.builder.appName("Advanced Aggregations").getOrCreate()

# Sales data
sales_data = [
    ("2023-01", "Electronics", 1200),
    ("2023-01", "Electronics", 800),
    ("2023-01", "Clothing", 300),
    ("2023-02", "Electronics", 1500),
    ("2023-02", "Clothing", 400),
    ("2023-02", "Clothing", 350)
]

df = spark.createDataFrame(sales_data, ["month", "category", "amount"])

print("Sales Data:")
df.show()

# Collect values into lists
print("Products sold by month:")
df.groupBy("month").agg(
    collect_list("category").alias("categories_sold"),
    collect_set("category").alias("unique_categories"),
    count("*").alias("transaction_count"),
    sum("amount").alias("total_sales")
).show(truncate=False)

# Statistical aggregations
print("\nSales statistics by category:")
df.groupBy("category").agg(
    count("*").alias("transactions"),
    sum("amount").alias("total"),
    stddev("amount").alias("std_dev"),
    variance("amount").alias("variance")
).show()

# Using expr for SQL-like aggregations
print("\nUsing expr() for complex aggregations:")
df.groupBy("category").agg(
    expr("sum(amount) as total_sales"),
    expr("avg(amount) as avg_sale"),
    expr("percentile_approx(amount, 0.5) as median_sale")
).show()

import pyspark.sql.functions as F

df.orderBy(F.desc('amount')).show()