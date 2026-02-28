"""
Exercise: Aggregations
======================
Week 2, Tuesday

Practice groupBy and aggregate functions on sales data.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, avg, count, min, max, countDistinct

# =============================================================================
# SETUP - Do not modify
# =============================================================================

spark = SparkSession.builder.appName("Exercise: Aggregations").master("local[*]").getOrCreate()

# Sample sales data
sales = [
    ("2023-01", "Electronics", "Laptop", 1200, "Alice"),
    ("2023-01", "Electronics", "Phone", 800, "Bob"),
    ("2023-01", "Electronics", "Tablet", 500, "Alice"),
    ("2023-01", "Clothing", "Jacket", 150, "Charlie"),
    ("2023-01", "Clothing", "Shoes", 100, "Diana"),
    ("2023-02", "Electronics", "Laptop", 1300, "Eve"),
    ("2023-02", "Electronics", "Phone", 850, "Alice"),
    ("2023-02", "Clothing", "Jacket", 175, "Bob"),
    ("2023-02", "Clothing", "Pants", 80, "Charlie"),
    ("2023-03", "Electronics", "Laptop", 1100, "Frank"),
    ("2023-03", "Electronics", "Phone", 750, "Grace"),
    ("2023-03", "Clothing", "Shoes", 120, "Alice")
]

df = spark.createDataFrame(sales, ["month", "category", "product", "amount", "salesperson"])

print("=== Exercise: Aggregations ===")
print("\nSales Data:")
df.show()

# =============================================================================
# TASK 1: Simple Aggregations (15 mins)
# =============================================================================

print("\n--- Task 1: Simple Aggregations ---")

# TODO 1a: Calculate total, average, min, and max amount across ALL sales
# Use agg() without groupBy
print("Amount Statistics")
df.agg(
    spark_sum('amount').alias('total_amount'),
    avg('amount').alias('avg_amount'),
    min('amount').alias('min_amount'),
    max('amount').alias('max_amount')
).show()

# TODO 1b: Count the total number of sales transactions
print(f"Total number of sales transactions: {df.count()}")

# TODO 1c: Count distinct categories
category_count = df.select('category').distinct().count()
print(f'Distinct category count: {category_count}')

# =============================================================================
# TASK 2: GroupBy with Single Aggregation (15 mins)
# =============================================================================

print("\n--- Task 2: GroupBy Single Aggregation ---")

# TODO 2a: Total sales amount by category
print("Total Sales Amount by Category")
df.groupBy('category').count().show()

# TODO 2b: Average sale amount by month
print("Average Sale Amount by Month")
df.groupBy('month').avg('amount').show()

# TODO 2c: Count of transactions by salesperson
print("Transaction Count by Salesperson")
df.groupBy('salesperson').count().show()

# =============================================================================
# TASK 3: GroupBy with Multiple Aggregations (20 mins)
# =============================================================================

print("\n--- Task 3: GroupBy Multiple Aggregations ---")

# TODO 3a: For each category, calculate:
# - Number of transactions
# - Total revenue
# - Average sale amount
# - Highest single sale
# Use meaningful aliases!
print("Category Statistics")
df.groupBy('category').agg(
    count('*').alias("transaction_count_by_category"),
    spark_sum('amount').alias('total_revenue_by_category'),
    avg('amount').alias('avg_sales_by_category'),
    max('amount').alias('highest_sale_by_category')
).show()

# TODO 3b: For each salesperson, calculate:
# - Number of sales
# - Total revenue
# - Distinct products sold (countDistinct)
print("Salesperson Statistics")
df.groupBy('salesperson').agg(
    count('*').alias('sale_count_by_salesperson'),
    spark_sum('amount').alias('total_revenue_by_salesperson'),
    countDistinct('product').alias('distinct_product_sale_count_by_salesperson')
).show()

# =============================================================================
# TASK 4: Multi-Column GroupBy (15 mins)
# =============================================================================

print("\n--- Task 4: Multi-Column GroupBy ---")

# TODO 4a: Calculate total sales by month AND category
print("Total Sales by Month and Category")
df.groupBy(['month', 'category']).sum('amount').show()

from pyspark.sql import Window
import pyspark.sql.functions as F
# TODO 4b: Find the top salesperson by month (hint: use multi-column groupBy)
# Note: Needed to search beyond the provided material to learn how to do this
monthly_sales = df.groupBy('month', 'salesperson') \
    .agg(
        spark_sum('amount').alias('total_sales')
    )
window = Window.partitionBy('month').orderBy(F.desc('total_sales'))
ranked_monthly_sales = monthly_sales.withColumn('rank', F.row_number().over(window))
print("Extra dataframe showing how I got the monthly rankings. Required research on how to use window functions.")
ranked_monthly_sales.show()
result = ranked_monthly_sales.filter(col('rank') == 1).drop('rank')
print('Top Salesperson by Month')
result.show()

# =============================================================================
# TASK 5: Filtering After Aggregation (15 mins)
# =============================================================================

print("\n--- Task 5: Filtering After Aggregation ---")

# TODO 5a: Find categories with total revenue > 2000
print("Categories with Total Revenue > 2000")
df.groupBy('category').agg(
    spark_sum('amount').alias('total_revenue')
).filter(col('total_revenue') > 2000).show()

# TODO 5b: Find salespeople who made more than 2 transactions
print("Salespeople Who Made More Than 2 Transactions")
df.groupBy('salesperson').agg(
    count('*').alias('num_transactions')
).filter(col('num_transactions') > 2).show()

# TODO 5c: Find month-category combinations with average sale > 500
print("Month-Category Combinations with Average Sale > 500")
df.groupBy(['month', 'category']).agg(
    avg('amount').alias('avg_sale')
).filter(col('avg_sale') > 500).show()

# =============================================================================
# CHALLENGE: Business Questions (20 mins)
# =============================================================================

print("\n--- Challenge: Business Questions ---")

# TODO 6a: Which category had the highest average transaction value?
highest_avg_value = df.groupBy('category').agg(
    avg('amount').alias('avg_transaction_value')
).orderBy(F.desc('avg_transaction_value')).first()
print(f"The category with the highest average transaction value: {highest_avg_value[0]}\nAt: ${highest_avg_value[1]}")
# TODO 6b: Who is the top salesperson by total revenue?
top_salesperson = df.groupBy('salesperson').agg(
    spark_sum('amount').alias('total_revenue')
).orderBy(F.desc('total_revenue')).first()
print(f"The salesperson with the highest total revenue: {top_salesperson[0]}\nAt: ${top_salesperson[1]}")

# TODO 6c: Which month had the most diverse products sold?
# HINT: Use countDistinct on product column
most_diverse_month = df.groupBy('month').agg(
    countDistinct('product').alias('distinct_count')
).orderBy(F.desc('distinct_count')).first()
print(f"The month with the most diverse products sold: {most_diverse_month[0]}\nWith: {most_diverse_month[1]} distinct products sold")

# =============================================================================
# CLEANUP
# =============================================================================

spark.stop()