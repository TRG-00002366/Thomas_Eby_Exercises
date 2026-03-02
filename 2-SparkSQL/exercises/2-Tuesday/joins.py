"""
Exercise: Joins
===============
Week 2, Tuesday

Practice all join types with customer and order data.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast

# =============================================================================
# SETUP - Do not modify
# =============================================================================

spark = SparkSession.builder.appName("Exercise: Joins").master("local[*]").getOrCreate()

# Customers
customers = spark.createDataFrame([
    (1, "Alice", "alice@email.com", "NY"),
    (2, "Bob", "bob@email.com", "CA"),
    (3, "Charlie", "charlie@email.com", "TX"),
    (4, "Diana", "diana@email.com", "FL"),
    (5, "Eve", "eve@email.com", "WA")
], ["customer_id", "name", "email", "state"])

# Orders
orders = spark.createDataFrame([
    (101, 1, "2023-01-15", 150.00),
    (102, 2, "2023-01-16", 200.00),
    (103, 1, "2023-01-17", 75.00),
    (104, 3, "2023-01-18", 300.00),
    (105, 6, "2023-01-19", 125.00),  # customer_id 6 does not exist!
    (106, 2, "2023-01-20", 180.00)
], ["order_id", "customer_id", "order_date", "amount"])

# Products (for multi-table join)
products = spark.createDataFrame([
    (101, "Laptop"),
    (102, "Phone"),
    (103, "Mouse"),
    (104, "Keyboard"),
    (107, "Monitor")  # Not in any order!
], ["order_id", "product_name"])

print("=== Exercise: Joins ===")
print("\nCustomers:")
customers.show()
print("Orders:")
orders.show()
print("Products:")
products.show()

# =============================================================================
# TASK 1: Inner Join (15 mins)
# =============================================================================

print("\n--- Task 1: Inner Join ---")

# TODO 1a: Join customers and orders (only matching records)
# Show customer name, order_id, order_date, amount
customers.join(orders, customers.customer_id == orders.customer_id).show()

# TODO 1b: How many orders have matching customers?
# HINT: Compare this count to total orders
order_count = orders.join(customers, 'customer_id', 'inner').count()
print(f"Num orders with matching customers: {order_count}")

# =============================================================================
# TASK 2: Left and Right Joins (20 mins)
# =============================================================================

print("\n--- Task 2: Left and Right Joins ---")

# TODO 2a: LEFT JOIN - All customers, with order info where available
# Who has NOT placed any orders?
customers.join(orders, customers.customer_id == orders.customer_id, 'left').show()

print("The customers who haven't placed orders:")
customers.join(orders, 'customer_id', 'left').filter(col('order_id').isNull()).show()

# TODO 2b: RIGHT JOIN - All orders, with customer info where available
# Which order has no matching customer?
customers.join(orders, 'customer_id', 'right').show()

print("The orders with no customer information:")
customers.join(orders, 'customer_id', 'right').filter(col('name').isNull()).show()

# TODO 2c: What is the difference between the two results?
# Answer in a comment:
# The difference between the two results is that in the left join, the records from the
# first table that don't match anything in the second table are included with Null values
# in columns to the second table. In the right join, it is the opposite. 
# The records from the second table and aren't in the first table are included with Null
# values in the columns corresponding to the first table.

# =============================================================================
# TASK 3: Full Outer Join (10 mins)
# =============================================================================

print("\n--- Task 3: Full Outer Join ---")

# TODO 3a: Perform a FULL OUTER join between customers and orders
# All customers AND all orders should appear
customers.join(orders, 'customer_id', 'full').show()

# TODO 3b: Filter to show only rows where there is a mismatch
# (customer without order OR order without customer)
customers.join(orders, 'customer_id', 'full').filter((col('name').isNull()) | (col('order_id').isNull())).show()

# =============================================================================
# TASK 4: Semi and Anti Joins (15 mins)
# =============================================================================

print("\n--- Task 4: Semi and Anti Joins ---")

# TODO 4a: LEFT SEMI JOIN - Customers who HAVE placed orders
# Only customer columns should appear
customers.join(orders, 'customer_id', 'left_semi').show()

# TODO 4b: LEFT ANTI JOIN - Customers who have NOT placed orders
customers.join(orders, 'customer_id', 'left_anti').show()

# TODO 4c: When would you use anti join in real data work?
# Answer in a comment:
# An anti join returns records with no match. There are many applications of this.
# In an application with users, you could use this to compare a table of users with
# a table of user action logs to get a list of inactive users. Perhaps you would use
# a view of user action logs from the past year and delete accounts that hadn't been
# used in that time.

# =============================================================================
# TASK 5: Handling Duplicate Columns (15 mins)
# =============================================================================

print("\n--- Task 5: Handling Duplicate Columns ---")

# After joining customers and orders, both have customer_id
# TODO 5a: Join and then DROP the duplicate customer_id column
customers.join(orders, customers.customer_id == orders.customer_id, 'inner') \
    .drop(orders.customer_id).show()

# TODO 5b: Alternative: Use aliases to reference specific columns
# HINT: customers.alias("c"), orders.alias("o")
c = customers.alias('c')
o = orders.alias('o')
c.join(o, c.customer_id == o.customer_id, 'full') \
    .drop(o.customer_id).show()


# =============================================================================
# TASK 6: Multi-Table Join (15 mins)
# =============================================================================

print("\n--- Task 6: Multi-Table Join ---")

# TODO 6a: Join customers -> orders -> products
# Show: customer name, order_id, amount, product_name
customers.join(orders, 'customer_id', 'inner') \
    .join(products, 'order_id', 'inner') \
    .select('name', 'order_id', 'amount', 'product_name').show()

# TODO 6b: What kind of join should you use when some orders might not have products?
print('Left join to account for orders without products')
customers.join(orders, 'customer_id', 'inner') \
    .join(products, 'order_id', 'left') \
    .select('name', 'order_id', 'amount', 'product_name').show()

# =============================================================================
# CHALLENGE: Real-World Scenarios (20 mins)
# =============================================================================

print("\n--- Challenge: Real-World Scenarios ---")

# TODO 7a: Find the total spending per customer (only customers with orders)
# Use join + groupBy + sum
customers.join(orders, 'customer_id', 'inner') \
    .groupBy('customer_id') \
    .sum('amount').show()

# TODO 7b: Find customers from CA who placed orders > $150
customers.filter(col('state') == "CA") \
    .join(orders, 'customer_id', 'inner') \
    .filter(col('amount') > 150) \
    .dropDuplicates(['customer_id']).show()

# TODO 7c: Find orders without valid product information
# (anti join pattern)
orders.join(products, 'order_id', 'left_anti').show()

# =============================================================================
# CLEANUP
# =============================================================================

spark.stop()