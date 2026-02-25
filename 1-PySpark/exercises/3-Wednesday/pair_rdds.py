from pyspark import SparkContext

sc = SparkContext("local[*]", "PairRDDs")

## TASK 1
print("TASK 1\n")

# Sample text
text = sc.parallelize([
    "Apache Spark is a fast and general engine",
    "Spark provides APIs in Python Java and Scala",
    "Spark is used for big data processing",
    "PySpark is the Python API for Spark"
])

# Implement Word Count:
# 1. Split lines into words
# 2. Convert to (word, 1) pairs
# 3. Sum counts by key
# 4. Sort by count descending

# YOUR CODE HERE
word_counts = text.flatMap(lambda line: line.split()) \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda x, y: x + y) \
    .sortBy(lambda pair: pair[1], ascending=False)

print("Word Counts (top 10):")
for word, count in word_counts.take(10):
    print(f"  {word}: {count}")


## TASK 2
print("\nTASK 2\n")

# Products
products = sc.parallelize([
    ("P001", "Laptop"),
    ("P002", "Mouse"),
    ("P003", "Keyboard"),
    ("P004", "Monitor")
])

# Prices
prices = sc.parallelize([
    ("P001", 999),
    ("P002", 29),
    ("P003", 79),
    ("P005", 199)  # Note: P005 not in products
])

# Task A: Inner join
inner = products.join(prices)
print(f"Inner join: {inner.collect()}")

# Task B: Left outer join (keep all products)
left = products.leftOuterJoin(prices)
print(f"Left join: {left.collect()}")

# Task C: Right outer join (keep all prices)
right = products.rightOuterJoin(prices)
print(f"Right join: {right.collect()}")

# Task D: Full outer join
full = products.fullOuterJoin(prices)
print(f"Full join: {full.collect()}")


## TASK 3
print("\nTASK 3\n")
print("There are no task 3 instructions")


## TASK 4
print("\nTASK 4\n")

# Employee data: (department, (name, salary))
employees = sc.parallelize([
    ("Engineering", ("Alice", 90000)),
    ("Engineering", ("Bob", 85000)),
    ("Sales", ("Charlie", 70000)),
    ("Engineering", ("Diana", 95000)),
    ("Sales", ("Eve", 75000)),
    ("HR", ("Frank", 60000))
])

# Task A: Count employees per department
dept_counts = employees.mapValues(lambda x: 1).reduceByKey(lambda x, y: x + y)# YOUR CODE using mapValues and reduceByKey
print(f"Employee counts: {dept_counts.collect()}")

# Task B: Sum salaries per department
dept_salaries = employees.mapValues(lambda x: x[1]).reduceByKey(lambda a, b: a + b)
print(f"Total salaries: {dept_salaries.collect()}")

# Task C: Average salary per department (hint: use aggregateByKey or combine count+sum)
avg_dept_salaries = employees.mapValues(lambda x: (x[1], 1)) \
    .reduceByKey(lambda pair1, pair2: (pair1[0] + pair2[0], pair1[1] + pair2[1])) \
    .mapValues(lambda pair: pair[0] / pair[1])

print(f"Average salaries: {avg_dept_salaries.collect()}")

# avg_dept_salaries2 = employees.aggregateByKey(
#     (0,0),
#     lambda acc, value: (acc[0] + value[1], acc[1] + 1),
#     lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])
# ).mapValues(lambda pair: pair[0] / pair[1])

# print(f"Average salaries (using aggregateByKey): {avg_dept_salaries2.collect()}")


## TASK 5
print("\nTASK 5\n")

# Sort word counts alphabetically
alphabetical = word_counts.sortByKey()
print(f"Alphabetical: {alphabetical.take(10)}")

# Sort by key descending
reverse = word_counts.sortByKey(ascending=False)
print(f"Reverse: {reverse.take(10)}")