from pyspark import SparkContext

sc = SparkContext("local[*]", "RDDBasics")

## Task 1
print("Task 1\n")

# 1. Create RDD from a Python list
numbers = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
print(f"Numbers: {numbers.collect()}")
print(f"Partitions: {numbers.getNumPartitions()}")

# 2. Create RDD with explicit partitions
# YOUR CODE: Create the same list with exactly 4 partitions
numbers_4_partitions = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 4)
print(f"Numbers (4): {numbers_4_partitions.collect()}")
print(f"Partitions: {numbers_4_partitions.getNumPartitions()}")

# 3. Create RDD from a range
# YOUR CODE: Create RDD from range(1, 101)
numbers_range = sc.parallelize(range(1, 101))
print(f"Numbers (range): {numbers_range.collect()}")
print(f"Partitions (range): {numbers_range.getNumPartitions()}")


## Task 2
print("\nTask 2\n")
# Given: numbers RDD [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

# Task A: Square each number
# Expected: [1, 4, 9, 16, 25, 36, 49, 64, 81, 100]
squared = numbers.map(lambda x: x**2)
print(f"Task 2A result: {squared.collect()}")

# Task B: Convert to strings with prefix
# Expected: ["num_1", "num_2", "num_3", ...]
prefixed = numbers.map(lambda x: f"num_{x}")
print(f"Task 2B result: {prefixed.collect()}")


## Task 3
print("\nTask 3\n")
# Task A: Keep only even numbers
# Expected: [2, 4, 6, 8, 10]
evens = numbers.filter(lambda x: x % 2 == 0)
print(f"Task 3A result: {evens.collect()}")
# Task B: Keep numbers greater than 5
# Expected: [6, 7, 8, 9, 10]
greater_than_5 = numbers.filter(lambda x: x > 5)
print(f"Task 3B result: {greater_than_5.collect()}")
# Task C: Combine - even AND greater than 5
# Expected: [6, 8, 10]
combined = evens.intersection(greater_than_5)
print(f"Task 3C result {combined.collect()}")


## Task 4
print("\nTask 4\n")
# Given sentences
sentences = sc.parallelize([
    "Hello World",
    "Apache Spark is Fast",
    "PySpark is Python plus Spark"
])

# Task A: Split into words (use flatMap)
# Expected: ["Hello", "World", "Apache", "Spark", ...]
words = sentences.flatMap(lambda x: x.split())
print(f"Task 4A result: {words.collect()}")

# Task B: Create pairs of (word, length)
# Expected: [("Hello", 5), ("World", 5), ...]
word_lengths = sentences.flatMap(lambda x: x.split()) \
    .map(lambda x: (x, len(x)))
print(f"Task 4B result: {word_lengths.collect()}")


## Task 5
print("\nTask 5\n")

# Given: log entries
logs = sc.parallelize([
    "INFO: User logged in",
    "ERROR: Connection failed",
    "INFO: Data processed",
    "ERROR: Timeout occurred",
    "DEBUG: Cache hit"
])

# Pipeline: Extract only ERROR messages, convert to uppercase words
# 1. Filter to keep only ERROR lines
# 2. Split each line into words
# 3. Convert each word to uppercase
# Expected: ["ERROR:", "CONNECTION", "FAILED", "ERROR:", "TIMEOUT", "OCCURRED"]
error_words = logs.filter(lambda line: "ERROR" in line) \
    .flatMap(lambda line: line.split()) \
    .map(lambda word: word.upper())
print(f"Task 5 result: {error_words.collect()}")

sc.stop()