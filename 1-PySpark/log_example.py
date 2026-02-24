from pyspark import SparkContext

sc = SparkContext("local[*]", "BasicRDDOperations")

# Sample log data
log_data = [
    "2024-01-15 10:30:22 INFO User login successful",
    "2024-01-15 10:31:05 ERROR Database connection failed",
    "2024-01-15 10:31:10 INFO Retry connection",
    "2024-01-15 10:31:15 ERROR Database connection failed",
    "2024-01-15 10:32:00 INFO Database connected",
    "2024-01-15 10:33:45 WARN High memory usage detected",
    "2024-01-15 10:35:00 ERROR Out of memory exception",
    "2024-01-15 10:35:05 INFO System restart initiated"
]

logs = sc.parallelize(log_data)

# Example 1: Filter and map
print("=== Error Logs (timestamp only) ===")
error_times = logs \
    .filter(lambda line: "ERROR" in line) \
    .map(lambda line: line.split()[0] + " " + line.split()[1])

for time in error_times.collect():
    print(f"  {time}")

# Example 2: FlatMap to extract words
print("\n=== Unique Log Levels ===")
log_levels = logs \
    .map(lambda line: line.split()[2]) \
    .distinct()

print(f"  {log_levels.collect()}")

# Example 3: Complex chain
print("\n=== Word Frequency in Error Messages ===")
error_words = logs \
    .filter(lambda line: "ERROR" in line) \
    .flatMap(lambda line: line.split()[3:]) \
    .map(lambda word: (word.lower(), 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .sortBy(lambda x: x[1], ascending=False)

for word, count in error_words.collect():
    print(f"  {word}: {count}")

# Example 4: Set operations
info_lines = logs.filter(lambda l: "INFO" in l)
warn_error_lines = logs.filter(lambda l: "WARN" in l or "ERROR" in l)

print(f"\n=== Line Counts ===")
print(f"  INFO lines: {info_lines.count()}")
print(f"  WARN/ERROR lines: {warn_error_lines.count()}")
print(f"  All lines: {logs.count()}")
print(f"  Combined (union): {info_lines.union(warn_error_lines).count()}")

sc.stop()