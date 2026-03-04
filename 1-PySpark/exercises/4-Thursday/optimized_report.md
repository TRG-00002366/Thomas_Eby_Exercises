# Spark Optimization Report

## Team Members
- Driver 1: [Ty Barron]
- Driver 2: [Thomas Eby]

## Issues Identified

### Issue 1: [groupByKey then map]
- Problem: Using groupByKey and then reducing with map requires a costly shuffle
- Impact: Slower performance due to more network traffic between executors
- Solution: Use reduceByKey to reduce within partitions before shuffling

### Issue 2: [Collect Large Dataset]
- Problem: Collecting a large dataset
- Impact: Can crash program if too big. Collect loads the entire result.
- Solution: Use take(n) to only get some of the data

### Issue 3: [Cache rdd]
- Problem: Calling actions on an rdd trigger computations
- Impact: Wasted effort when you could save the result and reuse it
- Solution: Use rdd.cache() to store the first computation of the rdd and reuse it

## Performance Comparison

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Time   |     4.744s   |   4.647s    |     0.097        |
| Memory |    2g    |   2g    |     0g        |

## Configuration Used
[List spark-submit options]
spark-submit \
    --master local[*] \
    --driver-memory 2g \
    --conf spark.default.parallelism=8 \
    --conf spark.sql.shuffle.partitions=8 \
    optimized_job.py

## Lessons Learned
[What did you learn about Spark optimization?]
Reducing shuffling and increasing parallelism will improve performance