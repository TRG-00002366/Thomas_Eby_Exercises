spark-submit \
    --master local[*] \
    --driver-memory 2g \
    --conf spark.default.parallelism=10 \
    --conf spark.sql.shuffle.partitions=4 \
    slow_job.py

# optimized job 4647 ms 2g 8 8
# slow job 4744 ms 2g 8 8