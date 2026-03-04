OPTIMIZATION REPORT
===================
Baseline Time:      __4.98__
Optimized Time:     __1.43__
Improvement:        __71.2%__% faster

Techniques Applied:
- Caching: cached the joins of sales with customer and sales with product since they were used multiple times
- Partitioning: we sort tables into different threads based on customer_id so they can be sorted within partitions  
- Bucketing: every table is bucketed based on columns that are often joined, that way join operations can be faster

Key Learnings:
- I learned a lot more about bucketing, which I did not have much practice with before
- I learned a lot more about partitioning, which I was still a bit confused on how it worked