"""
Exercise: SparkSession Setup and Configuration
===============================================
Week 2, Monday

Complete the TODOs below to practice creating and configuring SparkSession objects.
"""

from pyspark.sql import SparkSession

# =============================================================================
# TASK 1: Basic SparkSession Creation
# =============================================================================

# TODO 1a: Create a SparkSession with:
#   - App name: "MyFirstSparkSQLApp"
#   - Master: "local[*]"
# HINT: Use SparkSession.builder.appName(...).master(...).getOrCreate()

spark = SparkSession.builder.appName("MyFirstSparkSQLApp").master("local[*]").getOrCreate()  # Replace with your code

# TODO 1b: Print the following information:
#   - Spark version
#   - Application ID
#   - Default parallelism
# HINT: Access these via spark.version, spark.sparkContext.applicationId, etc.

print("=== Task 1: Basic SparkSession ===")
# Your print statements here
print(spark.version)
print(spark.sparkContext.applicationId)
print(spark.sparkContext.defaultParallelism)

# TODO 1c: Create a simple DataFrame with 3 columns and 5 rows
# to verify your session works

data = [
    ("James", 18, "Student"),
    ('Tommy', 23, "Data Engineer"),
    ('Ronald', 34, "Chef"),
    ('Jessie', 26, "Teacher"),
    ('Mary', 30, "Chef")
]  # Replace with sample data
columns = ['name', 'age', 'job']  # Replace with column names
df = spark.createDataFrame(data, columns)  # Create DataFrame

# Show the DataFrame
# df.show()
df.show()

# =============================================================================
# TASK 2: Configuration Exploration
# =============================================================================

print("\n=== Task 2: Configuration ===")

configured_spark = SparkSession.builder \
    .appName("ConfiguredApp") \
    .config("spark.sql.shuffle.partitions", 50) \
    .config("spark.driver.memory", '2g') \
    .getOrCreate()

# TODO 2a: Print the value of spark.sql.shuffle.partitions
# HINT: Use spark.conf.get("spark.sql.shuffle.partitions")

print(f"Shuffle partitions: {configured_spark.conf.get('spark.sql.shuffle.partitions')}")  # Complete this


# TODO 2b: Print at least 3 other configuration values
# Some options: spark.driver.memory, spark.executor.memory, spark.sql.adaptive.enabled
print(f"Adaptive enabled: {configured_spark.conf.get('spark.sql.adaptive.enabled')}")
print(f"Driver port: {configured_spark.conf.get('spark.driver.port')}")
print(f"Deploy mode: {configured_spark.conf.get('spark.submit.deployMode')}")
print(f"Driver memory: {configured_spark.conf.get('spark.driver.memory')}")

# TODO 2c: Try changing spark.sql.shuffle.partitions at runtime
# Does it work? Add a comment explaining what happens.

# Your code here
configured_spark.conf.set('spark.sql.shuffle.partitions', 10)
print(f"New shuffle partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")
# It does work. The amount of shuffle partitions is updated.

# =============================================================================
# TASK 3: getOrCreate() Behavior
# =============================================================================

print("\n=== Task 3: getOrCreate() Behavior ===")

# TODO 3a: Create another reference using getOrCreate with a DIFFERENT app name
spark2 = SparkSession.builder.appName("DifferentName").config("spart.executor.cores", 4).getOrCreate()  # Replace with SparkSession.builder.appName("DifferentName").getOrCreate()


# TODO 3b: Check which app name is actually used
print(f"spark app name: {spark.sparkContext.appName}")
print(f"spark2 app name: {spark2.sparkContext.appName}")


# TODO 3c: Are spark and spark2 the same object? Check with 'is' operator
print(f"spark and spark2 are the same object: {spark is spark2}")
# Yes they are

# TODO 3d: EXPLAIN IN A COMMENT: Why does getOrCreate() behave this way?
# Your explanation:
# It behaves this way because spark sessions share a SparkContext, so to avoid  
# errors, it is best to also only have one SparkSession. The common way of making 
# SparkSessions is getOrCreate() which will use an existing SparkSession instead 
# of making a new one if one already exists. To make a new one there is a method
# you can call on the first SparkSession, spark.newSession().


# =============================================================================
# TASK 4: Session Cleanup
# =============================================================================

print("\n=== Task 4: Cleanup ===")

# TODO 4a: Stop the SparkSession properly
# HINT: Use spark.stop()
print(f"SparkSession is stopped (before): {spark.sparkContext._jsc.sc().isStopped()}")
sc = spark.sparkContext
spark.stop()
print(f"SparkSession is stopped: {sc._jsc is None and SparkSession.getActiveSession() is None}")
# TODO 4b: Verify the session has stopped
# HINT: Check spark.sparkContext._jsc.sc().isStopped() before stopping


# =============================================================================
# STRETCH GOALS (Optional)
# =============================================================================

# Stretch 1: Create a helper function that builds a SparkSession with your
# preferred default configurations

def create_my_spark_session(app_name, shuffle_partitions=100, enable_hive=False):
    """
    Creates a SparkSession with custom defaults.
    
    Args:
        app_name: Name of the Spark application
        shuffle_partitions: Number of shuffle partitions (default: 100)
    
    Returns:
        SparkSession object
    """
    # TODO: Implement this function
    if enable_hive:
        custom_spark = SparkSession.builder.appName(app_name) \
            .config('spark.sql.shuffle.partitions', shuffle_partitions) \
            .enableHiveSupport() \
            .getOrCreate()
    else:
        custom_spark = SparkSession.builder.appName(app_name) \
            .config('spark.sql.shuffle.partitions', shuffle_partitions) \
            .getOrCreate()
    
    print(spark.sparkContext.getConf().getAll())
    return custom_spark

#s = create_my_spark_session("Cool App")
# Stretch 2: Enable Hive support
# HINT: Use .enableHiveSupport() in the builder chain
# Note: This may fail if Hive is not configured - that's okay!


# Stretch 3: List all configuration options
# HINT: spark.sparkContext.getConf().getAll() returns all settings