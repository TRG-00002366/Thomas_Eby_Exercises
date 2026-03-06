from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, round, sum as spark_sum, avg as spark_avg, month, coalesce, lit, when

def create_spark_session():
    return SparkSession.builder \
        .appName("Ride Analytics") \
        .master("local[*]") \
        .getOrCreate()

# T1: load datasets into dataframes including the header and inferring the schema
def load_datasets(spark, path):
    drivers_df = spark.read.csv(path + "/drivers.csv", header=True, inferSchema=True)
    rides_df = spark.read.csv(path + "/rides.csv", header=True, inferSchema=True)
    return drivers_df, rides_df

# T2: display schema and first 5 rows
def display_schema_and_preview(drivers_df, rides_df):
    print("\nDrivers:\n")
    drivers_df.printSchema()
    drivers_df.show(5)
    print("\nRides:\n")
    rides_df.printSchema()
    rides_df.show(5)

# T3: select only ride_id, pickup_location, dropoff_location, and fare_amount from rides_df
def column_selection(rides_df):
    return rides_df.select(col('ride_id'), col('pickup_location'), col('dropoff_location'), col('fare_amount'))

# T4: filter rides to only include premium rides that were over 5 miles in distance 
def filter_rides(rides_df):
    return rides_df.filter((col('distance_miles') > 5.0) & (col('ride_type') == 'premium'))

# T5: add a column for fare per mile
def fare_per_mile(rides_df):
    return rides_df.withColumn('fair_per_mile', round(col('fare_amount') / col('distance_miles'), 2))

# T6: drop the 'ride_type' column
def drop_ride_type(rides_df):
    return rides_df.drop('ride_type')

# T7: rename 'pickup_location' to 'start_area' and rename 'dropoff_location' to 'end_area'
def rename_columns(rides_df):
    return rides_df.withColumnsRenamed({'pickup_location': 'start_area', 'dropoff_location': 'end_area'})

# T8: group rides by their type and calculate a total fare amount for each
def total_fare_by_type(rides_df):
    return rides_df.groupBy('ride_type').agg(spark_sum('fare_amount').alias('total_fare_amount'))

# T9: group rides by driver id and compute each driver's average rating
def avg_rating_by_driver(rides_df):
    return rides_df.groupBy('driver_id').agg(round(spark_avg('rating'), 1).alias('avg_rating'))

# T10: join rides_df and drivers_df to get the driver information associated with each ride
def join_data(rides_df, drivers_df):
    return rides_df.join(drivers_df, 'driver_id', 'inner')

# T11: filter for rides during January, referred to as peak_rides_df, and filter for rides during February, referred to as off_peak_rides_df
#      union these DataFrames together
def peak_and_offpeak_rides(rides_df):
    peak_rides_df = rides_df.filter(month(col('ride_date')) == "1")
    off_peak_rides_df = rides_df.filter(month(col('ride_date')) == "2")
    return peak_rides_df.union(off_peak_rides_df)

# T12: find the top 3 highest-fare rides along with their pickup and dropoff locations using SQL query
#      register rides_df as a temporary view first so that it is available for querying
def sql_three_highest_fares(spark, rides_df):
    rides_df.createOrReplaceTempView('rides')
    return spark.sql("""
                SELECT fare_amount, pickup_location, dropoff_location
                FROM rides
                ORDER BY fare_amount
                LIMIT 3;
              """)

# O1: sort the rides by the fare_amount in reverse order and break ties with distance_miles in ascending order
def sort_rides(rides_df):
    return rides_df.sort(col('fare_amount').desc(), col('distance_miles'))

# O2: count the null values in the rating column and replace them with 0.0 
def count_and_replace_null_ratings(rides_df):
    count = rides_df.filter(col('rating').isNull()).count()
    replaced_df = rides_df.withColumn('rating', coalesce(col('rating'), lit(0.0)))
    return count, replaced_df

# O3: add 'ride_category' column to classify rides with a distance under 3 miles as 'short', between 3 and 8 miles as 'medium', and above 8 miles as 'long'
def with_ride_category(rides_df):
    return rides_df.withColumn('ride_category', 
                               when(col('distance_miles') < 3, 'short')
                               .when(col('distance_miles') < 8, 'medium')
                               .otherwise('long')
                               )

# O4: use a window function add a column with a running total of fare_amount for each driver
def running_fare_amount(rides_df):
    window = Window.partitionBy('driver_id').orderBy('ride_date').rowsBetween(Window.unboundedPreceding, Window.currentRow)
    return rides_df.withColumn('total_fare_amount', spark_sum('fare_amount').over(window))

# O5: save the joined DataFrame as a Parquet file in a directory named "Ride_Analytics_Results"
def save_joined_and_aggregated(rides_df, drivers_df, path):
    join_data(rides_df, drivers_df).coalesce(1).write.mode("overwrite").parquet(path + "/Ride_Analytics_Results")
    avg_rating_by_driver(rides_df).coalesce(1).write.option('header', 'true').mode('overwrite').csv(path + "/Ride_Analytics_Results/avg_ratings/")
    total_fare_by_type(rides_df).coalesce(1).write.option('header', 'true').mode('overwrite').csv(path + "/Ride_Analytics_Results/total_fares/")

def main():
    spark = create_spark_session()
    # T1
    drivers_df, rides_df = load_datasets(spark, "mini_project_2/data")
    # T2
    display_schema_and_preview(drivers_df, rides_df)
    # T3
    print("\nSubset of columns from rides_df:")
    column_selection(rides_df).show(5)
    # T4
    print("\nPremium rides over 5 miles:")
    filter_rides(rides_df).show(5)
    # T5
    print("\nFare per Mile:")
    fare_per_mile(rides_df).show(5)
    # T6
    print("\nRides without ride_type:")
    drop_ride_type(rides_df).show(5)
    # T7
    print("\nRides with nicer names:")
    rename_columns(rides_df).show(5)
    # T8
    print("\nTotal fare amount by type:")
    total_fare_by_type(rides_df).show(5)
    # T9
    print("\nAverage rating by driver:")
    avg_rating_by_driver(rides_df).show(5)
    # T10
    print("\nJoined data:")
    join_data(rides_df, drivers_df).show(5)
    # T11
    print("\nPeak and off-peak rides:")
    peak_and_offpeak_rides(rides_df).show(5)
    # T12
    print("\nTop 3 highest fares:")
    sql_three_highest_fares(spark, rides_df).show()
    # O1
    print("\nRides sorted by fare_amount DESC, distance_miles ASC:")
    sort_rides(rides_df).show(5)
    # O2
    print("\nNull rating count and table with null ratings set to 0.0:")
    count, table = count_and_replace_null_ratings(rides_df)
    print(f"Count: {count}")
    table.show(5)
    # O3
    print("\nRides with ride_category:")
    with_ride_category(rides_df).show(5)
    # O4
    print("\nRunning total of fare amounts per driver:")
    running_fare_amount(rides_df).show(5)
    # O5
    save_joined_and_aggregated(rides_df, drivers_df, "mini_project_2")
    print("Check /Ride_Analytics_Results for the output files")
    spark.stop()
    

if __name__ == "__main__":
    main()