from pyspark.sql import SparkSession, functions

def main():
    # Step 1: Create SparkSession
    spark = SparkSession.builder \
        .appName("MyFirstJob") \
        .master("local[*]") \
        .getOrCreate() 
        
    # Step 2: Create some data
    ## Sample data: (product, category, price, quantity)
    sales_data = [
        ("Laptop", "Electronics", 999.99, 5),
        ("Mouse", "Electronics", 29.99, 50),
        ("Desk", "Furniture", 199.99, 10),
        ("Chair", "Furniture", 149.99, 20),
        ("Monitor", "Electronics", 299.99, 15),
    ]

    # Create DataFrame with column names
    df = spark.createDataFrame(sales_data, ["product", "category", "price", "quantity"])

    # Step 3: Perform transformations
    ## Count total records: Print the total number of products
    count = df.count()
    
    ## Calculate total revenue per product: Add a column revenue = price * quantity
    revenue_df = df.withColumn("revenue", df["price"] * df["quantity"])

    ## Filter by category: Show only Electronics products
    electronics_df = df.filter(df["category"] == "Electronics")

    ## Aggregate by category: Calculate total revenue per category
    total_revenue_df = revenue_df.groupBy("category").agg(functions.round(functions.sum("revenue"), 2).alias("total_revenue")) # Rounding since my furnature total_revanue was comming in at 4999.700000000001

    # Step 4: Show results
    df.show()
    
    print("Total products: ", count, end='\n\n')
    
    print("Revenue per product")
    revenue_df.show()
    
    print("Electronics only:")
    electronics_df.show()

    print("Revenue by category:")
    total_revenue_df.show()

    # Step 5: Clean up
    spark.stop()

if __name__ == "__main__":
    main()