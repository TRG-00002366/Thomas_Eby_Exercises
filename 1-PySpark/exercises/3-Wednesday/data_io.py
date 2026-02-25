from pyspark import SparkContext

sc = SparkContext("local[*]", "DataIO")

## TASK 1
print("TASK 1\n")

# Load the CSV file
lines = sc.textFile("1-PySpark/exercises/3-Wednesday/sales_data.csv")

# Skip header line
header = lines.first()
data = lines.filter(lambda line: line != header)

print(f"Header: {header}")
print(f"Data records: {data.count()}")
print(f"First record: {data.first()}")


## TASK 2
print("\nTASK 2\n")

def parse_record(line):
    """Parse CSV line into structured data."""
    parts = line.split(",")
    return {
        "product_id": parts[0],
        "name": parts[1],
        "category": parts[2],
        "price": float(parts[3]),
        "quantity": int(parts[4])
    }

# Parse all records
parsed = data.map(parse_record)

# Show parsed data
for record in parsed.take(3):
    print(record)


## TASK 3
print("\nTASK 3\n")

# Calculate revenue for each product
revenue = parsed.map(lambda r: f"{r['product_id']},{r['name']},{r['price'] * r['quantity']:.2f}")

# Save to output directory
# YOUR CODE: Use saveAsTextFile to save revenue data
try:
    revenue.saveAsTextFile("1-PySpark/exercises/3-Wednesday/output")
    print("Data saved to output directory")
except Exception as e:
    if "FileAlreadyExistsException" in str(e):
        print("output already exists")
    else:
        print("Something went wrong saving revenue as text files")


## TASK 4
print("\nTASK 4\n")

all_data = sc.textFile("1-PySpark/exercises/3-Wednesday/sales_data*.csv")
header = all_data.first()

filtered_all_data = all_data.filter(lambda line: line != header)
print(filtered_all_data.collect())


## TASK 5 
try:
    filtered_all_data.coalesce(1).saveAsTextFile("1-PySpark/exercises/3-Wednesday/outputSingle")
    print("Data saved to outputSingle directory")
except Exception as e:
    if "FileAlreadyExistsException" in str(e):
        print("outputSingle already exists")
    else:
        print("Something went wrong saving filtered_all_data as a text file")

sc.stop()