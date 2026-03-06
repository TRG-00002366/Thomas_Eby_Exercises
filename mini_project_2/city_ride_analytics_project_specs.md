# Project Spec: City Ride Analytics (PySpark DataFrame Edition)

## 1. Project Overview
In this project, you will act as a Data Engineer for **"Metro Data Cab Co."** — a fictional ride-sharing company. Your task is to analyze the company's ride and driver datasets using PySpark's **DataFrame API** and **Spark SQL**. You will demonstrate your mastery of DataFrame operations, column manipulations, aggregations, joins, set operations, and SQL queries to extract actionable business insights.

---

## 2. Learning Objectives
By the end of this project, students will be able to:
- Create DataFrames from CSV files and inspect schemas.
- Select specific columns and filter rows using conditions.
- Add, rename, and remove columns for data shaping.
- Perform aggregations (`sum`, `avg`, `count`, `max`, `min`) using `groupBy`.
- Join multiple DataFrames to enrich datasets.
- Apply set operations (`union`) to combine DataFrames.
- Write and execute Spark SQL queries using temporary views.
- Sort, handle nulls, and write conditional logic on DataFrames.
- Save processed results in CSV and Parquet formats.

---

## 3. Data Source
- **Rides File:** `rides.csv` — Contains details of each ride (fare, distance, location, etc.).
- **Drivers File:** `drivers.csv` — Contains driver information (name, vehicle, experience).
- **Format:** Comma-Separated Values (CSV) with headers.

### 3.1 Schema: `rides.csv`

| Column            | Type   | Description                              |
|-------------------|--------|------------------------------------------|
| ride_id           | int    | Unique ride identifier                   |
| driver_id         | int    | Foreign key linking to `drivers.csv`     |
| pickup_location   | string | Neighborhood where the ride started      |
| dropoff_location  | string | Neighborhood where the ride ended        |
| distance_miles    | double | Total distance of the ride               |
| fare_amount       | double | Total fare charged to the passenger      |
| ride_date         | string | Date of the ride (YYYY-MM-DD)            |
| ride_type         | string | Type of ride: `standard`, `premium`, `pool` |
| rating            | double | Passenger rating (1.0 – 5.0), may be null |

### 3.2 Schema: `drivers.csv`

| Column        | Type   | Description                                  |
|---------------|--------|----------------------------------------------|
| driver_id     | int    | Unique driver identifier                     |
| driver_name   | string | Full name of the driver                      |
| vehicle_type  | string | Vehicle category: `sedan`, `suv`, `hatchback`|
| license_year  | int    | Year the driver obtained their license       |

---

## 4. Requirements: DataFrame Operations & Spark SQL (12 Tasks)

### T1: Create DataFrame from CSV
**Requirement:** Load `rides.csv` and `drivers.csv` into PySpark DataFrames with header and schema inference enabled.
- **Example:** Like opening a spreadsheet that automatically recognizes column names and data types from the first row.

### T2: Display Schema & Preview
**Requirement:** Print the schema of both DataFrames and display the first 5 rows.
- **Example:** Checking the column headers and a few sample rows before starting any analysis, like skimming the first page of a report.

### T3: Column Selection
**Requirement:** Select only `ride_id`, `pickup_location`, `dropoff_location`, and `fare_amount` columns from the rides DataFrame.
- **Example:** Highlighting only the columns you need in a spreadsheet and hiding the rest.

### T4: Filtering Rides
**Requirement:** Filter rides where `distance_miles > 5.0` AND `ride_type == "premium"`.
- **Example:** Asking a dispatcher: "Show me only the long premium rides" — they pull out just those records from the filing cabinet.

### T5: Adding a Derived Column — Fare Per Mile
**Requirement:** Add a new column `fare_per_mile` calculated as `fare_amount / distance_miles`.
- **Example:** A manager adding a "Unit Price" column to a purchase order by dividing total cost by quantity.

### T6: Removing Columns
**Requirement:** Drop the `ride_type` column from the rides DataFrame.
- **Example:** Tearing out a page from a notebook that you no longer need for your analysis.

### T7: Renaming Columns
**Requirement:** Rename `pickup_location` to `start_area` and `dropoff_location` to `end_area`.
- **Example:** Relabeling folders in a filing system from technical codes to human-friendly names.

### T8: Aggregation — Total Revenue by Ride Type
**Requirement:** Group rides by `ride_type` and calculate the total `fare_amount` for each type.
- **Example:** A cashier sorting receipts into "Standard," "Premium," and "Pool" piles, then totaling the amounts in each pile.

### T9: Aggregation — Average Rating per Driver
**Requirement:** Group rides by `driver_id` and compute the average `rating` for each driver.
- **Example:** A restaurant review site computing each restaurant's average star rating from all customer reviews.

### T10: Join — Enrich Rides with Driver Info
**Requirement:** Perform an **inner join** between the rides and drivers DataFrames on `driver_id` to create a combined dataset.
- **Example:** Matching each order slip to the delivery person's profile card by their employee ID.

### T11: Set Operations — Combine Peak & Off-Peak Rides
**Requirement:** Filter rides from January 2025 into a "peak" DataFrame and rides from February 2025 into an "off-peak" DataFrame, then **union** them into a single combined DataFrame.
- **Example:** Merging two separate attendance sheets (one for morning, one for afternoon) into one master list.

### T12: Spark SQL Queries
**Requirement:** Register the rides DataFrame as a temporary SQL view and execute an SQL query to find the **top 3 highest-fare rides** along with their pickup and dropoff locations.
- **Example:** Typing a SQL query into a database console: `SELECT * FROM rides ORDER BY fare_amount DESC LIMIT 3`.

---

## 5. Additional Requirements (5 Tasks)

### O1: Multi-Column Sorting
**Requirement:** Sort the rides DataFrame by `fare_amount` in descending order, then by `distance_miles` in ascending order.
- **Example:** Sorting a class roster first by grade (highest first) and then alphabetically by name within each grade.

### O2: Handling Nulls
**Requirement:** Count how many rides have a `null` rating, then fill those null values with `0.0`.
- **Example:** A teacher noticing blank cells in a grade sheet and writing "0" in each one so the average calculation doesn't break.

### O3: Conditional Column — Ride Category
**Requirement:** Add a column `ride_category` that labels each ride as `"short"` (< 3 miles), `"medium"` (3–8 miles), or `"long"` (> 8 miles) based on `distance_miles`.
- **Example:** A shipping company stamping each package as "Small," "Medium," or "Large" based on its weight.

### O4: Window Function — Running Revenue Total (Stretch Goal)
**Requirement:** Using a Window function, compute a running total of `fare_amount` ordered by `ride_date` for each driver.
- **Example:** A salesperson's cumulative commission tracker that keeps adding each new sale to the running balance.

### O5: Saving Results
**Requirement:** Save the joined (enriched) DataFrame as a **Parquet** file in a directory named `Ride_Analytics_Results`, and save the aggregation results as a **CSV** file.
- **Example:** Exporting a finished report as both a PDF (for archival) and an Excel file (for further editing).

---

## 6. Evaluation Criteria
- **Correctness:** Does the logic correctly operate on the ride-share dataset?
- **DataFrame Best Practices:** Are built-in DataFrame functions used instead of converting to RDDs?
- **SQL Usage:** Are Spark SQL queries written correctly using temporary views?
- **Schema Awareness:** Are column types handled properly (e.g., nulls, type casting)?
- **Documentation:** Are the code blocks commented to explain each transformation and its purpose?