# Customer Data Processing Pipeline

## Project Overview

This project implements a data processing pipeline that takes customer data in a CSV file format, processes the data for validation, and loads it into PostgreSQL. The pipeline uses Python, pandas, and PostgreSQL to:
1. Validate and clean the data.
2. Insert the data into a staging table.
3. Create country-specific tables and populate them with customer data.
4. Identify each customer's current country based on their most recent consultation date and update a summary table (`current_country`).

The pipeline is designed to handle large datasets efficiently by utilizing techniques such as batching, connection pooling, and indexing. Docker is used to run the PostgreSQL database.

## Features
- Data Validation: Ensures that the necessary columns (Customer_Name, Customer_Id, and Open_Date) are present and correctly formatted.
- Staging Table: All data is initially loaded into a staging table for further processing.
- Country-Specific Tables: For each country in the data, a separate table is created to store customer information, and customer data is inserted based on the country they belong to.
- Most Recent Country: The pipeline identifies the most recent country a customer was consulted in and updates this information in a `current_country` table.
- Handling Large Data: Implements efficient handling of large datasets using pandas, batch inserts with `executemany`, and connection pooling in PostgreSQL.

## Requirements

- Python 3.x
- Docker
- PostgreSQL
- pandas
- psycopg2 (PostgreSQL adapter for Python)

## Setup Instructions

1. Run PostgreSQL using Docker:
   
   If you don't already have PostgreSQL installed, you can run it using Docker:
  
   docker run --name my-postgres -e POSTGRES_USER=admin -e POSTGRES_PASSWORD=admin -e POSTGRES_DB=my_database -p 5432:5432 -d postgres
   
2. Install Required Python Libraries:
   
   Install the necessary libraries by running:
  
   pip install pandas psycopg2
   
3. Database Schema:
   
   The project creates a `staging` table for raw data ingestion, country-specific tables (e.g., `Table_USA`, `Table_Australia`), and a `current_country` table to store each customer’s most recent country information.

4. Database Connection:
   
   The database connection is managed using connection pooling for efficiency, especially when dealing with large datasets. This ensures multiple database connections are handled efficiently.

## Code Structure

### File 1: `validate_data.py` (Data Validation and Preprocessing)
This script is responsible for reading and validating the input data from a CSV file, formatting the dates, and handling invalid data.

- validate_header(df): Ensures the input file has the correct headers.
- preprocess_data(file_path): Cleans the data by:
  - Stripping whitespace.
  - Converting dates into the correct format.
  - Dropping rows with invalid or missing mandatory data.
  - Writing valid data to a cleaned CSV file for further processing.
  
- copy_data_to_staging(conn, cleaned_file_path): Loads the cleaned data into the staging table using the PostgreSQL `COPY` command for fast bulk insert.

### File 2: `load_data.py` (Data Loading and Processing)
This script processes the data from the staging table and inserts it into country-specific tables, and the `current_country` table.

- fill_country_tables(conn):
  - Fetches unprocessed data from the staging table.
  - Inserts customer records into country-specific tables.
  - Marks the records as processed.

- customer_current_country(conn):
  - Identifies the most recent `Last_Consulted_Date` for each customer and inserts or updates their current country in the `current_country` table using a window function (`ROW_NUMBER()`) to get the most recent date.

### Efficiency Improvements
- Batch Inserts: Instead of inserting one row at a time, `executemany()` is used to insert data in batches, reducing the number of round trips to the database.
- Indexes: Indexes are added to the `staging` table on columns like `DOB`, `Last_Consulted_Date`, and `processed` to speed up query execution.
- Connection Pooling: `psycopg2.pool.SimpleConnectionPool` is used to manage multiple database connections efficiently.

### SQL Queries

- Inserting into Country-Specific Tables:

  Each country has its own table. The records from the staging table are distributed based on the `Country` column, and additional fields like `Age` and `Days_Since_Last_Consulted` are calculated during the insertion.

- Updating the `current_country` Table:

  The most recent country for each customer is determined using a query that ranks rows by `Last_Consulted_Date`:
  
## Running the Project

1. Step 1: Start PostgreSQL in Docker.
   
   docker start my-postgres
   
2. Step 2: Run the `main.py` script to preprocess and load data into the staging table and then to populate country-specific tables and update the `current_country` table:
   
   python main.py
   
3. Step 3: Run the `test.py` script  for testing:
  
   python test.py
   
4. Step 4: Verify the results by querying the database.

## Conclusion

This project demonstrates the efficient handling of large datasets by:
- Validating and cleaning input data.
- Utilizing connection pooling for database efficiency.
- Using batch inserts and indexes to optimize PostgreSQL performance.
- Implementing window functions and CTEs to handle complex data transformations.

The setup is scalable and can be used to process millions of records in a performant way.