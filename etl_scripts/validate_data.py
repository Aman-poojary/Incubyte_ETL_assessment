import pandas as pd
from psycopg2 import pool
from data import country_codes, get_country_name

# Database connection pooling
db_params = {
    "host": "localhost",
    "port": 5432,
    "database": "my_database",
    "user": "admin",
    "password": "admin"
}

# Create a connection pool for efficient DB connection management
db_pool = pool.SimpleConnectionPool(1, 20, **db_params)

def get_connection():
    """Get a connection from the pool."""
    return db_pool.getconn()

def release_connection(conn):
    """Release a connection back to the pool."""
    db_pool.putconn(conn)

def validate_header(df):
    """Validate if the DataFrame contains the expected header columns."""
    expected_header = ['Customer_Name', 'Customer_Id', 'Open_Date', 'Last_Consulted_Date', 
                       'Vaccination_Id', 'Dr_Name', 'State', 'Country', 'DOB', 'Is_Active']
    
    # Access the columns of the DataFrame
    header = df.columns.tolist()
    # Check if the header matches the expected header
    if header != expected_header:
        raise ValueError("Invalid header")

def preprocess_data(file_path):
    """Preprocess the input CSV file, clean data, and handle invalid/missing values."""
    # Read CSV into a pandas DataFrame
    df = pd.read_csv(file_path, delimiter='|', dtype={'Customer_Id': str})

    # Drop unnecessary columns like 'Unnamed: 0' and 'H' (if they exist)
    columns_to_drop = [col for col in ['Unnamed: 0', 'H'] if col in df.columns]
    df.drop(columns=columns_to_drop, inplace=True)
    
    # Validate if the header is correct
    validate_header(df)

    # Define the mandatory columns
    mandatory_columns = ['Customer_Name', 'Customer_Id', 'Open_Date']

    # Keep rows where mandatory columns are present
    valid_data = df.dropna(subset=mandatory_columns)  
    
    # Ensure Customer_Id length does not exceed 18 characters
    valid_data = valid_data[valid_data['Customer_Id'].str.len() <= 18]

    # Reformat and convert 'DOB' from DDMMYYYY to YYYYMMDD, and then to datetime
    valid_data['DOB'] = (
        valid_data['DOB'].str.slice(4, 8) +  # Year (YYYY)
        valid_data['DOB'].str.slice(2, 4) +  # Month (MM)
        valid_data['DOB'].str.slice(0, 2)    # Day (DD)
    )

    # Convert 'Open_Date','Last_Consulted_Date', 'DOB' to datetime
    date_columns = ['Open_Date', 'Last_Consulted_Date', 'DOB']
    valid_data[date_columns] = valid_data[date_columns].apply(pd.to_datetime, format='%Y%m%d', errors='coerce')

    #Drop rows with invalid dates
    valid_data = valid_data.dropna(subset=['Open_Date'])

    # Replace empty strings with None, and handle NaT values
    valid_data.replace("", None, inplace=True)  # Convert empty strings to None
    valid_data = valid_data.where(valid_data.notna(), None)  # Convert NaT (and any other NaN) to None

    #Covert the datatype from object to datetime
    valid_data['Open_Date'] = valid_data['Open_Date'].astype('datetime64[ns]')
    valid_data['Last_Consulted_Date'] = valid_data['Last_Consulted_Date'].astype('datetime64[ns]')
    valid_data['DOB'] = valid_data['DOB'].astype('datetime64[ns]')

    # Save the cleaned and formatted data to a new CSV file for use with the COPY command
    cleaned_file_path = 'data/cleaned_customer_data.csv'
    valid_data.to_csv(cleaned_file_path, sep='|', index=False)
    
    return valid_data, cleaned_file_path, valid_data['Country'].unique()  # Return cleaned data path and unique countries

def copy_data_to_staging(conn, cleaned_file_path):
    with open(cleaned_file_path, 'r') as f:
        cursor = conn.cursor()
        # Use the COPY command to load data efficiently into PostgreSQL
        cursor.copy_expert('COPY staging (Customer_Name, Customer_Id, Open_Date, Last_Consulted_Date, Vaccination_Id, Dr_Name, State, Country, DOB, Is_Active) FROM STDIN WITH (FORMAT CSV, DELIMITER \'|\', HEADER)', f)
    conn.commit()

def create_staging_table_with_indexes(conn):
    """Create the staging table and add indexes for performance optimization."""
    cursor = conn.cursor()
    # Create the staging table if it does not exist
    cursor.execute('''CREATE TABLE IF NOT EXISTS staging (
        id SERIAL PRIMARY KEY,
        Customer_Name VARCHAR(255) NOT NULL,
        Customer_Id VARCHAR(18) NOT NULL,
        Open_Date DATE NOT NULL,
        Last_Consulted_Date DATE,
        Vaccination_Id CHAR(5),
        Dr_Name VARCHAR(255),
        State CHAR(5),
        Country CHAR(5),
        DOB DATE,
        Is_Active CHAR(1),
        processed BOOLEAN DEFAULT FALSE
    )''')

    # Create indexes for performance on frequently queried columns
    cursor.execute('''CREATE INDEX IF NOT EXISTS idx_last_consulted_date ON staging (Last_Consulted_Date);''')
    
    conn.commit()

def create_country_tables(conn, countries):
    # """Create country-specific tables based on the unique country codes."""
    cursor = conn.cursor()
    for country_code in countries:
        if country_code in country_codes:
            country_name = get_country_name(country_code)
            table_name = f"Table_{country_name}"
            cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id SERIAL PRIMARY KEY,
                Customer_Id VARCHAR(18) NOT NULL,
                Customer_Name VARCHAR(255) NOT NULL,
                Open_Date DATE NOT NULL,
                Last_Consulted_Date DATE,
                Vaccination_Id CHAR(5),
                Dr_Name VARCHAR(255),
                State CHAR(5),
                Country CHAR(5),
                DOB DATE,
                Is_Active CHAR(1),
                Age INTEGER,
                Days_Since_Last_Consulted INTEGER
            )
            ''')
    conn.commit()

def create_customer_current_country(conn):
    # """Create the current_country table to store the most recent country for each customer."""
    cursor = conn.cursor()

    # Create the table for storing current country information
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS current_country (
        Customer_Id VARCHAR(18) PRIMARY KEY,
        Customer_Name VARCHAR(255),
        Country VARCHAR(5),
        Last_Consulted_Date DATE
    )
    ''')
    conn.commit()

def main():
    conn = get_connection()  # Get a database connection from the pool
    try:
        file_path = 'data/customer_data.txt'
        
        # Preprocess data and get valid cleaned file path and unique country list
        valid_df, cleaned_file_path, unique_countries = preprocess_data(file_path)
        
        # Create staging table with indexes
        create_staging_table_with_indexes(conn)
        
        # Load valid data into the staging table
        copy_data_to_staging(conn, cleaned_file_path)

        # Create country-specific tables based on the unique countries in the valid data
        create_country_tables(conn, unique_countries)

        # Update the current_country table with the most recent country data for each customer
        create_customer_current_country(conn)
    except Exception as e:
        print(f"An error occurred: {e}")  # Log any errors
        conn.rollback()  # Rollback any changes in case of errors
    finally:
        release_connection(conn)  # Release the connection back to the pool

    print("------Data Validation Completed Successfully.")

if __name__ == "__main__":
    main()
