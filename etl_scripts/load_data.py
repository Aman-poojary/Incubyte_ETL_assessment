from datetime import date, datetime
import psycopg2.extras
from etl_scripts import get_connection, release_connection
from data import get_country_name

def fill_country_tables(conn):
    # Fill country-specific tables with customer data based on their last consulted date.
    cursor = conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
    today = date.today()

    # Get the last processed id to determine which records need processing
    cursor.execute('SELECT COALESCE(MAX(id), 0) FROM staging WHERE processed = TRUE')
    last_processed_id = cursor.fetchone()[0]

    # Fetch new records from the staging table that haven't been processed yet
    cursor.execute('SELECT * FROM staging WHERE id > %s AND processed = FALSE', (last_processed_id,))
    new_records = cursor.fetchall()

    # Dictionary to hold data for bulk inserts into country-specific tables
    country_data = {}

    for record in new_records:
        # Calculate age and days since last consulted
        age = (today - record.dob).days // 365 if record.dob else None
        days_since_last_consulted = (today - record.last_consulted_date).days if record.last_consulted_date else None

        # Get the country code and associated country name
        country_code = record.country
        if country_code and country_code is not None:
            country_code = country_code.strip()
            country = get_country_name(country_code)
            if country != "Unknown Country":
                table_name = f"table_{country}"  # Use lowercase for table names
                
                # Prepare data for bulk insertion into the appropriate country table
                if table_name not in country_data:
                    country_data[table_name] = []

                # Append record to the corresponding country's data list
                country_data[table_name].append(
                    (record.customer_name, record.customer_id, record.open_date, record.last_consulted_date,
                     record.vaccination_id, record.dr_name, record.state, record.country, record.dob, record.is_active, age, days_since_last_consulted)
                )

    # Insert all records into their respective country tables
    for table_name, records in country_data.items():
        cursor.executemany(f'''INSERT INTO {table_name} (Customer_Name, Customer_Id, Open_Date, Last_Consulted_Date, Vaccination_Id, Dr_Name, State, Country, DOB, Is_Active, Age, Days_Since_Last_Consulted) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''', records)

    # Mark all processed rows at once for efficiency
    processed_ids = [record.id for record in new_records]
    if processed_ids:
        cursor.execute('UPDATE staging SET processed = TRUE WHERE id = ANY(%s)', (processed_ids,))

    conn.commit()  # Commit all changes to the database

def load_customer_current_country(conn):
    # """Load the current_country table to store the most recent country for each customer."""
    cursor = conn.cursor()
    
    # Insert or update current country information based on the most recent consultation date
    cursor.execute('''
    WITH ranked_customers AS (
    SELECT
        Customer_Id,
        Customer_Name,
        Country,
        Last_Consulted_Date,
        ROW_NUMBER() OVER (
            PARTITION BY Customer_Id 
            ORDER BY Last_Consulted_Date DESC NULLS LAST
        ) AS rn
    FROM
        staging
    WHERE
        processed = FALSE
    AND
        Last_Consulted_Date IS NOT NULL
    AND 
        Country IS NOT NULL
    )
    INSERT INTO current_country (Customer_Id, Customer_Name, Country, Last_Consulted_Date)
    SELECT
        Customer_Id,
        Customer_Name,
        Country,
        Last_Consulted_Date
    FROM
        ranked_customers
    WHERE
        rn = 1
    ON CONFLICT (Customer_Id)
    DO UPDATE SET
        Customer_Name = EXCLUDED.Customer_Name,
        Country = EXCLUDED.Country,
        Last_Consulted_Date = EXCLUDED.Last_Consulted_Date;
    ''')

    conn.commit()

def main(): 
    """Main function to execute the data loading process."""
    conn = get_connection()  # Get a database connection from the pool

    try:
        load_customer_current_country(conn) # Load customer data into current country table
        fill_country_tables(conn)  # Load customer data into country tables
    except Exception as e:
        print(f"An error occurred: {e}")  # Log any errors
        conn.rollback()  # Rollback any changes in case of errors
    finally:
        release_connection(conn)  # Release the connection back to the pool

    print("------Data processing completed successfully.")

if __name__ == "__main__":
    main()
