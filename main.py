from etl_scripts import validate_main, load_main

if __name__ == "__main__":
    try:
        print("*Starting ETL process*")
        print("------Starting Data Validation")
        validate_main()
        
        print("------Starting Data Loading")
        load_main()

    except Exception as e:
        print(f"An error occurred during the testing process: {e}")
    finally:
        print("*ETL Process Completed*")
