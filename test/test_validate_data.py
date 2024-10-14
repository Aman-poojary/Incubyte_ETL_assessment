import unittest
import pandas as pd
from io import StringIO
from unittest.mock import patch, mock_open, MagicMock, call
from etl_scripts import preprocess_data, copy_data_to_staging, create_staging_table_with_indexes

class TestValidateData(unittest.TestCase):

    def setUp(self):
        # Sample data similar to your test file
        self.sample_data = StringIO("|H|Customer_Name|Customer_Id|Open_Date|Last_Consulted_Date|Vaccination_Id|Dr_Name|State|Country|DOB|Is_Active\n"
                                    "|D|Emily|100007|20101012|20221001|MVD|Sam|QLD|AU|11111992|A\n"
                                    "|D|Emma|100008|20101012|20220105|MVD|Paul|FL|USA|24111995|A\n"
                                    "|D|||20101012|20211013|MVD|Paul|CA|USA|06031987|A\n"
                                    )
        # Expected DataFrame structure from sample_data
        self.expected_df = pd.DataFrame({
            'Customer_Name': ['Emily', 'Emma', None],
            'Customer_Id': ['100007', '100008', None],
            'Open_Date': ['20101012', '20101012', '20101012'],
            'Last_Consulted_Date': ['20221001', '20220105', '20211013'],
            'Vaccination_Id': ['MVD', 'MVD', 'MVD'],
            'Dr_Name': ['Sam', 'Paul', 'Paul'],
            'State': ['QLD', 'FL', 'CA'],
            'Country': ['AU', 'USA', 'USA'],
            'DOB': ['11111992', '24111995', '06031987'],
            'Is_Active': ['A', 'A', 'A']
        })    

    @patch('builtins.open', new_callable=mock_open)
    @patch('pandas.read_csv')
    def test_preprocess_data(self, mock_read_csv, mock_file):
        # Mock the CSV read operation to return expected DataFrame
        mock_read_csv.return_value = self.expected_df

        # Call the function
        preprocessed_df, cleaned_file_path, unique_countries = preprocess_data('dummy_path.txt')

        # Check if the preprocessing returned the correct unique countries
        self.assertEqual(set(unique_countries), {'AU', 'USA'})

        # Assertions
        self.assertEqual(cleaned_file_path, 'data/cleaned_customer_data.csv')
        self.assertEqual(set(unique_countries), {'USA', 'AU'})
    
        # Check if invalid data was removed
        self.assertEqual(len(preprocessed_df), 2)

        # Check if dates were converted correctly
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(preprocessed_df['Open_Date']))
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(preprocessed_df['Last_Consulted_Date']))
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(preprocessed_df['DOB']))

        # Check if Customer_Id length is correct
        self.assertTrue(all(preprocessed_df['Customer_Id'].str.len() <= 18))

    def test_invalid_header(self):
        invalid_data = StringIO('''Name|ID|OpenDate|LastConsultedDate|VaccinationId|Doctor|State|Country|DateOfBirth|Active
                                   John Doe|CUST001|20220101|20230601|VAC01|Dr. Smith|CA|USA|19900315|A
                                ''')
        with patch('pandas.read_csv', return_value=pd.read_csv(invalid_data, sep='|')):
            with self.assertRaises(ValueError):
                preprocess_data('dummy_path.txt')


    @patch('etl_scripts.validate_data.open')
    @patch('etl_scripts.validate_data.get_connection')
    def test_copy_data_to_staging(self, mock_get_connection, mock_open):
        # Mock file open
        mock_file = MagicMock()
        mock_open.return_value.__enter__.return_value = mock_file
        
        # Mock database connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_connection.return_value = mock_conn

        # Call the function
        cleaned_file_path = 'data/cleaned_customer_data.csv'
        copy_data_to_staging(mock_conn, cleaned_file_path)

        # Assert the COPY command was executed
        mock_cursor.copy_expert.assert_called_once_with(
            'COPY staging (Customer_Name, Customer_Id, Open_Date, Last_Consulted_Date, Vaccination_Id, Dr_Name, State, Country, DOB, Is_Active) FROM STDIN WITH (FORMAT CSV, DELIMITER \'|\', HEADER)',
            mock_file
        )
        mock_conn.commit.assert_called_once()

    @patch('etl_scripts.validate_data.get_connection')
    def test_create_staging_table_with_indexes(self, mock_get_connection):
        # Mock database connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_connection.return_value = mock_conn

        # Call the function
        create_staging_table_with_indexes(mock_conn)

        # Assert that the table creation and index creation were executed
        mock_cursor.execute.assert_has_calls([
            call('''CREATE TABLE IF NOT EXISTS staging (
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
    )'''),
            call('CREATE INDEX IF NOT EXISTS idx_last_consulted_date ON staging (Last_Consulted_Date);')
        ])

        mock_conn.commit.assert_called_once()

