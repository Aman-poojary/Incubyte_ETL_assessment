import unittest
from unittest.mock import patch, MagicMock
from etl_scripts import load_customer_current_country

class TestLoadData(unittest.TestCase):   
    @patch('etl_scripts.load_data.get_connection')
    @patch('etl_scripts.load_data.release_connection')
    def test_load_customer_current_country(self, mock_release_connection, mock_get_connection):
        # Setup mock connection
        mock_conn = MagicMock()
        mock_get_connection.return_value = mock_conn

        # Mock the execute method
        mock_conn.cursor.return_value.execute.return_value = None

        # Call the function
        load_customer_current_country(mock_conn)

        # Assert that the insert/update was executed correctly
        mock_conn.cursor.return_value.execute.assert_called_once()
        self.assertIn("INSERT INTO current_country", str(mock_conn.cursor.return_value.execute.call_args[0][0]))

        mock_conn.commit.assert_called_once()  # Check that commit was called

