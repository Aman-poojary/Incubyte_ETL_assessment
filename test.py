import unittest
from test import TestValidateData, TestLoadData

# Create a test suite that loads all tests from the imported test cases
def suite():
    test_suite = unittest.TestSuite()
    # Add tests from each test case
    test_suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestValidateData))
    test_suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestLoadData))
    return test_suite

if __name__ == "__main__":
    try:
        print("------Starting Testing------")
        # Run the test suite
        runner = unittest.TextTestRunner()
        runner.run(suite())
    except Exception as e:
        print(f"An error occurred during the ETL process: {e}")
    finally:
        print("------Testing Process Completed------")
