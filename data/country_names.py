country_codes = {'USA', 'AU', 'PHL', 'CAN', 'IND'}

country_map = {
    "USA": "United_States",
    "IND": "India",
    "AU": "Australia",
    "CAN": "Canada",
    "PHL": "Philippines",
    "UK": "United_Kingdom",
    "DEU": "Germany",
    "FRA": "France",
    "JPN": "Japan",
    "CHN": "China",
    "BRA": "Brazil",
    "ZAF": "South_Africa",
    "RUS": "Russia"
    # Add more country codes and names as needed
}

# Optionally, you could also add a function to get the country name from the code
def get_country_name(code):
    return country_map.get(code, "Unknown Country")
