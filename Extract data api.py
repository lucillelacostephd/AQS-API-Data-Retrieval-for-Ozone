# -*- coding: utf-8 -*-
"""
Fetch hourly O3 data from EPA AQS API.
"""

import requests
import pandas as pd
import time
import logging
import os

# Configure logging to print to both console and file
log_file = r"C:\Users\LB945465\OneDrive - University at Albany - SUNY\State University of New York\Spyder\ozone_data_fetch.log"
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s",
                    handlers=[
                        logging.FileHandler(log_file, encoding="utf-8"),  # Fix logging error
                        logging.StreamHandler()  # Prints logs in the console
                    ])

# EPA AQS API Details
EPA_AQS_URL = "https://aqs.epa.gov/data/api/sampleData/bySite"
EPA_AQS_KEY = "goldfox28"  # Replace with your actual API key

# Define sites with their EPA Site IDs
sites = {
    "Queens": {"state": "36", "county": "081", "site": "0124"},
    "Chester": {"state": "34", "county": "027", "site": "3001"},
    #"Elizabeth": {"state": "34", "county": "039", "site": "0004"}, # not available
    #"Richmond": {"state": "36", "county": "085", "site": "0111"}, # not available
    #"Kings": {"state": "36", "county": "047", "site": "0118"}, # not available
}

# Ensure output directory exists
output_dir = r"C:\Users\LB945465\OneDrive - University at Albany - SUNY\State University of New York\Spyder"
os.makedirs(output_dir, exist_ok=True)

def fetch_epa_hourly_ozone_data(state, county, site, start_year=2000, end_year=2021):
    """Fetch hourly O3 data from EPA AQS API for a given site and time range."""
    all_data = []
    
    for year in range(start_year, end_year + 1):
        params = {
            "email": "llacoste@albany.edu",  
            "key": "goldfox28",
            "param": "44201",  # Ozone parameter code
            "duration": "1",  # 1-hour data
            "bdate": f"{year}0101",  
            "edate": f"{year}1231",  
            "state": state,
            "county": county,
            "site": site
        }

        try:
            response = requests.get("https://aqs.epa.gov/data/api/sampleData/bySite", params=params)
            response.raise_for_status()  # Raise error if request fails
            data = response.json()  # Convert response to JSON

            # Debug: Print API response structure
            print(f"\n✅ API Response for {site} ({year}):")
            print(data.keys())  # Print top-level keys of JSON
            
            # Ensure "Data" exists in the response
            if "Data" in data and isinstance(data["Data"], list) and len(data["Data"]) > 0:
                all_data.extend(data["Data"])
            else:
                print(f"⚠️ No data found for {site} in {year}.")
            
            time.sleep(1)  # Avoid API rate limits
        except requests.RequestException as e:
            print(f"❌ Error fetching data for {site} in {year}: {e}")
            continue  # Continue with the next year even if an error occurs

    return all_data

def process_and_save_hourly_data(site_name, state, county, site):
    """Fetch, process, and save hourly O3 data for a given site."""
    output_file = os.path.join(output_dir, f'ozone_hourly_data_{site_name}.csv')
    data = fetch_epa_hourly_ozone_data(state, county, site)
    
    if not data:
        logging.warning(f"No O3 data for {site_name}. Skipping.")
        return
    
    df = pd.DataFrame(data)

    # Debug: Print available columns
    print(f"Available columns for {site_name}: {df.columns.tolist()}")
    print(df.head())

    # Ensure the expected column exists
    if 'sample_measurement' not in df.columns:
        logging.error(f"❌ Missing required measurement column in {site_name}. Data not saved.")
        return

    # Convert O3 values to numeric
    df['sample_measurement'] = pd.to_numeric(df['sample_measurement'], errors='coerce')

    # Combine date and time for proper datetime formatting
    df['DateTime'] = pd.to_datetime(df['date_local'] + ' ' + df['time_local'])

    # Keep only relevant columns
    df_cleaned = df[['DateTime', 'sample_measurement']].rename(columns={'sample_measurement': 'Ozone Concentration'})

    # Save the cleaned data
    df_cleaned.to_csv(output_file, mode='w', header=True, index=False)
    print(f"✅ Saved hourly data for {site_name} to {output_file}")

    logging.info(f"Completed fetching for {site_name}.")

# Run script for all sites
for site_name, details in sites.items():
    process_and_save_hourly_data(site_name, details["state"], details["county"], details["site"])

print("All hourly data fetching and saving complete.")
