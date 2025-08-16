# Standard library imports
import os
import json

# Third-party imports
import pandas as pd
import pyarrow                  # Required by pandas to write to Parquet format
from dotenv import load_dotenv

# Load variables from .env
load_dotenv()

# AMD CIK number for entity
cik = 'CIK0000002488'

# Read in JSON unclean bronze data
file = os.getenv("BRONZE_DATA") + cik + "_facts.json"
with open(file, 'r') as f:
    raw_data = json.load(f)

# Extract Entity CIK #
entity_cik = raw_data.get('cik', {})

# Extract Entity name
entity_name = raw_data.get('entityName', {})

# Extract the us-gaap facts
us_gaap_facts = raw_data.get('facts', {}).get('us-gaap', {})

# List to hold the flattened data
flattened_data = []

# Loop throught each account in us-gaap_facts to flatten the nested structure.
# Each 'entry' within the 'USD' units represents a unique data point for a financial filing.
for account, details in us_gaap_facts.items():
    # Extract the description
    description = details.get('description')

    # Check for USD units and extract the values
    if 'USD' in details.get('units', {}):
        # Each entry is a record
        for entry in details['units']['USD']:
            flattened_data.append({
                'cik': entity_cik,
                'entity_name': entity_name,
                'account_name': account,
                'description': description,
                'start_date': entry.get('start'),
                'end_date': entry.get('end'),
                'value': entry.get('val'),
                'accession_number': entry.get('accn'),
                'fiscal_year': entry.get('fy'),
                'fiscal_period': entry.get('fp'),
                'form': entry.get('form'),
                'filed_date': entry.get('filed')
            })

# Create the DataFrame
financials_df = pd.DataFrame(flattened_data)

# Isolate NULL values and determine if they are okay
#print(financials_df.isnull().sum())
#print(financials_df[financials_df['description'].isnull()][['account_name', 'description','filed_date']])

## Convert data types
# Define a mapping for data type conversions
dtype_mapping = {
    'string': ['entity_name', 'account_name', 'description', 'accession_number', 'fiscal_period', 'form'],
    'int64': ['fiscal_year'],
    'datetime': ['start_date', 'end_date', 'filed_date']
}

# Apply data type conversions
for dtype, columns in dtype_mapping.items():
    for column in columns:
        if dtype == 'datetime':
            financials_df[column] = pd.to_datetime(financials_df[column], format="%Y-%m-%d")
        else:
            financials_df[column] = financials_df[column].astype(dtype)

# Pandas formatting to display more columns
pd.set_option("display.max_columns", 13)

# Review semi-clean data
print('\n\n',financials_df.info())

## Remove duplicates
# Sort by accession_number, then by start_date and end_date from newest to oldest
financials_df_sorted = financials_df.sort_values(
    by=['accession_number', 'start_date', 'end_date'],
    ascending=[True,False,False]
)

# Define the columns that make a record unique
subset_cols = ['accession_number', 'account_name']

# Drop duplicates, keeping the first occurrence (which is the latest)
financials_df_latest = financials_df_sorted.drop_duplicates(
    subset=subset_cols,
    keep='first'
)

# Review cleaned data
print('\n\n',financials_df_latest.info())

## Store data
# Define filename
file_name = f"{cik}_facts.parquet.gzip"

# Join the path and filename
full_path = os.path.join(os.getenv("SILVER_DATA"), file_name)

# Save cleaned DataFrame object into Parquet file
financials_df_latest.to_parquet(full_path, compression='gzip')