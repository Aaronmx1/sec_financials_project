# Standard library imports
import os
import json

# Third-party imports
import pandas as pd
import numpy as np
import pyarrow                  # Required by pandas to write to Parquet format
from dotenv import load_dotenv
import re                       # regex

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
                'fiscal_year': None,
                'fiscal_period': None,
                #'form': entry.get('form'),
                'filed_date': entry.get('filed'),
                'frame': entry.get('frame')
            })

# Create the DataFrame
financials_df = pd.DataFrame(flattened_data)

# Parse frame attribute to capture fiscal year and period of record
print("financials_df.head(): ", financials_df.head())

# Isolate NULL values and determine if they are okay
#print(financials_df.isnull().sum())
#print(financials_df[financials_df['description'].isnull()][['account_name', 'description','filed_date']])

## Convert data types
# Define a mapping for data type conversions
dtype_mapping = {
    'string': ['cik', 'entity_name', 'account_name', 'description', 'accession_number', 'fiscal_year', 'fiscal_period', 'frame'],
    'int64': ['value'],
    'datetime': ['start_date', 'end_date', 'filed_date']
}

# Apply data type conversions
for dtype, columns in dtype_mapping.items():
    for column in columns:
        if dtype == 'datetime':
            financials_df[column] = pd.to_datetime(financials_df[column], format="%Y-%m-%d")
        else:
            financials_df[column] = financials_df[column].astype(dtype)

# Pad every CIK in the column with leading zeroes to a length of 10 to match dim_submissions
financials_df['cik'] = financials_df['cik'].str.zfill(10)

# Pandas formatting to display more columns
pd.set_option("display.max_columns", 13)

## Remove duplicate records
# Records where 'frame' is NULL is removed because records exist as prior period values aiding in comparisons for US GAAP financial statement presentation purposes.
financials_df_frame_clean = financials_df[~financials_df['frame'].isnull()].copy()

# Parse 'frame' attribute to populate fiscal year and fiscal period
# Define regex pattern to extract year and quarter
pattern = r'CY(\d{4})(Q\d+)?'

# Create new columns from captured regex groups
financials_df_frame_clean[['fiscal_year', 'fiscal_period']] = financials_df_frame_clean['frame'].str.extract(pattern)

# Fill NULL quarters with FY to represent 10-K filing
financials_df_frame_clean['fiscal_period'] = financials_df_frame_clean['fiscal_period'].fillna('FY')

# Define the columns that make a record unique
subset_cols = ['accession_number', 'account_name']

# Enhance readability by adding a space between words in Account Name's
#financials_df_latest['account_name'] = (financials_df_latest['account_name']
financials_df_frame_clean['account_name'] = (financials_df_frame_clean['account_name']
                                        # Insert an empty space whenever a lowercase or number precedes an upper case letter
                                        .str.replace(r'([a-z0-9])([A-Z])', r'\1 \2', regex=True)
                                        .str.lower())

# Review cleaned data
print('\n\n',financials_df_frame_clean.info())
print("Shape of DataFrame: ", financials_df_frame_clean.shape)

## Store data
# Define filename
file_name = f"{cik}_facts.parquet.gzip"

# Join the path and filename
full_path = os.path.join(os.getenv("SILVER_DATA"), file_name)

# Save cleaned DataFrame object into Parquet file
financials_df_frame_clean.to_parquet(full_path, compression='gzip')