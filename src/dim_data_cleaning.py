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
file = os.getenv("BRONZE_DATA") + cik + "_submission.json"
with open(file, "r") as f:
    raw_data = json.load(f)

# Flattened data
flattened_data = []

# Add record
flattened_data.append({
    'cik': raw_data.get("cik", {}),
    'name': raw_data.get("name", {}),
    'sic_description': raw_data.get("sicDescription", {}),
    'owner_organization': raw_data.get("ownerOrg", {}),
    'ticker': raw_data.get("tickers", {})[0],
    'ein': raw_data.get("ein", {})
})

# Transfrom to DataFrame
submissions_df = pd.DataFrame(flattened_data)

## Convert data types
# Define a mapping for data conversions
dtype_mapping = {
    'string': ['cik', 'name', 'sic_description', 'owner_organization', 'ticker'],
    'int64': ['ein']
}

# Apply data conversions
for dtype, columns in dtype_mapping.items():
    for column in columns:
        submissions_df[column] = submissions_df[column].astype(dtype)

# Pandas formatting to display more columns
pd.set_option("display.max_columns", 11)

# Review final data types
print(submissions_df.head())
print(submissions_df.info())

## Store data
# Define filename
file_name = f"{cik}_submission.parquet.gzip"

# Join the path and filename
full_path = os.path.join(os.getenv("SILVER_DATA"), file_name)

# Save cleaned DataFrame object into Parquet file
submissions_df.to_parquet(full_path, compression='gzip')