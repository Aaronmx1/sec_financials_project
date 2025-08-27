# Standard library imports
import os

# Third-party imports
import pandas as pd
import pyarrow
from data_loader import load_dataframe_to_db
from dotenv import load_dotenv

def import_submissions_parquet():
    ''' Imports submissions files into submissions dimension table.'''
    ## Retrieve data
    # Define filename
    file_name = "CIK0000002488_submission.parquet.gzip"

    # Load variables from .env
    load_dotenv()

    # Join the path and filename
    full_path = os.path.join(os.getenv("SILVER_DATA"), file_name)

    # Retrieve Parquet as DataFrame
    df = pd.read_parquet(full_path)

    # Define the columns to be inserted into the database
    submission_columns = [
        'entity_name',
        'cik',
        'sic_description',
        'owner_organization',
        'ticker',
        'ein'
    ]

    # Import DataFrame into database
    load_dataframe_to_db(df, "dim_submissions", submission_columns)

def import_staging_facts_parquet():
    ''' Imports facts files into staging table.'''
    ## Retrieve data
    # Define filename
    file_name = "CIK0000002488_facts.parquet.gzip"

    # Load variables from .env
    load_dotenv()

    # Join the path and filename
    full_path = os.path.join(os.getenv("SILVER_DATA"), file_name)

    # Retrieve Parquet as DataFrame
    df = pd.read_parquet(full_path)

    # Replace any NaT values in datetime columns with None for database compatibility
    df['start_date'] = df['start_date'].replace({pd.NaT: None})

    # Define the columns to be inserted into database
    staging_fact_columns = [
        'cik',
        'entity_name',
        'account_name',
        'description',
        'start_date',
        'end_date',
        'value',
        'accession_number',
        'fiscal_year',
        'fiscal_period',
        #'form',
        'filed_date',
        'frame'
    ]

    # Import DataFrame into database
    load_dataframe_to_db(df, "staging_fact_data", staging_fact_columns)

if __name__ == '__main__':
    import_submissions_parquet()
    import_staging_facts_parquet()