# SEC Financials Project

This project automates the extraction, cleaning, and processing of financial data from the SEC EDGAR API. It retrieves company submission details and financial facts, flattens the nested JSON data, and prepares it for analysis.

***

## Installation

`pip install -r requirements.txt`
*(Note: It's good practice to create the requirements file with `pip freeze > requirements.txt`)*.

***

# Script Overview

### `dim_api_ingestion.py`

**Overview:** This script automates the retrieval of public company submission data from the U.S. Securities and Exchange Commission (SEC).

**Process:**
1.  **API Request:** Sends a `GET` request to the SEC's EDGAR API (`https://data.sec.gov/submissions/{CIK}.json`), targeting a specific entity using its Central Index Key (CIK).
2.  **Authentication:** The request includes a custom `User-Agent` in the header, which is required for programmatic API access. This identifier is loaded from an environment variable for security.
3.  **Validation & Parsing:** Checks the HTTP status code of the response. If successful (status code 200), it parses the JSON payload into a Python dictionary.
4.  **Data Storage:** Saves the extracted data as a JSON file to a local directory, with the file path managed via environment variables to avoid hard-coding. If the request fails, an error message is printed.


### `fact_api_ingestion.py`

**Overview:** This script automates the retrieval of a company's financial facts (XBRL data) from the SEC.

**Process:**
1.  **API Request:** Sends a `GET` request to the SEC's EDGAR API (`https://data.sec.gov/api/xbrl/companyfacts/{CIK}.json`).
2.  *(The rest of the process is identical to the `dim_api_ingestion.py` script).*


### `dim_data_cleaning.py`

**Overview:** This script reads the raw JSON submission data, flattens the nested structure, and transforms it into a clean pandas DataFrame.

**Process:**
1.  **Load Data:** Ingests the `_submission.json` file for a specific CIK.
2.  **Flatten JSON:** Extracts key-value pairs for company metadata such as CIK, name, ticker, and SIC description.
3.  **Create DataFrame:** Constructs a pandas DataFrame from the extracted data.
4.  **Data Type Conversion:** Converts columns to their appropriate data types (e.g., string, integer) for consistency and analysis.
5.  **Remove Duplicates:** Removes additional records associated to accession numbers to store only the most recent recorded amount for that account and accession number.
6.  **Data Storage:** Saves the final, cleaned DataFrame to a compressed Parquet file (.parquet.gzip). This columnar format is optimized for storage and analytical performance, preparing it for the next stage of the data pipeline, such as loading into a data warehouse.


### `fact_data_cleaning.py`

**Overview:** This script processes the raw JSON financial facts data, flattens the complex nested structure containing all financial measurements, and transforms it into a clean, tabular pandas DataFrame.

**Process:**
1.  **Load Data:** Ingests the `_facts.json` file.
2.  **Flatten Nested Facts:** Iterates through the `us-gaap` accounting concepts, extracting the label, description, and all associated financial values reported in 'USD'.
3.  **Construct DataFrame:** Builds a long-format DataFrame where each row represents a single financial filing for a specific accounting concept.
4.  **Data Type Conversion:** Converts columns to appropriate data types, including casting date strings to `datetime` objects and numerical columns to integers or floats.
5.  **Remove Duplicates:** Removes additional records associated to accession numbers to store only the most recent recorded amount for that account and accession number.
6.  **Data Storage:** Saves the final, cleaned DataFrame to a compressed Parquet file (.parquet.gzip). This columnar format is optimized for storage and analytical performance, preparing it for the next stage of the data pipeline, such as loading into a data warehouse.


### `db_connector.py`

**Overview:** This script creates the connection to our database.

**Process:**
1.  **Load Environment Variables:** Retrieves environment variables to conceal our database connection details.
2.  **Create Connection:** Using our environment variables, we create establish a connection to our database and return the connection details, otherwise the program exits during a failed connection.


### `data_loader.py`

**Overview:** This script receives the dataset, table name, and the number of attributes defined in the schema in order to flexibly load different datasets.

**Process:**
1.  **Connect to Database:** Receives connection to database and checks whether connection has been established.
2.  **Generate SQL:** The SQL script is generated dynamically using our parameters to guide the table and number of attributes to be inserted into.
3.  **Insert Records:** The batched records are inserted and the connection is closed when the transaction has been completed, otherwise a rollback is performed due to ACID failure.

***

## Database Schema

The project stores the cleaned data in a relational database using a **Star Schema**. This design features a central `fact_financial_reports` table containing quantitative measurements (facts). This fact table is linked to several dimension tables (`dim_submissions`, `dim_accounts`, `dim_forms`), which provide descriptive context.


### `create_database.sql`

**Overview:** This script creates the database to store SEC Financials.

**Process:**
1.  **Create Database:** Generates database structure to maintain tables.


### `create_tables.sql`

**Overview:** This script creates multiple tables forming a star schema with the financial values as the fact table and a submissions, accounts, and forms table providing dimensions to the fact table.  A staging table is used to parse data from the fact dataset in order to populate the dimension table before loading in the remaining fact data.

**Process:**
1.  **Create Table:** Generate dimenions, fact, and a staging table to house the data.

