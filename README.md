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
4.  **Data Typing:** Converts columns to their appropriate data types (e.g., string, integer) for consistency and analysis.
5.  **Data Storage:** Saves the final, cleaned DataFrame to a compressed Parquet file (.parquet.gzip). This columnar format is optimized for storage and analytical performance, preparing it for the next stage of the data pipeline, such as loading into a data warehouse.


### `fact_data_cleaning.py`

**Overview:** This script processes the raw JSON financial facts data, flattens the complex nested structure containing all financial measurements, and transforms it into a clean, tabular pandas DataFrame.

**Process:**
1.  **Load Data:** Ingests the `_facts.json` file.
2.  **Flatten Nested Facts:** Iterates through the `us-gaap` accounting concepts, extracting the label, description, and all associated financial values reported in 'USD'.
3.  **Construct DataFrame:** Builds a long-format DataFrame where each row represents a single financial filing for a specific accounting concept.
4.  **Data Typing:** Converts columns to appropriate data types, including casting date strings to `datetime` objects and numerical columns to integers or floats.
5.  **Data Storage:** Saves the final, cleaned DataFrame to a compressed Parquet file (.parquet.gzip). This columnar format is optimized for storage and analytical performance, preparing it for the next stage of the data pipeline, such as loading into a data warehouse.