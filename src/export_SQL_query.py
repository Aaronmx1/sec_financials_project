import pandas as pd
import os
from dotenv import load_dotenv
#from db_connector import create_db_connection
from sqlalchemy import create_engine, URL, text
import mariadb  # Allows communicationg between engine and mariadb

# Obtain .env variables
load_dotenv()

## Establish connection
# connection details
url_object = URL.create(
    "mariadb+mariadbconnector",
    username=os.getenv("DB_USER"),
    password = os.getenv("DB_PASSWORD"),
    host = os.getenv("DB_HOST"),
    database = os.getenv("DB_NAME"),
    port = os.getenv("DB_PORT")
)

# Create database engine
engine = create_engine(url_object)
print("engine: ", engine)

## Define SQL query
sql_query = "" \
    "select da.account_name, amount, start_date, end_date, fiscal_year, df.fiscal_period " \
    "from fact_financial_reports f " \
    "left join dim_accounts da on f.account_id=da.account_id " \
    "left join dim_forms df on f.form_id=df.form_id " \
    "where da.account_id = 1126 " \
    "order by start_date desc"

# Create file connection context manager
with engine.connect() as connection:
    # Execute query
    result = connection.execute(text(sql_query))

    # Get headers
    headers = result.keys()

    # Create DataFrame from query
    df = pd.DataFrame(data=result, columns=headers)

# Get directory path
output_directory = os.getenv("SQL_DATA")

# Combine path
file_path = os.path.join(output_directory, "out.csv")

# Save DataFrame to file path
df.to_csv(file_path, index=False)
print(f"DataFrame successfully saved to: {file_path}")