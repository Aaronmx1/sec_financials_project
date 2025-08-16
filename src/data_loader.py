# Standard library imports
import os

# Third-party imports
import pandas as pd
import mariadb
from db_connector import create_db_connection
import sys

def load_dataframe_to_db(df, table_name, columns, batch_size=500):
    """
    Receives a DataFrame and inserts its contents into a specified database table.

    Args:
        df (pd.DataFrame): The DataFrame to load.
        table_name (str): The name of the target database table.
        columns (list): A list of column names in the target table.
        batch_size (int): The number of rows to insert per batch.
    """

    # Initiate database connection
    conn = create_db_connection()

    # Check connection
    if conn is None:
        print("Error: Cannot create the database connection.")
        return

    try:
        # Get cursor to provide interface for interacting with database
        cur = conn.cursor()

        # Create placeholders string, e.g. "(?,?,?)"
        placeholders = ", ".join(["?"] * len(columns))

        # Create the full INSERT statement
        sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"

        for start in range(0, len(df), batch_size):
            end = start + batch_size
            batch = df.iloc[start:end]
            data_to_insert = batch[columns].values.tolist()
            cur.executemany(sql, data_to_insert)

        # Commit the transaction after all batches are successfully processed
        conn.commit()
        print(f"All data successfully inserted into {table_name}.")

    except mariadb.Error as e:
        print(f"Error loading data into {table_name}: {e}")
        conn.rollback() # Roll back the transaction on error
    
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")