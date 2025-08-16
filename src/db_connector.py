"""
@author: Aaron
Overview: Connect to Mariadb
Source: https://mariadb.com/resources/blog/how-to-connect-python-programs-to-mariadb/
"""

# Standard library imports
import os

# Third-party imports
from dotenv import load_dotenv
import sys
import mariadb

def create_db_connection():
    """
    Establishes and returns a connection to the MariaDB database.

    Exits the program if connection details are missing or if the
    connection fails.

    Returns:
        mariadb.connections.Connection: A connection object on success, otherwise exits.
    """
    # Obtain .env variables
    load_dotenv()
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    database = os.getenv("DB_NAME")

    # Evaluate environment variables are loaded
    if user is None or password is None or host is None or port is None or database is None:
        print("Error: One or more environment variables are missing.")
        sys.exit(1)

    # Convert port to integer
    try:
        port = int(port)
    except ValueError:
        print("Error: Invalid port number in .env file.")
        sys.exit(1)

    # Establish connection to database
    try:
        conn = mariadb.connect(
            user=user,
            password=password,
            host=host,
            port=port,
            database=database
        )
        print("Connection successful: ", conn)
    except mariadb.Error as e:
        print(f"Error connecting to MariaDB platform: {e}")
        sys.exit(1)
    
    return conn