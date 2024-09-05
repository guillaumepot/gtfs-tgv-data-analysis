"""
Contains common functions that are used by the tasks in the Airflow dags
"""


# LIB
import logging
import os
import psycopg2



# COMMON FUNCTIONS
def connect_to_postgres() -> psycopg2.connect:
    """
    Connects to the database using the provided credentials.

    Returns:
        psycopg2.connect: The connection object representing the connection to the database.
    """

    return psycopg2.connect(user=os.getenv("DATA_PG_USER"),
                            password=os.getenv("DATA_PG_PASSWORD"),
                            dbname=os.getenv("DATA_PG_DB"),
                            host=os.getenv("DATA_PG_HOST"),
                            port=os.getenv("DATA_PG_PORT"))
            


def clear_raw_files(storage_path: str) -> None:
    """
    Clear all raw files in the specified GTFS storage path.
    Args:
        storage_path (str): The path to the storage directory.
    """
    try:
        for filename in os.listdir(storage_path):
            file_path = os.path.join(storage_path, filename)
            if os.path.isfile(file_path):
                os.remove(file_path)
                logging.info(f"Deleted file: {file_path}")
    except Exception as e:
        raise e