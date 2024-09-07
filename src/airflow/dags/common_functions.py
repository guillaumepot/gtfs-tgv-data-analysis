"""
Contains common functions that are used by the tasks in the Airflow dags
"""

# LIB
import json
import logging
import os
import psycopg2


# VARS
source_file_path = "./sources.json"


# COMMON FUNCTIONS
def load_url(filename:str, source_file: str = source_file_path) -> str:
    """
    Load the URL corresponding to the given filename from the source file.
    Parameters:
    - filename (str): The name of the file for which the URL needs to be loaded.
    - source_file (str): The path to the source file containing the URLs. Default is "sources.json".
    Returns:
    - str: The URL corresponding to the given filename.
    """

    try:
        with open(source_file, 'r') as file:
            sources = json.load(file)
    except FileNotFoundError:
        raise FileNotFoundError(f"The source file {source_file} was not found.")
    except json.JSONDecodeError:
        raise ValueError(f"The source file {source_file} is not a valid JSON file.")

    if filename not in sources:
        raise KeyError(f"The filename {filename} was not found in the source file.")


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
    
