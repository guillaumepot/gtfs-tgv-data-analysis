"""
Contains common functions that are used by the tasks in the Airflow dags
"""

# LIB
import json
import logging
import os
import pandas as pd
import psycopg2


# VARS
source_file_path = os.path.join(os.path.dirname(__file__), 'sources.json')


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

    return sources[filename]



def load_df_from_file(**kwargs) -> dict:
    """
    Load GTFS data from a file into a pandas DataFrame and convert it to a JSON string.
    Args:
        **kwargs: Keyword arguments containing the following:
            - filepath (str): The path to the GTFS data file.
    Returns:
        dict: The GTFS data loaded into a DataFrame and converted to a JSON string.
    Raises:
        FileNotFoundError: If the specified file does not exist.
        Exception: If the specified file is not supported or does not exist.
    """
    # Check if the file exists
    filepath = kwargs.get('filepath')
    if not filepath or not os.path.exists(filepath):
        raise FileNotFoundError(f"File {filepath} not found")

    # Load the file into a dataframe
    df = pd.read_csv(filepath)
    logging.info(f"Dataframe loaded, shape: {df.shape}")

    # Convert the dataframe to a JSON string
    return df.to_json(orient='records')



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
    


def load_json_as_df(json_datas:dict) -> pd.DataFrame:
    """
    Load a JSON string as a pandas DataFrame.
    Args:
        json_datas (dict): The JSON string to load.
    Returns:
        pd.DataFrame: The JSON data loaded into a DataFrame.
    """
    json_str = json.dumps(json_datas)
    return pd.read_json(json_str)

def reverse_json_to_df(df:pd.DataFrame) -> dict:
    """
    Reverse the JSON string to a pandas DataFrame.
    """
    return df.to_json(orient='records')