"""
Contains functions that are used by the tasks in the Airflow dags
"""


# LIB
from datetime import datetime, timezone
import json
import logging
import os
import psycopg2
import requests



# TASK FUNCTIONS
def download_file_from_url(url:str, filename:str, storage_path:str) -> None:
    """
    Download a file from the given URL and save it to the specified storage path with the given filename.
    Args:
        url (str): The URL of the file to be downloaded.
        filename (str): The name of the file to be saved.
        storage_path (str): The path where the file will be saved.
    Raises:
        Exception: If the file download fails with a non-200 status code.
    Returns:
        None
    """
    # Download file & save file
    response = requests.get(url)
    logging.info(f"Request status code: {response.status_code}")


    if response.status_code == 200:
        with open(os.path.join(storage_path, filename), 'wb') as file:
            file.write(response.content)
        logging.info(f"File downloaded and saved in {storage_path} as {filename}")
    else:
        raise Exception(f"Failed to download file from {url} ; status code: {response.status_code}")
