"""
Contains functions that are used by the tasks in the Airflow dags
"""


# LIB
from datetime import datetime, timezone
import json
import logging
import os
import pandas as pd
import psycopg2
import requests

from .common_functions import load_json_as_df, reverse_json_to_df

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



def transform_file(**kwargs) -> dict:
    """
    
    """

    # File dedicated functions
    def transform_gares_de_voyageurs(df:pd.dataFrame) -> pd.dataFrame:
        """
        
        """
        pass


    def transform_occupation_gares(df:pd.dataFrame) -> pd.dataFrame:
        """
        
        """
        pass


    def transform_ponctualite_globale_tgv(df:pd.dataFrame) -> pd.dataFrame:
        """
        
        """
        pass


    def transform_ponctualite_tgv_par_route(df:pd.dataFrame) -> pd.dataFrame:
        """
        
        """
        pass


    # Function logic
    task_instance = kwargs.get('task_instance')
    if not task_instance:
        raise ValueError("task_instance is required in kwargs")

    file = kwargs.get('file')
    if not file:
        raise ValueError("file is required in kwargs")


    transform_functions = {
        'gares_de_voyageurs.csv': transform_gares_de_voyageurs,
        'occupation_gares.csv': transform_occupation_gares,
        'ponctualite_globale_tgv.csv': transform_ponctualite_globale_tgv,
        'ponctualite_tgv_par_route.csv': transform_ponctualite_tgv_par_route
    }


    if file not in transform_functions:
        logging.warning(f"No transformation function for file: {file}.txt")
        return []


    logging.info(f"Transforming {file} data")

    # Retrieve the XCom value and transform the data
    json_datas = task_instance.xcom_pull(task_ids=f"load_{file}")
    # Load as DF
    df_json_datas = load_json_as_df(json_datas)
    # Transform Data
    transformed_data = transform_functions[file](df_json_datas)
    # Revserse DF to JSON
    transformed_data_json = reverse_json_to_df(transformed_data)
    logging.info(f"{file} data transformed")

    return transformed_data_json





























def save_file(file:str) -> None:
    """
    
    """
    pass