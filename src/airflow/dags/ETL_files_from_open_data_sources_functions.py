"""
Contains functions that are used by the tasks in the Airflow dags
"""


# LIB
import json
import logging
import os
import pandas as pd
import requests

from common_functions import load_json_as_df, reverse_json_to_df


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
    Transform the data based on the given file.
    Parameters:
    - kwargs (dict): Keyword arguments containing 'task_instance' and 'file'.
    Returns:
    - dict: Transformed data in JSON format.
    Raises:
    - ValueError: If 'task_instance' or 'file' is not provided in kwargs.
    """

    # File dedicated functions
    def transform_gares_de_voyageurs(df:pd.DataFrame) -> pd.DataFrame:
        """
        Nothing to change
        """
        return df


    def transform_occupation_gares(df:pd.DataFrame) -> pd.DataFrame:
        """
        Nothing to change        
        """
        return df


    def transform_ponctualite_globale_tgv(df:pd.DataFrame) -> pd.DataFrame:
        """
        Nothing to change
        """
        return df


    def transform_ponctualite_tgv_par_route(df:pd.DataFrame) -> pd.DataFrame:
        """
        Nothing to change
        """
        return df


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





def save_file(**kwargs) -> None:
    """
    Save files to the specified storage path.
    Args:
        **kwargs: Additional keyword arguments.
            task_instance (TaskInstance): The task instance.
            storage_path (str): The storage path to save the files.
    Raises:
        ValueError: If task_instance is not provided in kwargs.
    """

    task_instance = kwargs.get('task_instance')
    if not task_instance:
        raise ValueError("task_instance is required in kwargs")

    # Get storage path
    clean_storage_path = kwargs.get('clean_storage_path')

    # Retrieve transformed data
    logging.info("Retrieving transformed data")
    data = task_instance.xcom_pull(task_ids=f"transform_{kwargs['file']}")


    # Save file
    with open(os.path.join(clean_storage_path, kwargs['file']), 'w') as f:
        json.dump(data, f)
    logging.info(f"{kwargs['file']} data saved in {clean_storage_path}")