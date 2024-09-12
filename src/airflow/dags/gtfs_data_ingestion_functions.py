"""
Contains functions that are used by the tasks in the Airflow dags
"""


# LIB
#from airflow.models import TaskInstance
import json
import logging
import os
import pandas as pd
import requests
import zipfile


from .common_functions import connect_to_postgres, load_json_as_df, reverse_json_to_df
from .gtfs_queries import queries


# TASKS FUNCTIONS
def get_gtfs_files(gtfs_url:str, gtfs_storage_path:str) -> None:
    """
    Downloads GTFS files from the given URL and saves them to the specified storage path.
    Args:
        gtfs_url (str): The URL of the GTFS files.
        gtfs_storage_path (str): The path where the GTFS files will be saved.
    Raises:
        Exception: If there is an error while getting the GTFS files.
    """
    
    try:
        # Get the file
        response = requests.get(gtfs_url)
        logging.info(f"Response status code: {response.status_code}")

        # Save the file
        zip_file_path = os.path.join(gtfs_storage_path, "export_gtfs_voyages.zip")
        with open(zip_file_path, "wb") as file:
            file.write(response.content)
        logging.info(f"File saved to {zip_file_path}")
        
        # Extract the contents of the zip file
        with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
            zip_ref.extractall(gtfs_storage_path)
        logging.info(f"Files extracted to {gtfs_storage_path}")

        # Delete the zip file
        os.remove(zip_file_path)
        logging.info(f"Zip file deleted")
            
    except Exception as e:
        raise Exception(f"Error while getting the GTFS files: {e}")



def data_cleaner(**kwargs) -> list:
    """
    Clean and transform GTFS data based on the specified file.
        **kwargs: Keyword arguments containing the necessary parameters.
            - task_instance: The task instance object.
            - file: The name of the file to be transformed.
        list: The transformed data in JSON format.
    Raises:
        ValueError: If task_instance or file is not provided in kwargs.
    """

    # Functions to transform the data depending on the file
    def transform_routes(df:pd.DataFrame) -> pd.DataFrame:
        """
        Transform the routes data.
        """
        # Remove unnecessary columns
        df.drop(columns=['agency_id', 'route_desc', 'route_url', 'route_color', 'route_text_color'], inplace=True)

        # Add a new column to store transport equivalent for route_type
        transports = {
            0: 'tramway',
            1: 'subway',
            2: 'rail',
            3: 'bus',
            4: 'ferry',
            5: 'cable_car',
            6: 'funicular',
            7: 'troleybus',
            11: 'light_rail',
        }

        df['route_name'] = df['route_type'].apply(lambda x: transports[x] if x in transports else 'unknown')

        return df

    def transform_calendar_dates(df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform the calendar dates data.
        """
        # Ensure the date column is in the correct format
        df['date'] = pd.to_datetime(df['date'], format='%Y%m%d').dt.strftime('%Y-%m-%d')
        return df


    def transform_stops(df:pd.DataFrame) -> pd.DataFrame:
        """
        Transform the stops data.
        """
        # Remove unnecessary columns
        df.drop(columns=['stop_desc', 'zone_id', 'stop_url'], inplace=True)

        return df

    def transform_stop_times(df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform the stop times data.
        """
        # Remove unnecessary columns
        df.drop(columns=['stop_headsign', 'shape_dist_traveled'], inplace=True)

        # Ensure the time columns are in the correct format
        def correct_time_format(time_str):
            h, m, s = map(int, time_str.split(':'))
            h = h % 24  # Correct hours to be within 0-23
            return f"{h:02}:{m:02}:{s:02}"

        df['arrival_time'] = df['arrival_time'].apply(correct_time_format)
        df['departure_time'] = df['departure_time'].apply(correct_time_format)

        return df

    def transform_trips(df:pd.DataFrame) -> pd.DataFrame:
        """
        Transform the trips data.
        """
        # Remove unnecessary columns
        df.drop(columns=['shape_id'], inplace=True)

        return df

    # Function logic
    task_instance = kwargs.get('task_instance')
    if not task_instance:
        raise ValueError("task_instance is required in kwargs")

    file = kwargs.get('file')
    if not file:
        raise ValueError("file is required in kwargs")

    transform_functions = {
        'calendar_dates': transform_calendar_dates,
        'routes': transform_routes,
        'stops': transform_stops,
        'stop_times': transform_stop_times,
        'trips': transform_trips,
    }


    if file not in transform_functions:
        logging.warning(f"No transformation function for file: {file}.txt")
        return []


    logging.info(f"Transforming {file} data")

    # Retrieve the XCom value and transform the data
    json_datas = task_instance.xcom_pull(task_ids=f"load_{file}.txt")
    # Decode JSON string
    json_datas = json.loads(json_datas)
    # Load as DF
    df_json_datas = load_json_as_df(json_datas)
    # Transform Data
    transformed_data = transform_functions[file](df_json_datas)
    # Revserse DF to JSON
    transformed_data_json = reverse_json_to_df(transformed_data)
    logging.info(f"{file} data transformed")

    return transformed_data_json



def ingest_gtfs_data_to_database(**kwargs) -> None:
    """
    Ingests GTFS data into the database.
    Args:
        **kwargs: Keyword arguments containing the following parameters:
            - task_instance (TaskInstance): The task instance object.
            - file (str): The name of the file to ingest.
    Raises:
        ValueError: If task_instance or file is not provided in kwargs.
        ValueError: If no data is found for the corresponding task_id.
        ValueError: If no query or data template is found for the given file.
    Returns:
        None
    """
    task_instance = kwargs.get('task_instance')
    if not task_instance:
        raise ValueError("task_instance is required in kwargs")

    file = kwargs.get('file')
    if not file:
        raise ValueError("file is required in kwargs")


    # Retrieve the XCom value and transform the data
    transformed_data_json = task_instance.xcom_pull(task_ids=f"transform_{kwargs['file']}")
    if not transformed_data_json:
        raise ValueError(f"No data found for task_id transform_{file}")


    transformed_data_json = json.loads(transformed_data_json) # TEST SI ERREUR


    # Get the query and the corresponding data
    query, data_template = queries.get(file)
    if not query or not data_template:
        raise ValueError(f"No query found for file {file}")


    # Connect to the database
    conn = connect_to_postgres()
    cursor = conn.cursor()

    # Push the data to the database
    try:
        for row in transformed_data_json:
            data = tuple(row[key] for key in data_template)
            cursor.execute(query, data)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()