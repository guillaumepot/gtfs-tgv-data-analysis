"""
Contains functions that are used by the tasks in the Airflow dags
"""


# LIB
from airflow.models import TaskInstance
import ast
from datetime import datetime, timezone
from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToDict
import json
import logging
import os
import pandas as pd
import psycopg2
import requests
from typing import Optional
import zipfile


from common_functions import connect_to_postgres


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



def load_gtfs_data_to_dataframe(**kwargs) -> dict:
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
    df_json = df.to_json(orient='records')

    # Send as XCom
    return {'df_json': df_json}



def data_transformer(**kwargs) -> list:
    """
    Transform the data based on the given file.
    Parameters:
    - kwargs: A dictionary containing the following key-value pairs:
        - file (str): The name of the file to be transformed.
    Returns:
    - cleaned_json_list (list): A list of transformed JSON strings.
    """
    
    # Functions to transform the data depending on the file
    def transform_routes(json_datas:dict) -> dict:
        """
        Transform the routes data.
        """

        # Load json datas as a dataframe
        df = pd.read_json(json_datas)

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

        # Convert the dataframe to a JSON string
        json_datas = df.to_json(orient='records')

        return json_datas


    def transform_calendar_dates(json_datas:dict) -> dict:
        """
        Transform the calendar dates data.
        """

        # Load json datas as a dataframe
        df = pd.read_json(json_datas)

        # Convert the dataframe to a JSON string
        json_datas = df.to_json(orient='records')

        return json_datas


    def transform_stops(json_datas:dict) -> dict:
        """
        Transform the stops data.
        """

        # Load json datas as a dataframe
        df = pd.read_json(json_datas)

        # Remove unnecessary columns
        df.drop(columns=['stop_desc', 'zone_id', 'stop_url'], inplace=True)

        # Convert the dataframe to a JSON string
        json_datas = df.to_json(orient='records')

        return json_datas


    def transform_stop_times(json_datas:dict) -> dict:
        """
        Transform the stop times data.
        """

        # Load json datas as a dataframe
        df = pd.read_json(json_datas)

        # Remove unnecessary columns
        df.drop(columns=['stop_headsign', 'shape_dist_traveled'], inplace=True)

        # Convert the dataframe to a JSON string
        json_datas = df.to_json(orient='records')

        return json_datas


    def transform_trips(json_datas:dict) -> dict:
        """
        Transform the trips data.
        """

        # Load json datas as a dataframe
        df = pd.read_json(json_datas)

        # Remove unnecessary columns
        df.drop(columns=['shape_id'], inplace=True)

        # Convert the dataframe to a JSON string
        json_datas = df.to_json(orient='records')

        return json_datas



    # Function logic

    task_instance = kwargs['task_instance']


    # Transform the data depending on the file
    if kwargs['file'] == 'agency':
        # Agency data not needed
        pass

    elif kwargs['file'] == 'calendar_dates':
        logging.info(f"Transforming calendar_dates data")
        # Retrieve the XCom value
        json_datas = task_instance.xcom_pull(task_ids=f"load_{kwargs['file']}")
        # Transform the data
        transformed_data = transform_calendar_dates(json_datas)
        logging.info(f"Calendar_dates data transformed")

    elif kwargs['file'] == 'feed_info':
        # Feed info data not needed
        pass

    elif kwargs['file'] == 'routes':
        logging.info(f"Transforming routes data")
        # Retrieve the XCom value
        json_datas = task_instance.xcom_pull(task_ids=f"load_{kwargs['file']}")
        # Transform the data
        transformed_data = transform_routes(json_datas)
        logging.info(f"Routes data transformed")

    elif kwargs['file'] == 'stops':
        logging.info(f"Transforming stops data")
        # Retrieve the XCom value
        json_datas = task_instance.xcom_pull(task_ids=f"load_{kwargs['file']}")
        # Transform the data
        transformed_data = transform_stops(json_datas)
        logging.info(f"Stops data transformed")

    elif kwargs['file'] == 'stop_times':
        logging.info(f"Transforming stop_times data")
        # Retrieve the XCom value
        json_datas = task_instance.xcom_pull(task_ids=f"load_{kwargs['file']}")
        # Transform the data
        transformed_data = transform_stop_times(json_datas)
        logging.info(f"Stop_times data transformed")

    elif kwargs['file'] == 'transfers':
        # Transfers data not needed
        pass

    elif kwargs['file'] == 'trips':
        logging.info(f"Transforming trips data")
        # Retrieve the XCom value
        json_datas = task_instance.xcom_pull(task_ids=f"load_{kwargs['file']}")
        # Transform the data
        transformed_data = transform_trips(json_datas))


    return transformed_data



def ingest_gtfs_data_to_database(**kwargs) -> None:
    """
    
    """

    # Queries
    queries = {routes: ""}



    """
    INSERT INTO routes (route_id, route_short_name, route_long_name, route_type, route_name)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (route_id)
    DO UPDATE SET
    route_short_name = EXCLUDED.route_short_name,
    route_long_name = EXCLUDED.route_long_name,
    route_type = EXCLUDED.route_type,
    route_name = EXCLUDED.route_name
    """, 
    (
    row["route_id"],
    row["route_short_name"],
    row["route_long_name"],
    row["route_type"],
    row["route_name"])



            
    # Retrieve the XCom value
    task_instance = kwargs['task_instance']
    data_to_ingest = task_instance.xcom_pull(task_ids=f"transform_{kwargs['file']}")


    # Initialize the connection and cursor
    conn = None
    cursor = None


    # Push data to the database
    try:
        # Connect to the database
        conn = connect_to_postgres()
        cursor = conn.cursor()
    # Raise an error if the connection fails
    except psycopg2.DatabaseError as e:
        logging.error(f"Database error: {e}")
        raise ValueError(f"Failed to push data to PostgreSQL: {e}")

    # Raise an error if an unexpected error occurs
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        raise ValueError(f"An unexpected error occurred: {e}")


    else:
        if kwargs['filename'] == "routes":
            for row in data_to_ingest:
                cursor.execute(query[filename], row)
        else:
            pass
        

        # Commit the transaction
        conn.commit()
    
    # Close the cursor and connection
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()