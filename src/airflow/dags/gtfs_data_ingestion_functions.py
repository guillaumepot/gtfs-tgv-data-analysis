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
    
    """

    try:
        # Get the file
        response = requests.get(gtfs_url)
        
        # Save the file
        zip_file_path = os.path.join(gtfs_storage_path, "export_gtfs_voyages.zip")
        with open(zip_file_path, "wb") as file:
            file.write(response.content)
        
        # Extract the contents of the zip file
        with zipfile.ZipFile("export_gtfs_voyages.zip", "r") as zip_ref:
            zip_ref.extractall(gtfs_storage_path)

        # Delete the zip file
        os.remove(zip_file_path)
            
    except Exception as e:
        raise Exception(f"Error while getting the GTFS files: {e}")



def load_gtfs_data_to_dataframe(**kwargs) -> str:
    """
    
    """
    if kwargs['file'] == 'routes':
        # Load the routes.txt file
        df = pd.read_csv(kwargs['gtfs_file_path'])
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
        }

        df['route_name'] = df['route_type'].apply(lambda x: transports[x] if x in transports else 'unknown')

        # Convert to json
        df_json = df.to_json(orient='records')

        return df_json

    else:
        raise Exception(f"File {kwargs['file']} not supported or doesn't exist")
    


def ingest_gtfs_data_to_database(**kwargs) -> None:
    """
    
    """
    # Retrieve the XCom value
    task_instance = kwargs['task_instance']
    data_to_ingest = task_instance.xcom_pull(task_ids='gtfs_routes_loader')

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
        # routes_gtfs table
        if kwargs['table'] == "routes_gtfs":
            # Loops [..]
            for row in data_to_ingest:
                cursor.execute(f"""
                    INSERT INTO {kwargs['table']} (
                    route_id, route_short_name, route_long_name, route_type, route_name
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (route_id)
                    DO UPDATE SET
                    route_short_name = EXCLUDED.route_short_name,
                    route_long_name = EXCLUDED.route_long_name,
                    route_type = EXCLUDED.route_type,
                    route_name = EXCLUDED.route_name
                    """, (
                        row["route_id"],
                        row["route_short_name"],
                        row["route_long_name"],
                        row["route_type"],
                        row["route_name"]
                    ))

              
        # Wrong table name
        else:
            raise ValueError(f"Invalid table name: {kwargs['table']}")
        

        # Commit the transaction
        conn.commit()

    
    # Close the cursor and connection
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()