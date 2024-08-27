"""
Contains functions that are used by the tasks in the Airflow dags
"""


# LIB
from airflow.models import TaskInstance
from datetime import datetime, timezone
from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToDict
import json
import logging
import os
import psycopg2
import requests



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
            




# TASKS FUNCTIONS
def get_gtfs_rt_data(gtfs_rt_url: str) -> str:
    """
    Fetches GTFS-RT data from the given URL.
    Args:
        gtfs_rt_url (str): The URL to fetch the GTFS-RT data from.
        verbose (bool, optional): Whether to print verbose output. Defaults to True.
    Returns:
        gtfs_realtime_pb2.FeedMessage: The parsed GTFS-RT data.
    Raises:
        ValueError: If an error occurs while fetching the GTFS-RT data.
    """


    # Function to fetch data
    def fetch_data(url: str) -> requests.Response:
        """
        Fetches GTFS-RT data from the given URL.
        Args:
            url (str): The URL to fetch the data from.
        Returns:
            requests.Response: The response object containing the fetched data.
        Raises:
            requests.exceptions.RequestException: If an error occurs while fetching the data.
        """
        try:
            response = requests.get(url)
            response.raise_for_status()
        
        except requests.exceptions.RequestException as e:
            logging.error(f"An error occurred while fetching the data: {e}")
            raise ValueError(f"Failed to fetch data: {e}")
        
        else:
            return response

    # Get response
    response = fetch_data(gtfs_rt_url)
    # Parse GTFS RT Datas
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)
    # Convert FeedMessage to dictionary
    feed_dict = MessageToDict(feed)
    # Serialize dictionary to JSON string
    feed_json = json.dumps(feed_dict)
    
    return feed_json



def transform_feed(**kwargs) -> list:
    """
    Transform the GTFS Real-Time feed data into a list of trip data and stop times data.
    Parameters:
    - kwargs: A dictionary of keyword arguments.
    Returns:
    - all_trip_data: A list of dictionaries containing trip data.
    - all_stop_times_data: A list of dictionaries containing stop times data.
    """

    # Retrieve the XCom value
    task_instance = kwargs['task_instance']
    feed_json = task_instance.xcom_pull(task_ids='get_feed_gtfs_rt')

    # Convert string to dictionary (from Xcom, the feed_dict is a string)
    feed_dict = json.loads(feed_json)

    # Remove entity key and keep the list value as dictionary
    feed_dict = feed_dict["entity"]

    # Generate empty lists
    all_trip_data = []
    trip_data = {} # Empty dictionary to store trip data

    # Loop through the feed data
    for elt in feed_dict:
        trip_id = elt["tripUpdate"]["trip"]["tripId"]
        departure_date = elt["tripUpdate"]["trip"]["startDate"]
        origin_departure_time = elt["tripUpdate"]["trip"]["startTime"]
        updated_at = elt["tripUpdate"]["timestamp"]

        # Convert Unix timestamp to datetime string to inject into the PG database
        updated_at = int(updated_at)
        updated_at = datetime.fromtimestamp(updated_at, timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

        # Stop Times Data
        for stop_time_update in elt["tripUpdate"]["stopTimeUpdate"]:
            stop_id = stop_time_update["stopId"]
            stop_arrival_time = stop_time_update["arrival"]["time"] if "arrival" in stop_time_update else None
            stop_departure_time = stop_time_update["departure"]["time"] if "departure" in stop_time_update else None
            stop_delay_arrival = int(stop_time_update["arrival"]["delay"]/60 if "arrival" in stop_time_update else 0)
            stop_delay_departure = int(stop_time_update["departure"]["delay"]/60 if "departure" in stop_time_update else 0)

            # Convert Unix timestamps to datetime strings
            if stop_arrival_time is not None:
                stop_arrival_time = datetime.fromtimestamp(int(stop_arrival_time), timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

            if stop_departure_time is not None:
                stop_departure_time = datetime.fromtimestamp(int(stop_departure_time), timezone.utc).strftime('%Y-%m-%d %H:%M:%S')


            trip_data = {
                "trip_id": trip_id,
                "departure_date": departure_date,
                "origin_departure_time": origin_departure_time,
                "updated_at": updated_at,
                "stop_id": stop_id,
                "stop_arrival_time": stop_arrival_time,
                "stop_departure_time": stop_departure_time,
                "stop_delay_arrival": stop_delay_arrival,
                "stop_delay_departure": stop_delay_departure
            }

            all_trip_data.append(trip_data)
            trip_data = {}

    return all_trip_data



def push_feed_data_to_db(**kwargs) -> None:
    """
    Pushes feed data to the database.
    Args:
        **kwargs: Arbitrary keyword arguments.
    Returns:
        None
    Raises:
        ValueError: If failed to push data to PostgreSQL or an unexpected error occurs.
        psycopg2.DatabaseError: If there is a database error.    
    """
    # Retrieve the XCom value
    task_instance = kwargs['task_instance']
    all_trip_data = task_instance.xcom_pull(task_ids='transform_feed_gtfs_rt')

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
        # Trips GTFS RT table update
        if kwargs['table'] == "trips_gtfs_rt":
            # Loop through the feed data and insert or update the data in the database
            for trip_row in all_trip_data:
                cursor.execute(f"""
                    INSERT INTO {kwargs['table']} (
                    trip_id, departure_date, origin_departure_time, updated_at,
                    stop_id, stop_arrival_time, stop_departure_time,
                    stop_delay_arrival, stop_delay_departure
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (trip_id, departure_date, stop_id)
                    DO UPDATE SET
                        origin_departure_time = EXCLUDED.origin_departure_time,
                        updated_at = EXCLUDED.updated_at,
                        stop_arrival_time = EXCLUDED.stop_arrival_time,
                        stop_departure_time = EXCLUDED.stop_departure_time,
                        stop_delay_arrival = EXCLUDED.stop_delay_arrival,
                        stop_delay_departure = EXCLUDED.stop_delay_departure;
                    """, (
                        trip_row["trip_id"], 
                        trip_row["departure_date"], 
                        trip_row["origin_departure_time"],
                        trip_row["updated_at"], 
                        trip_row["stop_id"], 
                        trip_row["stop_arrival_time"], 
                        trip_row["stop_departure_time"], 
                        trip_row["stop_delay_arrival"], 
                        trip_row["stop_delay_departure"]
                ))


            # Commit the transaction
            conn.commit()

        # Wrong table name
        else:
            raise ValueError(f"Invalid table name: {kwargs['table']}")

    
    # Close the cursor and connection
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()