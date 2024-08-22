"""
Contains functions that are used by the tasks in the Airflow dags
"""


# LIB
from airflow.models import TaskInstance
from datetime import datetime
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
        asyncpg.connection: The connection object representing the connection to the database.
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
            return response
        
        except requests.exceptions.RequestException as e:
            return None


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
    Transform the feed dictionary into a list of trip data and stop times data.
    
    Parameters:
    - feed_dict (dict): The dictionary containing the feed data.
    
    Returns:
    - tuple: A tuple containing two lists:
        - all_trip_data (list): A list of dictionaries containing trip data.
        - all_stop_times_data (list): A list of dictionaries containing stop times data.
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
    all_stop_times_data = []

    # Loop through the feed data
    for elt in feed_dict:
        trip_id = elt["tripUpdate"]["trip"]["tripId"]
        departure_date = elt["tripUpdate"]["trip"]["startDate"]
        departure_time = elt["tripUpdate"]["trip"]["startTime"]
        updated_at = elt["tripUpdate"]["timestamp"]

        # Convert Unix timestamp to datetime string to inject into the PG database
        updated_at = int(updated_at)
        updated_at = datetime.utcfromtimestamp(updated_at).strftime('%Y-%m-%d %H:%M:%S')


        # Trip Data
        trip_data = {
            "trip_id": trip_id,
            "departure_date": departure_date,
            "departure_time": departure_time,
        }

        all_trip_data.append(trip_data)

        # Stop Times Data
        for stop_time_update in elt["tripUpdate"]["stopTimeUpdate"]:
            stop_id = stop_time_update["stopId"]
            arrival_time = stop_time_update["arrival"]["time"] if "arrival" in stop_time_update else None
            departure_time = stop_time_update["departure"]["time"] if "departure" in stop_time_update else None
            delay_arrival = int(stop_time_update["arrival"]["delay"]/60 if "arrival" in stop_time_update else 0)
            delay_departure = int(stop_time_update["departure"]["delay"]/60 if "departure" in stop_time_update else 0)

            # Convert Unix timestamps to datetime strings
            if arrival_time:
                arrival_time = datetime.utcfromtimestamp(int(arrival_time)).strftime('%Y-%m-%d %H:%M:%S')
            if departure_time:
                departure_time = datetime.utcfromtimestamp(int(departure_time)).strftime('%Y-%m-%d %H:%M:%S')



            stop_times_data = {
                "trip_id": trip_id,
                "stop_id": stop_id,
                "arrival_time": arrival_time,
                "departure_time": departure_time,
                "delay_arrival": delay_arrival,
                "delay_departure": delay_departure,
                "update_time": updated_at,
            }
            
            all_stop_times_data.append(stop_times_data)

    return all_trip_data, all_stop_times_data





def push_feed_data_to_db(**kwargs) -> None:
    """

    """
    # Retrieve the XCom value
    task_instance = kwargs['task_instance']
    feed_data = task_instance.xcom_pull(task_ids='transform_feed_gtfs_rt')[0] if kwargs['table'] == 'trips_gtfs_rt' else task_instance.xcom_pull(task_ids='transform_feed_gtfs_rt')[1]

    logging.info(f'feed_data first trips: {feed_data[:3]}')

    try:
        with connect_to_postgres() as conn:
            with conn.cursor() as cursor:
                # trip table update
                if kwargs['table']  == "trips_gtfs_rt":
                    for trip in feed_data:
                        cursor.execute(f"""
                            INSERT INTO {kwargs['table'] } (trip_id, departure_date, departure_time)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (trip_id, departure_date)
                            DO UPDATE SET departure_time = EXCLUDED.departure_time
                            WHERE {kwargs['table'] }.departure_time <> EXCLUDED.departure_time;
                        """, (trip["trip_id"], trip["departure_date"], trip["departure_time"]))

                # stop_time_update table update
                elif kwargs['table']  == "stop_time_update_gtfs_rt":
                    for stop_time in feed_data:
                        cursor.execute(f"""
                            INSERT INTO {kwargs['table'] } (trip_id, stop_id, arrival_time, departure_time, delay_arrival, delay_departure, update_time)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (trip_id, stop_id)
                            DO UPDATE SET arrival_time = EXCLUDED.arrival_time,
                                          departure_time = EXCLUDED.departure_time,
                                          delay_arrival = EXCLUDED.delay_arrival,
                                          delay_departure = EXCLUDED.delay_departure,
                                          update_time = EXCLUDED.update_time
                            WHERE {kwargs['table'] }.arrival_time <> EXCLUDED.arrival_time
                            OR {kwargs['table'] }.departure_time <> EXCLUDED.departure_time
                            OR {kwargs['table'] }.delay_arrival <> EXCLUDED.delay_arrival
                            OR {kwargs['table'] }.delay_departure <> EXCLUDED.delay_departure;
                        """, (
                            stop_time["trip_id"], stop_time["stop_id"], stop_time["arrival_time"], 
                            stop_time["departure_time"], stop_time["delay_arrival"], 
                            stop_time["delay_departure"], stop_time["update_time"]
                        ))

            conn.commit()
    
    except psycopg2.DatabaseError as e:
        logging.error(f"Database error: {e}")
        raise ValueError(f"Failed to push data to PostgreSQL: {e}")

    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        raise ValueError(f"An unexpected error occurred: {e}")