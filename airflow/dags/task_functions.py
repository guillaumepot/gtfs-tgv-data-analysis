"""

"""


# LIB
import asyncpg
import datetime
from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToDict
import os
import requests


# VARS
postgres_host = os.getenv('DATA_PG_HOST', 'localhost')
postgres_port = os.getenv('DATA_PG_PORT', 5432)
postgres_user = os.getenv('DATA_PG_USER', 'root')
postgres_password = os.getenv('DATA_PG_PASSWORD', 'root')
postgres_db = os.getenv('DATA_PG_DB', 'train_delay_db')



# COMMON FUNCTIONS
async def connect_to_postgres() -> asyncpg.connection:
    """
    Connects to the database using the provided credentials.

    Returns:
        asyncpg.connection: The connection object representing the connection to the database.
    """

    return await asyncpg.connect(user=postgres_user,
                                   password=postgres_password,
                                   database=postgres_db,
                                   host=postgres_host,
                                   port=postgres_port)
            




# TASKS FUNCTIONS
def get_gtfs_rt_data(gtfs_rt_url:str) -> gtfs_realtime_pb2.FeedMessage:
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


    def fetch_data(url:str) -> requests.Response:
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
    
    
    return feed_dict


def transform_feed(feed_dict:dict) -> list:
    """
    Transform the feed dictionary into a list of trip data and stop times data.
    Parameters:
    - feed_dict (dict): The dictionary containing the feed data.
    Returns:
    - all_trip_data (list): A list of dictionaries containing trip data.
    - all_stop_times_data (list): A list of dictionaries containing stop times data.
    """
    # Remove Header key
    del feed_dict['header']
    # Remove entity key and keep the list value as dictionary
    feed_dict = feed_dict['entity']


    # Generate empty lists
    all_trip_data = []
    all_stop_times_data = []

    # Loop through the feed data
    for elt in feed_dict:
        trip_id = elt['tripUpdate']['trip']['tripId']
        departure_date = elt['tripUpdate']['trip']['startDate']
        departure_time = elt['tripUpdate']['trip']['startTime']
        updated_at = elt['tripUpdate']['timestamp']

        # Trip Data
        trip_data = {
            'trip_id': trip_id,
            'departure_date' : departure_date,
            'departure_time' : departure_time,
        }

        all_trip_data.append(trip_data)

        # Stop Times Data
        for stop_time_update in elt['tripUpdate']['stopTimeUpdate']:
            stop_id = stop_time_update['stopId']
            arrival_time = stop_time_update['arrival']['time'] if 'arrival' in stop_time_update else None
            departure_time = stop_time_update['departure']['time'] if 'departure' in stop_time_update else None
            delay_arrival = int(stop_time_update['arrival']['delay']/60 if 'arrival' in stop_time_update else 0)
            delay_departure = int(stop_time_update['departure']['delay']/60 if 'departure' in stop_time_update else 0)

            stop_times_data = {
                'trip_id': trip_id,
                'stop_id': stop_id,
                'arrival_time': arrival_time,
                'departure_time': departure_time,
                'delay_arrival': delay_arrival,
                'delay_departure': delay_departure,
                'update_time': updated_at,
            }
            
            all_stop_times_data.append(stop_times_data)

    return all_trip_data, all_stop_times_data


async def push_feed_data_to_db(feed_data:list, table:str) -> None:
    """
    
    """
    try:
        conn = await connect_to_postgres()
    except:
        raise ValueError("Error while connecting to the database")
    else:
        pass
    finally:
        conn.close()