from datetime import datetime, timedelta
from google.transit import gtfs_realtime_pb2
import json
import os
import pandas as pd
import requests


url = "https://proxy.transport.data.gouv.fr/resource/sncf-tgv-gtfs-rt-trip-updates" # TEMP
gtfs_storage_path="../datas/gtfs/" # TEMP
clean_data_path="../datas/cleaned/" # TEMP

available_conversion_dicts = ["stop_dict", "trip_dict"]


# FUTUR URL VAR DECLARATION
"""
try:
    url = os.getenv("GTFS_RT_URL")
    gtfs_storage_path = os.getenv("GTFS_STORAGE_PATH")
    clean_data_path = os.getenv("CLEAN_DATA_PATH")
except:
    raise ValueError("GTFS_STORAGE_PATH, GTFS_RT_URL or CLEAN_DATA_PATH environment variable not set")
 """




def get_gtfs_rt_data(url:str) -> gtfs_realtime_pb2.FeedMessage:
    """
    
    """

    feed = gtfs_realtime_pb2.FeedMessage()

    # Get feed
    try:
        response = requests.get(url)
        feed.ParseFromString(response.content)

    except requests.exceptions.RequestException as e:
        raise ValueError(f"Error while fetching GTFS-RT data: {e}")

    return feed



def get_conversion_dict(gtfs_storage_path:str, conversion_dict:str, invert:bool=True) -> dict:
    """
    
    """
    if conversion_dict not in available_conversion_dicts:
        raise ValueError(f"Conversion dictionary {conversion_dict} not available")

    # Get filepath
    dict_filepath = os.path.join(gtfs_storage_path, f"{conversion_dict}.json")

    # Load file as dictionary
    try:
        with open(dict_filepath, "r") as file:
            requested_dictionary = json.load(file)
    except Exception as e:
        print("An error occurred:", str(e))
    

    # Invert dictionary if requested
    if invert:
        inverted_dict = {v: k for k, v in requested_dictionary.items()}
        return inverted_dict

    return requested_dictionary




def convert_feed_to_dataframe(feed: gtfs_realtime_pb2.FeedMessage, gtfs_storage_path:str) -> pd.DataFrame:
    """
    
    """

    # Get stop dictionary
    stop_dict = get_conversion_dict(gtfs_storage_path, conversion_dict="stop_dict", invert=True)

    # Initialize empty list to store data
    data = []


    # Iterate over entities
    for entity in feed.entity:
        trip_id = entity.trip_update.trip.trip_id
        departure_time = entity.trip_update.trip.start_time
        departure_date = entity.trip_update.trip.start_date

        # Initialize stops and iterate over them
        stops = []
        for stop in entity.trip_update.stop_time_update:
            stop_id = stop.stop_id
            # Get stop_name based on stop_id dictionary
            stop_name = stop_dict.get(stop_id, stop_id)

            stop_arrival_time = stop.arrival.time
            stop_arrival_delay = stop.arrival.delay
            stop_departure_time = stop.departure.time
            stop_departure_delay = stop.departure.delay

            # Convert stop times to datetime
            if stop_arrival_time != 0:
                stop_arrival_time = datetime.fromtimestamp(stop_arrival_time).strftime('%H:%M:%S')
            if stop_departure_time != 0:
                stop_departure_time = datetime.fromtimestamp(stop_departure_time).strftime('%H:%M:%S')


            stops.append([stop_name, stop_arrival_time, stop_arrival_delay, stop_departure_time, stop_departure_delay])



        # Last stop is terminus, so arrival time is the last stop's arrival time
        arrival_time = stops[-1][1]

        # Append data    
        data.append([trip_id, departure_date, departure_time, arrival_time, stops])



    # Return DataFrame
    return pd.DataFrame(data, columns=['Trip ID', 'Departure Date', 'Departure Time', 'Arrival Time', 'Stops'])




def clean_feed_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    
    """
    # Add Origin and Destination columns based on first and last stop in Stops column
    df['Origin'] = df['Stops'].apply(lambda x: x[0][0])
    df['Destination'] = df['Stops'].apply(lambda x: x[-1][0])

    # Convert trip ID to Train number
    trips_dict = get_conversion_dict(gtfs_storage_path, conversion_dict="trip_dict", invert=True) # trip dict
    df['trip_headsign'] = df['Trip ID'].map(trips_dict) # get trip headsign from trip dict
    df = df.drop(columns=['Trip ID']) # drop trip id column
    df.dropna(subset=['trip_headsign'], inplace=True) # drop rows with NaN values in trip_headsign column
    df['trip_headsign'] = df['trip_headsign'].astype('int') # convert trip headsign to integer

    # Convert Departure Date to datetime
    df['Departure Date'] = pd.to_datetime(df['Departure Date'], format='%Y%m%d')

    # Reorganize columns order
    df = df[['trip_headsign', 'Departure Date', 'Departure Time', 'Origin', 'Arrival Time', 'Destination', 'Stops']]

    return df


def save_cleaned_feed_df(df: pd.DataFrame, gtfs_storage_path:str) -> None:
    """
    
    """
    # Save DataFrame
    df.to_csv(os.path.join(gtfs_storage_path, "cleaned_feed.csv"), index=False)


def load_cleaned_feed_df(gtfs_storage_path:str) -> pd.DataFrame:
    """
    
    """
    # Load DataFrame
    df = pd.read_csv(os.path.join(gtfs_storage_path, "cleaned_feed.csv"))

    return df



def update_feed() -> None:
    """
    
    """
    feed = get_gtfs_rt_data(url)
    df = convert_feed_to_dataframe(feed, gtfs_storage_path)
    df = clean_feed_dataframe(df)

    base_df = load_cleaned_feed_df(clean_data_path)

    # Join new dataset with base dataset
    new_df = pd.concat([base_df, df])
    # Drop duplicates based on trip_headsign and Departure Date columns, keeping the last one
    new_df.drop_duplicates(subset=['trip_headsign', 'Departure Date'], keep='last', inplace=True)

    # Save new dataset
    save_cleaned_feed_df(new_df, clean_data_path)


if __name__ == "__main__":
    update_feed()