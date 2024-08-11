"""
- Extract GTFS Real Time (RT) from SNCF open data, append existing data and save the new dataset.
"""

# LIB
import ast
from datetime import datetime, timedelta
from dotenv import load_dotenv
from google.transit import gtfs_realtime_pb2
import json
import os
import pandas as pd
import requests
from typing import Optional


# EXCEPTION CLASS
class CouldNotFindEnvVar(ValueError):
    def __init__(self, message:Optional[str]=None):
        if message is None:
            message = "Could not find environment variable"


# VAR
# Load environment variables file
load_dotenv(dotenv_path='./url.env')


# Get environment variables
try:
    url = os.getenv("GTFS_RT_URL")
    gtfs_storage_path = os.getenv("GTFS_STORAGE_PATH")
    clean_data_path = os.getenv("CLEAN_DATA_PATH")
except:
    error =  CouldNotFindEnvVar("Some environment variables could not be found")
    error.add_note = "Please make sure that the following environment variables are set: GTFS_RT_URL, GTFS_STORAGE_PATH, CLEAN_DATA_PATH"
    raise error


available_conversion_dicts = ["stop_dict", "trip_dict"]



# FUNCTIONS

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



def get_conversion_dict(gtfs_storage_path:str, conversion_dict:str) -> dict:
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
    

    return requested_dictionary




def convert_feed_to_dataframe(feed: gtfs_realtime_pb2.FeedMessage, gtfs_storage_path:str) -> pd.DataFrame:
    """
    
    """

    # Get stop dictionary
    stop_dict = get_conversion_dict(gtfs_storage_path, conversion_dict="stop_dict")

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
    trips_dict = get_conversion_dict(gtfs_storage_path, conversion_dict="trip_dict") # trip dict
    df['trip_headsign'] = df['Trip ID'].map(trips_dict) # get trip headsign from trip dict
    df = df.drop(columns=['Trip ID']) # drop trip id column
    df.dropna(subset=['trip_headsign'], inplace=True) # drop rows with NaN values in trip_headsign column
    df['trip_headsign'] = df['trip_headsign'].astype('int') # convert trip headsign to integer

    # Convert Departure Date to datetime
    df['Departure Date'] = pd.to_datetime(df['Departure Date'], format='%Y%m%d').dt.date

    # Reorganize columns order
    df = df[['trip_headsign', 'Departure Date', 'Departure Time', 'Origin', 'Arrival Time', 'Destination', 'Stops']]

    return df


def save_cleaned_feed_df(df: pd.DataFrame, clean_data_path:str) -> None:
    """
    
    """
    # Save DataFrame
    df.to_csv(os.path.join(clean_data_path, "cleaned_feed.csv"), index=False)


def load_cleaned_feed_df(clean_data_path:str) -> pd.DataFrame:
    """
    
    """
    # Load DataFrame
    df = pd.read_csv(os.path.join(clean_data_path, "cleaned_feed.csv"))

    # Convert Stops column to list of lists
    df['Stops'] = df['Stops'].apply(ast.literal_eval)

    return df



def create_feed() -> None:
    """
    
    """
    feed = get_gtfs_rt_data(url)
    df = convert_feed_to_dataframe(feed, gtfs_storage_path)
    df = clean_feed_dataframe(df)
    print('df len:', len(df))
    save_cleaned_feed_df(df, clean_data_path)




def update_feed() -> None:
    """
    
    """
    # Get new data
    feed = get_gtfs_rt_data(url)
    df_new_data = convert_feed_to_dataframe(feed, gtfs_storage_path)
    df_new_data = clean_feed_dataframe(df_new_data)

    # Load existing data
    base_df = load_cleaned_feed_df(clean_data_path)

    # Append new data to existing data
    df = pd.concat([base_df, df_new_data], ignore_index=True)

    # Convert Departure Date to datetime for proper comparison
    df['Departure Date'] = pd.to_datetime(df['Departure Date'])
    # Remove duplicated based on trip_headsign and Departure Date columns
    df = df.drop_duplicates(subset=['trip_headsign', 'Departure Date'], keep='last')

    # Save new data
    save_cleaned_feed_df(df, clean_data_path)





if __name__ == "__main__":
    user_choice = input("Do you want to create or update the feed?: (c/u) ")
    if user_choice.lower() == "c":
        create_feed()
    elif user_choice.lower() == "u":
        update_feed()