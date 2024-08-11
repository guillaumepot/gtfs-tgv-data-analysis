"""
- Get train information for a specific train number and departure date.
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

from gtfs_rt_updater import load_cleaned_feed_df


# EXCEPTION CLASS
class CouldNotFindEnvVar(ValueError):
    def __init__(self, message:Optional[str]=None):
        if message is None:
            message = "Could not find environment variable"


# VAR

try:
    url = os.getenv("GTFS_RT_URL")
    gtfs_storage_path = os.getenv("GTFS_STORAGE_PATH")
    clean_data_path = os.getenv("CLEAN_DATA_PATH")
except:
    error =  CouldNotFindEnvVar("Some environment variables could not be found")
    error.add_note = "Please make sure that the following environment variables are set: GTFS_RT_URL, GTFS_STORAGE_PATH, CLEAN_DATA_PATH"
    raise error


# FUNCTIONS

def get_train_informations(train_nb:int, departure_date:str) -> pd.DataFrame:
    """
    Get train information for a specific train number and departure date.
    """
    df = load_cleaned_feed_df(clean_data_path)

    df = df[(df['trip_headsign'] == train_nb) & (df['Departure Date'] == departure_date)]
    
    if df.empty:
        raise ValueError("No data found for the specified train number and departure date")
    
    # Decompile Stops
    stops = []
    for stops_list in df["Stops"]:
        for elt in stops_list:
            stop_name = elt[0]
            stop_arrival_time = elt[1]
            stop_arrival_delay = int(elt[2] / 60) # Convert seconds to minutes
            stop_departure_time = elt[3]
            stop_departure_delay = int(elt[4] / 60) # Convert seconds to minutes

            stops.append([stop_name, stop_arrival_time, stop_arrival_delay, stop_departure_time, stop_departure_delay])

    # Create DataFrame
    df = pd.DataFrame(stops, columns=['Stop Name', 'Arrival Time', 'Arrival Delay (mins)', 'Departure Time', 'Departure Delay (mins)'])

    return df



if __name__ == "__main__":
    train_nb = input("Enter the train number: ")
    train_nb = int(train_nb)
    departure_date = input("Enter the departure date (YYYY-MM-DD): ")

    if departure_date == "":
        departure_date = datetime.now().strftime("%Y-%m-%d")


    print("Loading train information:", train_nb, departure_date)

    df = get_train_informations(train_nb, departure_date)
    print(df)