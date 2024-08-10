"""
- 
"""



# LIB
import ast
from datetime import datetime, timedelta
from google.transit import gtfs_realtime_pb2
import json
import os
import pandas as pd
import requests

from gtfs_rt_updater import load_cleaned_feed_df

# VAR
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


def get_train_informations(train_nb:int, departure_date:str) -> pd.DataFrame:
    """

    """
    df = load_cleaned_feed_df(clean_data_path)


    df = df[(df['trip_headsign'] == train_nb) & (df['Departure Date'] == departure_date)]
 
    # Decompile Stops
    stops = []
    for stops_list in df["Stops"]:
        stops_list = ast.literal_eval(stops_list)
        for elt in stops_list:
            stop_name = elt[0]
            stop_arrival_time = elt[1]
            stop_arrival_delay = elt[2]
            stop_departure_time = elt[3]
            stop_departure_delay = elt[4]

            stops.append([stop_name, stop_arrival_time, stop_arrival_delay, stop_departure_time, stop_departure_delay])

    # Create DataFrame
    df = pd.DataFrame(stops, columns=['Stop Name', 'Arrival Time', 'Arrival Delay (secs)', 'Departure Time', 'Departure Delay (secs)'])


    return df



if __name__ == "__main__":
    train_nb = input("Enter the train number: ")
    train_nb = int(train_nb)
    departure_date = input("Enter the departure date (YYYY-MM-DD): ")
    df = get_train_informations(train_nb, departure_date)
    print(df)