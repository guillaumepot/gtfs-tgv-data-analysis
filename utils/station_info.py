"""
- Get next trains fort a specific train station
"""


# LIB
from datetime import datetime
from dotenv import load_dotenv
import os
import pandas as pd
from typing import Optional

from gtfs_rt_updater import load_cleaned_feed_df


# EXCEPTION CLASS
class CouldNotFindEnvVar(ValueError):
    def __init__(self, message:Optional[str]=None):
        if message is None:
            message = "Could not find environment variable"


# VAR
# Load environment variables file
load_dotenv(dotenv_path='./url.env')


try:
    url = os.getenv("GTFS_RT_URL")
    gtfs_storage_path = os.getenv("GTFS_STORAGE_PATH")
    clean_data_path = os.getenv("CLEAN_DATA_PATH")
except:
    error =  CouldNotFindEnvVar("Some environment variables could not be found")
    error.add_note = "Please make sure that the following environment variables are set: GTFS_RT_URL, GTFS_STORAGE_PATH, CLEAN_DATA_PATH"
    raise error



# FUNCTIONS

import pandas as pd
from datetime import datetime

def display_station_info(chosen_station: str, train_display_nb: int = 10, filter_from_now=True) -> pd.DataFrame:
    """
    Display the next train departures from a given station.
    """


    # Load the cleaned feed DataFrame
    df_cleaned = load_cleaned_feed_df(clean_data_path)

    print(df_cleaned['Stops'].dtype)

    datas = []


    # Iterate through each row in the DataFrame
    for index, row in df_cleaned.iterrows():
        trip_headsign = row['trip_headsign']
        stops = row['Stops']
    


    
        # Iterate through each stop in Stops
        for stop in stops:
            # Debug statement to inspect the stop data
            station, arrival_time, arrival_delay, departure_time, departure_delay = stop
            if station == chosen_station:
                datas.append({
                    'trip_headsign': trip_headsign,
                    'arrival_time': arrival_time,
                    'arrival_delay': arrival_delay,
                    'departure_time': departure_time,
                    'departure_delay': departure_delay
                })
    
    # Convert the list to a new DataFrame
    df = pd.DataFrame(datas, columns=['trip_headsign', 'arrival_time', 'arrival_delay', 'departure_time', 'departure_delay'])
    
    # Filter according to the next 'train_display_nb' trains
    df = df.head(train_display_nb)

    # Generate df_departures and df_arrivals
    df_departures = df.drop(columns=['arrival_time', 'arrival_delay'])
    df_departures = df_departures[df_departures['departure_time'] != 0]
    df_departures['departure_time'] = pd.to_datetime(df_departures['departure_time'], format='%H:%M:%S').dt.time

    df_arrivals = df.drop(columns=['departure_time', 'departure_delay'])
    df_arrivals = df_arrivals[df_arrivals['arrival_time'] != 0]
    df_arrivals['arrival_time'] = pd.to_datetime(df_arrivals['arrival_time'], format='%H:%M:%S').dt.time

    if filter_from_now:
        # Get the current time without microseconds
        now = datetime.now().time()

        # Filter departures and arrivals to include only times from now
        df_departures = df_departures[df_departures['departure_time'] >= now]
        df_arrivals = df_arrivals[df_arrivals['arrival_time'] >= now]


    return df_departures, df_arrivals




if __name__ == "__main__":
    chosen_station = input("Enter the station choice: ")
    departure_or_arrival = input("Enter 'D' for Departures, 'A' for Arrivals: ")
    train_display_nb = int(input("Enter the number of trains to display: "))

    print("Loading station information:", chosen_station)

    df_departures, df_arrivals = display_station_info(chosen_station, train_display_nb)

    if departure_or_arrival == 'D':
        print(df_departures)
    elif departure_or_arrival == 'A':
        print(df_arrivals)