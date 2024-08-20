"""
- Get next trains fort a specific train station
- Get train information for a specific train number and departure date.

"""


# LIB
from datetime import datetime
from dotenv import load_dotenv
from exceptiongroup import ExceptionGroup
import os
import pandas as pd
from typing import Optional

from data_updater import load_cleaned_feed_df




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
    # Create empty error list
    error_list: list[ValueError] = []

    # Get env variables
    gtfs_rt_url = os.getenv("GTFS_RT_URL")
    gtfs_storage_path = os.getenv("GTFS_STORAGE_PATH")
    clean_data_path = os.getenv("CLEAN_DATA_PATH")

    # If env variables not found, add error to list
    if gtfs_rt_url is None:
        error_list.append(CouldNotFindEnvVar("GTFS_RT_URL environment variable could not be found"))
    if gtfs_storage_path is None:
        error_list.append(CouldNotFindEnvVar("GTFS_STORAGE_PATH environment variable could not be found"))
    if clean_data_path is None:
        error_list.append(CouldNotFindEnvVar("CLEAN_DATA_PATH environment variable could not be found"))


    if error_list:
        raise ExceptionGroup("One or more errors occurred", error_list)


except ExceptionGroup as e:
    error =  CouldNotFindEnvVar("Some environment variables could not be found")
    error.add_notes = "Please check your .env file"
    raise error from e



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
            if station.lower() == chosen_station:
                datas.append({
                    'trip_headsign': trip_headsign,
                    'arrival_time': arrival_time,
                    'arrival_delay': arrival_delay,
                    'departure_time': departure_time,
                    'departure_delay': departure_delay
                })
    
    # Convert the list to a new DataFrame
    df = pd.DataFrame(datas, columns=['trip_headsign', 'arrival_time', 'arrival_delay', 'departure_time', 'departure_delay'])
    

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

    # Filter according to the next 'train_display_nb' trains
    df_departures = df_departures.head(train_display_nb)
    df_arrivals = df_arrivals.head(train_display_nb)


    return df_departures, df_arrivals



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


    try:
        action_choice = int(input("Choose an action:\n"
                                "1. Get next trains for a specific station\n"
                                "2. Get train information for a specific train number and departure date\n"))
        
        if action_choice == 1:
            chosen_station = input("Enter the station choice: ")
            chosen_station = chosen_station.lower()
            departure_or_arrival = input("Enter 'D' for Departures, 'A' for Arrivals: ")
            train_display_nb = int(input("Enter the number of trains to display: "))

            print("Loading station information:", chosen_station)

            df_departures, df_arrivals = display_station_info(chosen_station, train_display_nb)

            if departure_or_arrival == 'D':
                print(df_departures)
            elif departure_or_arrival == 'A':
                print(df_arrivals)


        elif action_choice == 2:
            train_nb = input("Enter the train number: ")
            train_nb = int(train_nb)
            departure_date = input("Enter the departure date (YYYY-MM-DD): ")

            if departure_date == "":
                departure_date = datetime.now().strftime("%Y-%m-%d")


            print("Loading train information:", train_nb, departure_date)

            df = get_train_informations(train_nb, departure_date)
            print(df)


        else:
            raise ValueError("Invalid action choice")


    except ValueError as e:
        print("An error occurred:", str(e))
    
    else:
        print("Action completed successfully")
    
    finally:
        print("End of program")