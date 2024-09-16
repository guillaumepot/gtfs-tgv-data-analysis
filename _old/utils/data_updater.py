"""
- Extract GTFS Real Time (RT) from SNCF open data, append existing data and save the new dataset.
- Extract GTFS data from SNCF open data, unzip and save the files.
- Update the following datasets:


"""

# LIB
import ast
from datetime import datetime
from dotenv import load_dotenv
from exceptiongroup import ExceptionGroup
from google.transit import gtfs_realtime_pb2
import json
import os
import pandas as pd
import requests
from typing import Optional
import zipfile





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
    gtfs_url = os.getenv("GTFS_URL")
    gtfs_storage_path = os.getenv("GTFS_STORAGE_PATH")
    clean_data_path = os.getenv("CLEAN_DATA_PATH")
    raw_data_path = os.getenv("RAW_DATA_PATH")
    datasources_url_json_path = os.getenv("DATASOURCES_URL_JSON_PATH")

    # If env variables not found, add error to list
    if gtfs_rt_url is None:
        error_list.append(CouldNotFindEnvVar("GTFS_RT_URL environment variable could not be found"))
    if gtfs_url is None:
        error_list.append(CouldNotFindEnvVar("GTFS_URL environment variable could not be found"))
    if gtfs_storage_path is None:
        error_list.append(CouldNotFindEnvVar("GTFS_STORAGE_PATH environment variable could not be found"))
    if clean_data_path is None:
        error_list.append(CouldNotFindEnvVar("CLEAN_DATA_PATH environment variable could not be found"))
    if raw_data_path is None:
        error_list.append(CouldNotFindEnvVar("RAW_DATA_PATH environment variable could not be found"))
    if datasources_url_json_path is None:
        error_list.append(CouldNotFindEnvVar("DATASOURCES_URL_JSON_PATH environment variable could not be found"))

    if error_list:
        raise ExceptionGroup("One or more errors occurred", error_list)


except ExceptionGroup as e:
    error =  CouldNotFindEnvVar("Some environment variables could not be found")
    error.add_notes = "Please check your .env file"
    raise error from e



available_conversion_dicts = ["stop_dict", "trip_dict"]



# FUNCTIONS

def get_gtfs_rt_data(gtfs_rt_url:str) -> gtfs_realtime_pb2.FeedMessage:
    """
    
    """

    feed = gtfs_realtime_pb2.FeedMessage()

    # Get feed
    try:
        response = requests.get(gtfs_rt_url)
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
    feed = get_gtfs_rt_data(gtfs_rt_url)
    df = convert_feed_to_dataframe(feed, gtfs_storage_path)
    df = clean_feed_dataframe(df)
    print('df len:', len(df))
    save_cleaned_feed_df(df, clean_data_path)


def update_feed() -> None:
    """
    
    """
    # Get new data
    feed = get_gtfs_rt_data(gtfs_rt_url)
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


def get_gtfs_files(gtfs_url:str, gtfs_storage_path:str) -> None:
    """
    
    """

    try:
        # Get the file
        response = requests.get(gtfs_url)
        
        # Save the file
        with open("export_gtfs_voyages.zip", "wb") as file:
            file.write(response.content)
        
        # Extract the contents of the zip file
        with zipfile.ZipFile("export_gtfs_voyages.zip", "r") as zip_ref:
            zip_ref.extractall(gtfs_storage_path)

        # Delete the zip file
        os.remove("export_gtfs_voyages.zip")
            
    except Exception as e:
        print("An error occurred:", str(e))


def get_stop_dictionary(gtfs_storage_path:str) -> None:
    """

    """
    # Generate path
    stop_filepath = os.path.join(gtfs_storage_path, "stops.txt")
    stop_dict_filepath = os.path.join(gtfs_storage_path, "stop_dict.json")

    # Load file as dataframe
    df_stops = pd.read_csv(stop_filepath)

    # Filter columns
    df_stops = df_stops[['stop_id', 'stop_name']]

    # Get stop dictionary
    stop_dict = dict(zip(df_stops['stop_id'], df_stops['stop_name']))

    # Save dictionary
    with open(stop_dict_filepath, "w") as file:
        json.dump(stop_dict, file)


def get_trip_dictionary(gtfs_storage_path:str) -> None:
    """

    """
    # Generate path
    trip_filepath = os.path.join(gtfs_storage_path, "trips.txt")
    trip_dict_filepath = os.path.join(gtfs_storage_path, "trip_dict.json")

    # Load file as dataframe
    df_trips = pd.read_csv(trip_filepath)

    # Filter columns
    df_trips = df_trips[['trip_id', 'trip_headsign']]

    # Get trip dictionary
    trip_dict = dict(zip(df_trips['trip_id'], df_trips['trip_headsign']))

    # Save dictionary
    with open(trip_dict_filepath, "w") as file:
        json.dump(trip_dict, file)



def download_raw_datas(data_sources_url_json_path:str, raw_data_path:str) -> list:
    """

    """

    # Load the urls
    with open(data_sources_url_json_path, "r") as f:
        url_dict = json.load(f)



    # Download the files and add items to a new dictionary
    filepath_dict:dict[str,str] = {}

    for key, url in url_dict.items():
        file = requests.get(url)
        with open(f"{raw_data_path}/{key}.csv", "wb") as f:
            f.write(file.content)

        filepath_dict[key] = f"{raw_data_path}/{key}.csv"



    return filepath_dict




def clean_raw_datas(clean_data_path:str, raw_data_filepath_dict:dict[str,str]) -> None:
    """

    """

    def clean_train_stations(filepath) -> None:
        """
        
        """
        df = pd.read_csv(filepath,
                         header=0,
                         sep=";"
                         )

        df.loc[df['Nom'] == 'Neuilly Porte Maillot RER E', 'Position géographique'] = df.loc[df['Nom'] == 'Neuilly Porte Maillot RER E', 'Position géographique'].fillna('48.877983, 2.284516')
        df.dropna(subset=['Position géographique'], inplace=True)
        savepath = f"{clean_data_path}/train_stations.csv"
        df.to_csv(savepath, sep=";", index=False)



    def clean_station_occupation(filepath) -> None:
        """
        
        """
        df = pd.read_csv(filepath, sep=";")
        df.drop(columns=["Segmentation DRG"], inplace=True)
        savepath = f"{clean_data_path}/station_occupation.csv"
        df.to_csv(savepath, sep=";", index=False)

        

    def clean_tgv_ponctuality_global(filepath) -> None:
        """
        
        """
        df = pd.read_csv(filepath, sep=";")
        savepath = f"{clean_data_path}/tgv_ponctuality_global.csv"
        df.to_csv(savepath, sep=";", index=False)



    def clean_tgv_ponctuality_by_route(filepath) -> None:
        """
        
        """
        df = pd.read_csv(filepath, sep=";")
        df.drop(columns=["Commentaire annulations", "Commentaire retards au départ", "Commentaire retards à l'arrivée"], inplace=True)
        savepath = f"{clean_data_path}/tgv_ponctuality_by_route.csv"
        df.to_csv(savepath, sep=";", index=False)



    for key, value in raw_data_filepath_dict.items():
        if "train_stations" in key:
            clean_train_stations(value)
        if "station_occupation" in key:
            clean_station_occupation(value)
        if "tgv_ponctuality_global" in key:
            clean_tgv_ponctuality_global(value)
        if "tgv_ponctuality_by_route" in key:
            clean_tgv_ponctuality_by_route(value)





if __name__ == "__main__":

    try:
        action_choice = int(input("Choose an action to do: \n"
                            "1. (C)reate or (U)pdate feed\n"
                            "2. Update GTFS datas\n"
                            "3. Update Datasets\n"
                            "Enter the number of the action you want to do: "))
        
        if action_choice == 1:
            user_choice = input("Do you want to create or update the feed?: (c/u) ")
            if user_choice.lower() == "c":
                create_feed()
            elif user_choice.lower() == "u":
                update_feed()

        elif action_choice == 2:
            get_gtfs_files(gtfs_url, gtfs_storage_path)
            get_stop_dictionary(gtfs_storage_path)
            get_trip_dictionary(gtfs_storage_path)
            print("GTFS data extracted and dictionaries generated")

        elif action_choice == 3:
            print("Datasets update in progress...")
            filepath_dict = download_raw_datas(datasources_url_json_path, raw_data_path)
            clean_raw_datas(clean_data_path, filepath_dict)
            print("Datasets updated")


        else:
            raise ValueError("Invalid action choice")
        
    except ValueError as e:
        print("An error occurred:", str(e))
    
    else:
        print("Action completed successfully")
    
    finally:
        print("End of program")