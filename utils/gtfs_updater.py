"""
- Extract GTFS data from SNCF open data, unzip and save the files.
"""

# LIB
from dotenv import load_dotenv
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


try:
    gtfs_url = os.getenv("GTFS_URL")
    gtfs_storage_path = os.getenv("GTFS_STORAGE_PATH")
except:
    error =  CouldNotFindEnvVar("Some environment variables could not be found")
    error.add_note = "Please make sure that the following environment variables are set: GTFS_RT_URL, GTFS_STORAGE_PATH"
    raise error





# FUNCTIONS

def get_gtfs_files(url:str, gtfs_storage_path:str) -> None:
    """
    
    """

    try:
        # Get the file
        response = requests.get(url)
        
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
    df_stops = df_stops[['stop_name', 'stop_id']]

    # Get stop dictionary
    stop_dict = dict(zip(df_stops['stop_name'], df_stops['stop_id']))

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
    df_trips = df_trips[['trip_headsign', 'trip_id']]

    # Get trip dictionary
    trip_dict = dict(zip(df_trips['trip_headsign'], df_trips['trip_id']))

    # Save dictionary
    with open(trip_dict_filepath, "w") as file:
        json.dump(trip_dict, file)




if __name__ == "__main__":
    get_gtfs_files(url, gtfs_storage_path)
    get_stop_dictionary(gtfs_storage_path)
    get_trip_dictionary(gtfs_storage_path)
    print("GTFS data extracted and dictionaries generated")