#!/venv/bin/activate


# Lib
from datetime import datetime
import folium
from google.transit import gtfs_realtime_pb2
import json
import os
import pandas as pd
import requests
import shutil
import zipfile


# Var
temp_dir = './data_temp/'
gtfs_storage_path:str="./data_temp/"

available_conversion_dicts = ["stop_dict", "trip_dict"]

gtfs_url:str="https://eu.ftp.opendatasoft.com/sncf/gtfs/export_gtfs_voyages.zip"
gtfs_rt_url="https://proxy.transport.data.gouv.fr/resource/sncf-tgv-gtfs-rt-trip-updates"


# Func - Task 1
def get_gtfs_files(gtfs_url:str=gtfs_url, temp_dir:str=temp_dir) -> None:
    """
    Fetch the GTFS files
    """
    zip_path = os.path.join(temp_dir, "export_gtfs_voyages.zip")

    try:
        # Get the file
        response = requests.get(gtfs_url)
        
        # Save the file
        with open(zip_path, "wb") as file:
            file.write(response.content)
        
        # Extract the contents of the zip file
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(temp_dir)

        # Delete the zip file
        os.remove(zip_path)
            
    except Exception as e:
        print("An error occurred:", str(e))


def get_conversion_dictionaries(gtfs_storage_path:str=gtfs_storage_path) -> None:
    """
    Create the stop and trip dictionaries
    """
    # Generate paths
    stop_filepath = os.path.join(gtfs_storage_path, "stops.txt")
    trip_filepath = os.path.join(gtfs_storage_path, "trips.txt")

    # Load files as dataframe
    df_stops = pd.read_csv(stop_filepath)
    df_trips = pd.read_csv(trip_filepath)

    # Filter columns
    df_stops = df_stops[['stop_id', 'stop_name']]
    df_trips = df_trips[['trip_id', 'trip_headsign']]

    # Get dictionaries
    stop_dict = dict(zip(df_stops['stop_id'], df_stops['stop_name']))
    trip_dict = dict(zip(df_trips['trip_id'], df_trips['trip_headsign']))

    # Save dictionaries
    return stop_dict, trip_dict



# Func - Task 2
def get_gtfs_rt_data(gtfs_rt_url:str) -> gtfs_realtime_pb2.FeedMessage:
    """
    Fetch GTFS RT feeds from source url
    """
    feed = gtfs_realtime_pb2.FeedMessage()
    # Get feed
    try:
        response = requests.get(gtfs_rt_url)
        feed.ParseFromString(response.content)
    except requests.exceptions.RequestException as e:
        raise ValueError(f"Error while fetching GTFS-RT data: {e}")

    return feed


def convert_feed_to_dataframe(feed: gtfs_realtime_pb2.FeedMessage, stop_dict:dict) -> pd.DataFrame:
    """
    Converts Feed to a DF, convert some informations such as stop_id to stop_name
    """
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


def clean_feed_dataframe(df: pd.DataFrame, trip_dict:dict) -> pd.DataFrame:
    """
    Cleen the feed DF
    """
    # Add Origin and Destination columns based on first and last stop in Stops column
    df['Origin'] = df['Stops'].apply(lambda x: x[0][0])
    df['Destination'] = df['Stops'].apply(lambda x: x[-1][0])

    # Convert trip ID to Train number
    df['trip_headsign'] = df['Trip ID'].map(trip_dict) # get trip headsign from trip dict
    df = df.drop(columns=['Trip ID']) # drop trip id column
    df.dropna(subset=['trip_headsign'], inplace=True) # drop rows with NaN values in trip_headsign column
    df['trip_headsign'] = df['trip_headsign'].astype('int') # convert trip headsign to integer

    # Convert Departure Date to datetime
    df['Departure Date'] = pd.to_datetime(df['Departure Date'], format='%Y%m%d').dt.date

    # Reorganize columns order
    df = df[['trip_headsign', 'Departure Date', 'Departure Time', 'Origin', 'Arrival Time', 'Destination', 'Stops']]

    return df



# Func - Updater
def updater():
    # Task 0
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)

    # Task 1
    print("Starting GTFS Data Fetching and cleaning...")
    get_gtfs_files()
    stops = pd.read_csv(temp_dir + "stops.txt")
    stop_dict, trip_dict = get_conversion_dictionaries()
    print("GTFS Data Fetching and cleaning done.")

    # Task 2
    print("Starting GTFS-RT [Real Time] Data Fetching and cleaning...")
    # Fetch Feed
    feed = get_gtfs_rt_data(gtfs_rt_url)

    # Get cleaned Feed as DF
    feed_df = convert_feed_to_dataframe(feed, stop_dict)
    cleaned_feed_df = clean_feed_dataframe(feed_df, trip_dict)
    print("GTFS-RT Data Fetching and cleaning done.")

    # Task 0
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)

    return feed, feed_df, cleaned_feed_df, stop_dict, trip_dict, stops


# Func - save_datas
def save_datas(save_dir:str) -> None:
    """
    Save GTFS & GTFS RT data into a directory
    """
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)

    save_operations = [
        ("feed.pb", feed.SerializeToString(), "wb"),
        ("feed_df.csv", feed_df.to_csv(index=False), "w"),
        ("cleaned_feed_df.csv", cleaned_feed_df.to_csv(index=False), "w"),
        ("stop_dict.json", json.dumps(stop_dict), "w"),
        ("trip_dict.json", json.dumps(trip_dict), "w")
    ]

    for filename, data, mode in save_operations:
        file_path = os.path.join(save_dir, filename)
        with open(file_path, mode) as f:
            f.write(data)

# Func - display_station_info
def display_station_info(cleaned_feed_df:pd.DataFrame, chosen_station: str, train_display_nb: int = 10, filter_from_now=True) -> pd.DataFrame:
    """
    Display the next train departures from a given station.
    """
    print(cleaned_feed_df['Stops'].dtype)

    datas = []

    # Iterate through each row in the DataFrame
    for index, row in cleaned_feed_df.iterrows():
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
    
    # Return if data is empty
    if not datas:
        station_list = sorted(cleaned_feed_df['Stops'].apply(lambda x: x[0][0]).unique())
        print("Available stations are:", station_list)
        raise ValueError("No data found for the specified station or station not found")

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


# Func - get_train_informations
def get_train_informations(cleaned_feed_df:pd.DataFrame, train_nb:int, departure_date:str) -> pd.DataFrame:
    """
    Get train information for a specific train number and departure date.
    """
    cleaned_feed_df['Departure Date'] = pd.to_datetime(cleaned_feed_df['Departure Date']).dt.strftime('%Y-%m-%d')
    departure_date = pd.to_datetime(departure_date).strftime('%Y-%m-%d')
    train_df = cleaned_feed_df[(cleaned_feed_df['trip_headsign'] == train_nb) & (cleaned_feed_df['Departure Date'] == departure_date)]

    if train_df.empty:
        unique_trips = cleaned_feed_df[['trip_headsign', 'Departure Date']].drop_duplicates()
        train_list = sorted(list(zip(unique_trips['trip_headsign'], unique_trips['Departure Date'])))
        for elt in train_list:
            print(f"Train number: {elt[0]}, Departure Date: {elt[1]}")
        raise ValueError("No data found for the specified train number and departure date")

    # Decompile Stops
    stops = []
    for stops_list in train_df["Stops"]:
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


# Func - generate_map
def generate_map(df:pd.DataFrame, stops:pd.DataFrame, train_nb:int) -> None:
    """
    Generate a map with the train stops
    """
    # Getting map center
    center_lat = stops['stop_lat'].mean()
    center_lon = stops['stop_lon'].mean()
    map = folium.Map(location=[center_lat, center_lon], zoom_start=6)

    # Add markers for each stop
    train_stops = df['Stop Name'].unique()
    stop_datas = []
    for stop in train_stops:
        stop_lat = stops[stops['stop_name'] == stop]['stop_lat'].values[0]
        stop_lon = stops[stops['stop_name'] == stop]['stop_lon'].values[0]
        stop_datas.append([stop, stop_lat, stop_lon])

    stops_datas_df = pd.DataFrame(stop_datas, columns=['stop_name', 'stop_lat', 'stop_lon'])

    for idx, row in stops_datas_df.iterrows():
        folium.Marker(
            location=[row['stop_lat'], row['stop_lon']],
            popup=row['stop_name'],
            tooltip=row['stop_name']
        ).add_to(map)

    # Add lines connecting the stops
    coordinates = stops_datas_df[['stop_lat', 'stop_lon']].values.tolist()
    folium.PolyLine(locations=coordinates, color='blue').add_to(map)

    # Save the map to an HTML file
    map.save(f'{train_nb}_map.html')                      




if __name__ == "__main__":
    print("Starting updater process...")
    feed, feed_df, cleaned_feed_df, stop_dict, trip_dict, stops = updater()

    try:
        print("Updated datas available, do you want to save them ? (y/n)")
        save_choice = input()

    except:
        print("Invalid input, datas not saved, continue without saving")
        save_choice = "n"

    else:
        if save_choice == "y":
            print("Saving datas...")
            print("Datas will be saved in the following directory: ./saved_datas/")
            save_dir = "./saved_datas/"
            save_datas(save_dir)

        else:
            print("Datas not saved, continue without saving")

    finally:
        try:
            action_choice = int(input("Choose an action:\n"
                                    "1. Get next trains for a specific station\n"
                                    "2. Get train information for a specific train number and departure date\n"))

            if action_choice == 1:
                pass
                chosen_station = input("Enter the station choice: ")
                chosen_station = chosen_station.lower()
                departure_or_arrival = input("Enter 'D' for Departures, 'A' for Arrivals: ")
                #train_display_nb = int(input("Enter the number of trains to display: "))

                print("Loading station information:", chosen_station)

                df_departures, df_arrivals = display_station_info(cleaned_feed_df, chosen_station, train_display_nb=25)
                if departure_or_arrival == 'D':
                    print(df_departures)
                elif departure_or_arrival == 'A':
                    print(df_arrivals)


            elif action_choice == 2:
                pass
                train_nb = input("Enter the train number: ")
                train_nb = int(train_nb)
                departure_date = input("Enter the departure date (YYYY-MM-DD): ")

                if departure_date == "":
                    departure_date = datetime.now().strftime("%Y-%m-%d")

                print("Loading train information:", train_nb, departure_date)
                df = get_train_informations(cleaned_feed_df, train_nb, departure_date)
                print(df)

                print("Generate map? (y/n)")
                map_choice = input()
                if map_choice == "y":
                    print("Generating map")
                    generate_map(df, stops, train_nb)

                else:
                    print("Skipping map generation")

            else:
                raise ValueError("Invalid action choice")


        except ValueError as e:
            print("An error occurred:", str(e))
        else:
            print("Action completed successfully")
        finally:
            print("End of program")