if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


# LIB
from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToDict
import requests
import time
import unittest
from unittest.mock import patch, Mock

# FUNCTION
@data_loader
def get_gtfs_rt_data(*args, **kwargs) -> gtfs_realtime_pb2.FeedMessage:
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


    # Args
    gtfs_rt_url:str="https://proxy.transport.data.gouv.fr/resource/sncf-tgv-gtfs-rt-trip-updates"

    # Get response
    response = fetch_data(gtfs_rt_url)

    # Parse GTFS RT Datas
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)
    
    # Convert FeedMessage to dictionary
    feed_dict = MessageToDict(feed)
    
    
    return feed_dict

# TEST
@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
    

    # Perform additional checks on the feed if necessary
    assert isinstance(output, dict), 'The output is not a valid dictionary'