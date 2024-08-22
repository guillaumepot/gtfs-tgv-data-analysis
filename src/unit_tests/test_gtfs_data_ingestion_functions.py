# LIB
import os
import pytest
import sys
from unittest import mock
import requests
import json

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.airflow.dags.gtfs_data_ingestion_functions import connect_to_postgres, get_gtfs_rt_data, transform_feed, push_feed_data_to_db


# VAR
# Mock environment variables
@mock.patch.dict('os.environ', {
    'DATA_PG_HOST': 'localhost',
    'DATA_PG_PORT': '5432',
    'DATA_PG_USER': 'root',
    'DATA_PG_PASSWORD': 'root',
    'DATA_PG_DB': 'train_delay_db'
})

# TESTS
def test_connect_to_postgres():
    with mock.patch('psycopg2.connect') as mock_connect:
        connect_to_postgres()

        mock_connect.assert_called_once_with(
            user='root',
            password='root',
            dbname='train_delay_db',
            host='localhost',
            port='5432'
        )


def test_get_gtfs_rt_data():
    gtfs_rt_url = "http://example.com/gtfs-rt"
    mock_response = mock.Mock()
    mock_response.content = b'\x08\x02\x12\x07\x08\x01\x10\x01\x18\x01'
    
    with mock.patch('requests.get', return_value=mock_response):
        with mock.patch('google.transit.gtfs_realtime_pb2.FeedMessage.ParseFromString') as mock_parse:
            mock_parse.return_value = None
            result = get_gtfs_rt_data(gtfs_rt_url)
            assert isinstance(result, str)


def test_transform_feed():
    feed_json = '{"entity": [{"tripUpdate": {"trip": {"tripId": "123", "startDate": "20230101", "startTime": "12:00:00"}, "stopTimeUpdate": [{"stopId": "stop1", "arrival": {"time": 1672531200, "delay": 60}, "departure": {"time": 1672531800, "delay": 120}}], "timestamp": 1672531200}}]}'
    task_instance = mock.Mock()
    task_instance.xcom_pull.return_value = feed_json
    kwargs = {'task_instance': task_instance}
    
    all_trip_data, all_stop_times_data = transform_feed(**kwargs)
    
    assert len(all_trip_data) == 1
    assert all_trip_data[0]['trip_id'] == '123'
    assert len(all_stop_times_data) == 1
    assert all_stop_times_data[0]['stop_id'] == 'stop1'
    assert all_stop_times_data[0]['trip_id'] == '123'