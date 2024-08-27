"""
Test DAG - gtfs rt data ingestion
"""

# LIB
import os
import pytest
import sys
from unittest import mock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.airflow.dags.gtfs_rt_data_ingestion_functions import connect_to_postgres, get_gtfs_rt_data, transform_feed, push_feed_data_to_db


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
    # Adjust the Unix timestamps to match the expected datetime strings in UTC
    timestamp_updated_at = 1672564800  # Corresponds to '2023-01-01 12:00:00' UTC
    timestamp_arrival = timestamp_updated_at + 300  # 5 minutes later
    timestamp_departure = timestamp_updated_at + 600  # 10 minutes later

    feed_json = f'''
    {{
        "entity": [
            {{
                "tripUpdate": {{
                    "trip": {{
                        "tripId": "123",
                        "startDate": "20230101",
                        "startTime": "12:00:00"
                    }},
                    "stopTimeUpdate": [
                        {{
                            "stopId": "stop1",
                            "arrival": {{
                                "time": {timestamp_arrival},
                                "delay": 60
                            }},
                            "departure": {{
                                "time": {timestamp_departure},
                                "delay": 120
                            }}
                        }}
                    ],
                    "timestamp": {timestamp_updated_at}
                }}
            }}
        ]
    }}
    '''

    task_instance = mock.Mock()
    task_instance.xcom_pull.return_value = feed_json
    kwargs = {'task_instance': task_instance}
    
    all_trip_data = transform_feed(**kwargs)


    # Assertions
    assert len(all_trip_data) == 1
    assert all_trip_data[0]['trip_id'] == '123'
    assert all_trip_data[0]['departure_date'] == '20230101'
    assert all_trip_data[0]['origin_departure_time'] == '12:00:00'
    assert all_trip_data[0]['updated_at'] == '2023-01-01 09:20:00'
    assert all_trip_data[0]['stop_id'] == 'stop1'
    assert all_trip_data[0]['stop_arrival_time'] == '2023-01-01 09:25:00'
    assert all_trip_data[0]['stop_departure_time'] == '2023-01-01 09:30:00'
    assert all_trip_data[0]['stop_delay_arrival'] == 1
    assert all_trip_data[0]['stop_delay_departure'] == 2



def test_push_feed_data_to_db():
    # Mock the connect_to_postgres function
    with mock.patch('src.airflow.dags.gtfs_rt_data_ingestion_functions.connect_to_postgres') as mock_connect:
        # Mock the connection and cursor
        mock_conn = mock.Mock()
        mock_cursor = mock.Mock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        # Mock the task_instance.xcom_pull method
        task_instance = mock.Mock()
        task_instance.xcom_pull.return_value = [
            {
                "trip_id": "123",
                "departure_date": "20230101",
                "origin_departure_time": "12:00:00",
                "updated_at": "2023-01-01 09:20:00",
                "stop_id": "stop1",
                "stop_arrival_time": "2023-01-01 09:25:00",
                "stop_departure_time": "2023-01-01 09:30:00",
                "stop_delay_arrival": 1,
                "stop_delay_departure": 2
            }
        ]

        # Call the function with the necessary arguments
        push_feed_data_to_db(task_instance=task_instance, table="trips_gtfs_rt")

        # Assertions
        mock_cursor.execute.assert_called_once_with(
            """
                    INSERT INTO trips_gtfs_rt (
                    trip_id, departure_date, origin_departure_time, updated_at,
                    stop_id, stop_arrival_time, stop_departure_time,
                    stop_delay_arrival, stop_delay_departure
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (trip_id, departure_date, stop_id)
                    DO UPDATE SET
                        origin_departure_time = EXCLUDED.origin_departure_time,
                        updated_at = EXCLUDED.updated_at,
                        stop_arrival_time = EXCLUDED.stop_arrival_time,
                        stop_departure_time = EXCLUDED.stop_departure_time,
                        stop_delay_arrival = EXCLUDED.stop_delay_arrival,
                        stop_delay_departure = EXCLUDED.stop_delay_departure;
                    """, (
                "123", "20230101", "12:00:00", "2023-01-01 09:20:00", "stop1",
                "2023-01-01 09:25:00", "2023-01-01 09:30:00", 1, 2
            )
        )
        mock_conn.commit.assert_called_once()
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()