# LIB
import os
import pytest
import sys
from unittest import mock



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
            database='train_delay_db',
            host='localhost',
            port= 5432
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
    all_trip_data, all_stop_times_data = transform_feed(feed_json)
    
    assert len(all_trip_data) == 1
    assert all_trip_data[0]['trip_id'] == '123'
    assert len(all_stop_times_data) == 1
    assert all_stop_times_data[0]['stop_id'] == 'stop1'



def test_push_feed_data_to_db_trips():
    feed_data = [
        {
            'trip_id': '123',
            'departure_date': '20230101',
            'departure_time': '12:00:00'
        }
    ]
    table = 'trips_gtfs_rt'

    mock_conn = mock.Mock()
    with mock.patch('src.airflow.dags.gtfs_data_ingestion_functions.connect_to_postgres', return_value=mock_conn):
        push_feed_data_to_db(feed_data, table)

        mock_conn.execute.assert_called_once_with(
            """
                    INSERT INTO trips_gtfs_rt (trip_id, departure_date, departure_time)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (trip_id, departure_date)
                    DO UPDATE SET departure_time = EXCLUDED.departure_time
                    WHERE trips_gtfs_rt.departure_time <> EXCLUDED.departure_time;
                """,
            '123', '20230101', '12:00:00'
        )
        mock_conn.close.assert_called_once()



def test_push_feed_data_to_db_stop_times():
    feed_data = [
        {
            'trip_id': '123',
            'stop_id': 'stop1',
            'arrival_time': 1672531200,
            'departure_time': 1672531800,
            'delay_arrival': 60,
            'delay_departure': 120,
            'update_time': 1672531200
        }
    ]
    table = 'stop_time_update_gtfs_rt'

    mock_conn = mock.Mock()
    with mock.patch('src.airflow.dags.gtfs_data_ingestion_functions.connect_to_postgres', return_value=mock_conn):
        push_feed_data_to_db(feed_data, table)

        mock_conn.execute.assert_called_once_with(
            """
                    INSERT INTO stop_time_update_gtfs_rt (trip_id, stop_id, arrival_time, departure_time, delay_arrival, delay_departure, update_time)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    ON CONFLICT (trip_id, stop_id)
                    DO UPDATE SET arrival_time = EXCLUDED.arrival_time,
                                  departure_time = EXCLUDED.departure_time,
                                  delay_arrival = EXCLUDED.delay_arrival,
                                  delay_departure = EXCLUDED.delay_departure,
                                  update_time = EXCLUDED.update_time
                    WHERE stop_time_update_gtfs_rt.arrival_time <> EXCLUDED.arrival_time
                    OR stop_time_update_gtfs_rt.departure_time <> EXCLUDED.departure_time
                    OR stop_time_update_gtfs_rt.delay_arrival <> EXCLUDED.delay_arrival
                    OR stop_time_update_gtfs_rt.delay_departure <> EXCLUDED.delay_departure;
                """,
            '123', 'stop1', 1672531200, 1672531800, 60, 120, 1672531200
        )
        mock_conn.close.assert_called_once()