"""
Test DAG - GTFS rt data ingestion
"""

# LIB
import json
import os
import pytest
import sys
from unittest import mock
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.airflow.dags.gtfs_rt_data_ingestion_functions import get_gtfs_rt_data, transform_feed, push_feed_data_to_db


# Test get_gtfs_rt_data
@patch('src.airflow.dags.gtfs_rt_data_ingestion_functions.requests.get')
def test_get_gtfs_rt_data(mock_get):
    # Mock response
    mock_response = MagicMock()
    mock_response.content = b'\x08\x02\x12\x07\n\x05Hello'
    mock_get.return_value = mock_response

    gtfs_rt_url = "http://example.com/gtfs-rt"
    result = get_gtfs_rt_data(gtfs_rt_url)

    assert isinstance(result, str)
    assert "entity" in result



# Test transform_feed
def test_transform_feed():
    # Mock task_instance and xcom_pull
    mock_task_instance = MagicMock()
    mock_task_instance.xcom_pull.return_value = json.dumps({
        "entity": [
            {
                "tripUpdate": {
                    "trip": {
                        "tripId": "12345",
                        "startDate": "20230101",
                        "startTime": "12:00:00"
                    },
                    "timestamp": 1672531200,
                    "stopTimeUpdate": [
                        {
                            "stopId": "stop1",
                            "arrival": {"time": 1672534800, "delay": 300},
                            "departure": {"time": 1672535400, "delay": 600}
                        },
                        {
                            "stopId": "stop2",
                            "arrival": {"time": 1672536000, "delay": 900},
                            "departure": {"time": 1672536600, "delay": 1200}
                        }
                    ]
                }
            }
        ]
    })

    kwargs = {'task_instance': mock_task_instance}

    expected_output = [
        {
            "trip_id": "12345",
            "departure_date": "20230101",
            "origin_departure_time": "12:00:00",
            "updated_at": "2023-01-01 00:00:00",
            "stop_id": "stop1",
            "stop_arrival_time": "2023-01-01 01:00:00",
            "stop_departure_time": "2023-01-01 01:10:00",
            "stop_delay_arrival": 5,
            "stop_delay_departure": 10
        },
        {
            "trip_id": "12345",
            "departure_date": "20230101",
            "origin_departure_time": "12:00:00",
            "updated_at": "2023-01-01 00:00:00",
            "stop_id": "stop2",
            "stop_arrival_time": "2023-01-01 01:20:00",
            "stop_departure_time": "2023-01-01 01:30:00",
            "stop_delay_arrival": 15,
            "stop_delay_departure": 20
        }
    ]

    result = transform_feed(**kwargs)

    assert result == expected_output


# Test push_feed_data_to_db
@patch('src.airflow.dags.gtfs_rt_data_ingestion_functions.connect_to_postgres')
def test_push_feed_data_to_db(mock_connect_to_postgres):
    # Mock the database connection and cursor
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_connect_to_postgres.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor

    # Mock task_instance and xcom_pull
    mock_task_instance = MagicMock()
    mock_task_instance.xcom_pull.return_value = [
        {
            "trip_id": "12345",
            "departure_date": "20230101",
            "origin_departure_time": "12:00:00",
            "updated_at": "2023-01-01 00:00:00",
            "stop_id": "stop1",
            "stop_arrival_time": "2023-01-01 01:00:00",
            "stop_departure_time": "2023-01-01 01:10:00",
            "stop_delay_arrival": 5,
            "stop_delay_departure": 10
        },
        {
            "trip_id": "12345",
            "departure_date": "20230101",
            "origin_departure_time": "12:00:00",
            "updated_at": "2023-01-01 00:00:00",
            "stop_id": "stop2",
            "stop_arrival_time": "2023-01-01 01:20:00",
            "stop_departure_time": "2023-01-01 01:30:00",
            "stop_delay_arrival": 15,
            "stop_delay_departure": 20
        }
    ]

    kwargs = {
        'task_instance': mock_task_instance,
        'table': 'trips_gtfs_rt'
    }

    # Call the function
    push_feed_data_to_db(**kwargs)

    # Verify that the cursor.execute method was called with the expected SQL and parameters
    expected_sql = """
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
                    """
    expected_params = [
        (
            "12345", "20230101", "12:00:00", "2023-01-01 00:00:00", "stop1",
            "2023-01-01 01:00:00", "2023-01-01 01:10:00", 5, 10
        ),
        (
            "12345", "20230101", "12:00:00", "2023-01-01 00:00:00", "stop2",
            "2023-01-01 01:20:00", "2023-01-01 01:30:00", 15, 20
        )
    ]

    assert mock_cursor.execute.call_count == 2
    mock_cursor.execute.assert_any_call(expected_sql, expected_params[0])
    mock_cursor.execute.assert_any_call(expected_sql, expected_params[1])

    # Verify that the connection commit and close methods were called
    mock_conn.commit.assert_called_once()
    mock_cursor.close.assert_called_once()
    mock_conn.close.assert_called_once()