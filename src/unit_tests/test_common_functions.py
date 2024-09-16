# src/unit_tests/test_common_functions.py
"""
Test DAG - Common functions
"""

# LIB
import json
import os
import pandas as pd
import pytest
from unittest import mock


from src.airflow.dags.common_functions import load_url, load_df_from_file, connect_to_postgres, clear_raw_files, load_json_as_df, reverse_json_to_df



# Test load_url function
def test_load_url():
    mock_sources = {
        "file1": "http://example.com/file1",
        "file2": "http://example.com/file2"
    }
    with mock.patch("builtins.open", mock.mock_open(read_data=json.dumps(mock_sources))):
        assert load_url("file1") == "http://example.com/file1"
        assert load_url("file2") == "http://example.com/file2"
        with pytest.raises(KeyError):
            load_url("file3")

# Test load_df_from_file function
def test_load_df_from_file_file_not_found():
    filepath = "/tmp/non_existent_file.csv"

    # Mock os.path.exists
    with mock.patch("os.path.exists", return_value=False) as mock_exists:
        with pytest.raises(FileNotFoundError):
            load_df_from_file(filepath=filepath)

            # Assertions
            mock_exists.assert_called_once_with(filepath)



# Test connect_to_postgres function
@mock.patch.dict(os.environ, {
    "DATA_PG_USER": "test_user",
    "DATA_PG_PASSWORD": "test_password",
    "DATA_PG_DB": "test_db",
    "DATA_PG_HOST": "test_host",
    "DATA_PG_PORT": "5432"
})
@mock.patch("psycopg2.connect")
def test_connect_to_postgres(mock_connect):
    connect_to_postgres()
    mock_connect.assert_called_once_with(
        user="test_user",
        password="test_password",
        dbname="test_db",
        host="test_host",
        port="5432"
    )



# Test clear_raw_files function
@mock.patch("os.listdir")
@mock.patch("os.path.isfile")
@mock.patch("os.remove")
def test_clear_raw_files(mock_remove, mock_isfile, mock_listdir):
    mock_listdir.return_value = ["file1", "file2", "file3"]
    mock_isfile.side_effect = lambda x: True

    clear_raw_files("/mock/path")

    mock_remove.assert_any_call("/mock/path/file1")
    mock_remove.assert_any_call("/mock/path/file2")
    mock_remove.assert_any_call("/mock/path/file3")
    assert mock_remove.call_count == 3



# Test load_json_as_df function
def test_load_json_as_df():
    json_data = {
        "name": ["Alice", "Bob"],
        "age": [25, 30]
    }
    df = load_json_as_df(json_data)
    expected_df = pd.DataFrame(json_data)
    pd.testing.assert_frame_equal(df, expected_df)



# Test reverse_json_to_df function
def test_reverse_json_to_df():
    data = {
        "name": ["Alice", "Bob"],
        "age": [25, 30]
    }
    df = pd.DataFrame(data)
    json_data = reverse_json_to_df(df)
    expected_json_data = df.to_json(orient='records')
    assert json_data == expected_json_data