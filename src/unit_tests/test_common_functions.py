"""
Test DAG - Common functions
"""

# LIB
import os
import pytest
import sys
import json
from unittest import mock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.airflow.dags.common_functions import load_url, connect_to_postgres, clear_raw_files



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