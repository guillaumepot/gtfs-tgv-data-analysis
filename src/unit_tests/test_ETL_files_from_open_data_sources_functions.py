"""
Test DAG - ETL - Get open data files from source
"""

# LIB
import json
import os
import pytest
import requests
import sys
from unittest import mock
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.airflow.dags.ETL_files_from_open_data_sources_functions import download_file_from_url



@patch('requests.get')
@patch('builtins.open', new_callable=mock.mock_open)
def test_download_file_from_url(mock_open, mock_get):
    # Test successful download
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.content = b'Test content'
    mock_get.return_value = mock_response

    url = 'http://example.com/testfile'
    filename = 'testfile'
    storage_path = '/fakepath'

    download_file_from_url(url, filename, storage_path)

    mock_get.assert_called_once_with(url)
    mock_open.assert_called_once_with(os.path.join(storage_path, filename), 'wb')
    mock_open().write.assert_called_once_with(b'Test content')

    # Test failed download
    mock_get.reset_mock()
    mock_open.reset_mock()
    mock_response.status_code = 404 
    mock_get.return_value = mock_response
    with pytest.raises(Exception) as excinfo:
        download_file_from_url(url, filename, storage_path)

    assert str(excinfo.value) == f"Failed to download file from {url} ; status code: {mock_response.status_code}"
    mock_get.assert_called_once_with(url)
    mock_open.assert_not_called()



