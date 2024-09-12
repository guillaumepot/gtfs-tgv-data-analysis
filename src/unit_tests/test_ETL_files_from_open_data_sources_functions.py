"""
Test DAG - ETL - Get open data files from source
"""

# LIB
import os
import pandas as pd
import pytest
import sys
from unittest import mock
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.airflow.dags.ETL_files_from_open_data_sources_functions import download_file_from_url, transform_file, save_file


def load_json_as_df(json_data):
    # Mock implementation of load_json_as_df
    return pd.DataFrame(json_data)

def reverse_json_to_df(df):
    # Mock implementation of reverse_json_to_df
    return df.to_dict(orient='records')



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



@mock.patch('src.airflow.dags.ETL_files_from_open_data_sources_functions.load_json_as_df', side_effect=load_json_as_df)
@mock.patch('src.airflow.dags.ETL_files_from_open_data_sources_functions.reverse_json_to_df', side_effect=reverse_json_to_df)
def test_transform_file(mock_load_json_as_df, mock_reverse_json_to_df):
    # Mock task_instance and its xcom_pull method
    mock_task_instance = MagicMock()
    mock_task_instance.xcom_pull.return_value = [
        {'col1': 'value1', 'col2': 'value2', 'Commentaire annulations': 'test', 'Commentaire retards au départ': 'test', "Commentaire retards à l'arrivée": 'test'}
    ]

    # Test cases
    test_cases = [
        ('gares_de_voyageurs.csv', [{'col1': 'value1', 'col2': 'value2', 'Commentaire annulations': 'test', 'Commentaire retards au départ': 'test', "Commentaire retards à l'arrivée": 'test'}]),
        ('occupation_gares.csv', [{'col1': 'value1', 'col2': 'value2', 'Commentaire annulations': 'test', 'Commentaire retards au départ': 'test', "Commentaire retards à l'arrivée": 'test'}]),
        ('ponctualite_globale_tgv.csv', [{'col1': 'value1', 'col2': 'value2', 'Commentaire annulations': 'test', 'Commentaire retards au départ': 'test', "Commentaire retards à l'arrivée": 'test'}]),
        ('ponctualite_tgv_par_route.csv', [{'col1': 'value1', 'col2': 'value2'}])  # Expected output after dropping columns
    ]

    for file, expected_output in test_cases:
        result = transform_file(task_instance=mock_task_instance, file=file)
        assert result == expected_output

    # Test for file with no transformation function
    with mock.patch('logging.warning') as mock_warning:
        result = transform_file(task_instance=mock_task_instance, file='unknown_file.csv')
        assert result == []
        mock_warning.assert_called_once_with("No transformation function for file: unknown_file.csv.txt")

    # Test for missing task_instance
    with pytest.raises(ValueError, match="task_instance is required in kwargs"):
        transform_file(file='gares_de_voyageurs.csv')

    # Test for missing file
    with pytest.raises(ValueError, match="file is required in kwargs"):
        transform_file(task_instance=mock_task_instance)