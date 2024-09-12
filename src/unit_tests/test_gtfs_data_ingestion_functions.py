"""
Test DAG - GTFS data ingestion
"""

# LIB
import os
import pandas as pd
import pytest
from unittest import mock

from src.airflow.dags.gtfs_data_ingestion_functions import get_gtfs_files, data_cleaner, ingest_gtfs_data_to_database


def test_get_gtfs_files():
    gtfs_url = "http://example.com/gtfs.zip"
    gtfs_storage_path = "/tmp/gtfs"

    # Mock requests.get
    mock_response = mock.Mock()
    mock_response.status_code = 200
    mock_response.content = b"fake content"
    with mock.patch("requests.get", return_value=mock_response) as mock_get:
        # Mock open
        with mock.patch("builtins.open", mock.mock_open()) as mock_file:
            # Mock zipfile.ZipFile
            with mock.patch("zipfile.ZipFile") as mock_zip:
                mock_zip.return_value.__enter__.return_value.extractall = mock.Mock()
                # Mock os.remove
                with mock.patch("os.remove") as mock_remove:
                    get_gtfs_files(gtfs_url, gtfs_storage_path)

                    # Assertions
                    mock_get.assert_called_once_with(gtfs_url)
                    mock_file.assert_called_once_with(os.path.join(gtfs_storage_path, "export_gtfs_voyages.zip"), "wb")
                    mock_zip.assert_called_once_with(os.path.join(gtfs_storage_path, "export_gtfs_voyages.zip"), "r")
                    mock_remove.assert_called_once_with(os.path.join(gtfs_storage_path, "export_gtfs_voyages.zip"))



def test_data_cleaner():
    task_instance = mock.Mock()
    file = "routes"
    json_data = '[{"route_id": 1, "route_type": 2, "agency_id": "1", "route_desc": "desc", "route_url": "url", "route_color": "color", "route_text_color": "text_color"}]'
    expected_transformed_json = '[{"route_id":1,"route_type":2,"route_name":"rail"}]'

    # Mock task_instance.xcom_pull
    task_instance.xcom_pull.return_value = json_data

    # Mock pandas.read_json
    mock_df = pd.DataFrame({
        'route_id': [1],
        'route_type': [2],
        'agency_id': ['1'],
        'route_desc': ['desc'],
        'route_url': ['url'],
        'route_color': ['color'],
        'route_text_color': ['text_color']
    })
    with mock.patch("pandas.read_json", return_value=mock_df) as mock_read_json:
        # Mock DataFrame.to_json
        with mock.patch.object(pd.DataFrame, "to_json", return_value=expected_transformed_json) as mock_to_json:
            result = data_cleaner(task_instance=task_instance, file=file)

            # Assertions
            task_instance.xcom_pull.assert_called_once_with(task_ids=f"load_{file}.txt")
            mock_read_json.assert_called_once_with(json_data)
            mock_to_json.assert_called_once_with(orient='records')
            assert result == expected_transformed_json



def test_data_cleaner_missing_task_instance():
    with pytest.raises(ValueError, match="task_instance is required in kwargs"):
        data_cleaner(file="routes")



def test_data_cleaner_missing_file():
    task_instance = mock.Mock()
    with pytest.raises(ValueError, match="file is required in kwargs"):
        data_cleaner(task_instance=task_instance)



def test_ingest_gtfs_data_to_database_missing_task_instance():
    with pytest.raises(ValueError, match="task_instance is required in kwargs"):
        ingest_gtfs_data_to_database(file="routes")



def test_ingest_gtfs_data_to_database_missing_file():
    task_instance = mock.Mock()
    with pytest.raises(ValueError, match="file is required in kwargs"):
        ingest_gtfs_data_to_database(task_instance=task_instance)
        ingest_gtfs_data_to_database(task_instance=task_instance)