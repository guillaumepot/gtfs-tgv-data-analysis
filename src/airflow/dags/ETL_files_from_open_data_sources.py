"""
DAG ETL - Get open data files from source
"""

# LIB
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.filesystem import FileSensor


import datetime
import os

# Task functions
from ETL_files_from_open_data_sources_functions import download_file_from_url, transform_file, save_file
from common_functions import load_url, clear_raw_files


# VARS
dag_scheduler = os.getenv('ETL_OPEN_DATA_FILES_SCHEDULER', None)
raw_files_storage_path = "/opt/airflow/storage/raw/"
clean_files_storage_path = "/opt/airflow/storage/clean/"

# Define the expected files the dag should download and process
expected_files = [
    "gares_de_voyageurs.csv",
    "occupation_gares.csv",
    "ponctualite_globale_tgv.csv",
    "ponctualite_tgv_par_route.csv"
]



# DAG
ETL_get_files_from_open_data = DAG(
    dag_id = "ETL_get_files_from_open_data",
    description = 'Get files from SNCF open data, process them and store them in storage',
    tags = ['download', 'files', 'data', 'storage', 'ETL'],
    catchup = False,
    schedule_interval =  dag_scheduler,
    start_date = days_ago(1),
    doc_md = """
    # ETL_get_files_from_open_data
    This DAG is responsible for downloading files from SNCF open data, processing them, and storing them in the designated storage.
    """
)



### TASKS ###
file_downloaders = []
file_sensors = []
file_transformers = []
file_savers = []

for file_name in expected_files:

    # Download files
    file_downloader = PythonOperator(
        task_id = f'download_{file_name}',
        dag = ETL_get_files_from_open_data,
        python_callable = download_file_from_url,
        op_kwargs = {'url': load_url(file_name),
                     'filename': file_name,
                     'storage_path': raw_files_storage_path},
        retries = 3,
        retry_delay = datetime.timedelta(seconds=120),
        on_failure_callback=None,
        on_success_callback=None,
        trigger_rule='dummy',
        doc_md = f"""
        # Download {file_name}
        This task downloads the file {file_name} from the specified URL and stores it in the raw files storage path.
        """
        )
    file_downloaders.append(file_downloader)



    # Sensors: Check if the files are downloaded
    file_sensor = FileSensor(
        task_id=f"check_{file_name}",
        dag=ETL_get_files_from_open_data, 
        fs_conn_id="fs_default",
        filepath=f'{raw_files_storage_path}{file_name}',
        poke_interval=60,
        timeout=180,
        mode='reschedule',
        trigger_rule='all_success',
        doc_md = f"""
        # Check {file_name}
        This task checks if the file {file_name} has been downloaded to the raw files storage path.
        """
    )
    file_sensors.append(file_sensor)


    
    # Transform files
    file_transformer = PythonOperator(
        task_id = f'transform_{file_name}',
        dag = ETL_get_files_from_open_data,
        python_callable = transform_file,
        op_kwargs = {'file': file_name},
        retries = 1,
        retry_delay = datetime.timedelta(seconds=30),
        on_failure_callback=None,
        on_success_callback=None,
        trigger_rule='all_success',
        doc_md = f"""
        # Transform {file_name}
        This task transforms the file {file_name} after it has been downloaded.
        """
        )
    file_transformers.append(file_transformer)


    # Save files
    file_saver = PythonOperator(
        task_id = f'save_{file_name}',
        dag = ETL_get_files_from_open_data,
        python_callable = save_file,
        op_kwargs = {'storage_path': clean_files_storage_path},
        retries = 1,
        retry_delay = datetime.timedelta(seconds=30),
        on_failure_callback=None,
        on_success_callback=None,
        trigger_rule='all_success',
        doc_md = f"""
        # Save {file_name}
        This task saves the transformed file {file_name} to the clean files storage path.
        """
        )
    file_savers.append(file_saver)



# Clear raw files
clear_raw_data_files = PythonOperator(
    task_id = 'clear_raw_data_files',
    dag = ETL_get_files_from_open_data,
    python_callable = clear_raw_files,
    op_kwargs = {'storage_path': raw_files_storage_path},
    retries = 1,
    retry_delay = datetime.timedelta(seconds=30),
    on_failure_callback=None,
    on_success_callback=None,
    trigger_rule='all_done',
    doc_md = """
    # Clear Raw Data Files
    This task clears the raw data files from the raw files storage path after all processing is complete.
    """
    )



# DEPENDENCIES
for file_downloader, file_sensor, file_transformer, file_saver in zip(file_downloaders, file_sensors, file_transformers, file_savers):
    file_downloader >> file_sensor >> file_transformer >> file_saver

file_savers >> clear_raw_data_files