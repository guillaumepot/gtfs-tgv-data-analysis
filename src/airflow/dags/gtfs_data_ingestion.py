"""
This DAG is responsible for getting GTFS data from various sources, processing them, and storing them in a database.
"""


# LIB
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.filesystem import FileSensor


import datetime
import os

# Task functions
from gtfs_data_ingestion_functions import get_gtfs_files, data_cleaner, ingest_gtfs_data_to_database
from common_functions import clear_raw_files, load_url, load_df_from_file


# VARS
dag_scheduler = os.getenv('GTFS_INGESTION_SCHEDULER', None)
gtfs_storage_path = "/opt/airflow/storage/gtfs/"
gtfs_url = load_url("gtfs_url")




# Define the expected files the GTFS feed should have
expected_files = [
    "calendar_dates.txt",
    "routes.txt",
    "stops.txt",
    "stop_times.txt",
    "trips.txt"
]


# DAG
gtfs_ingestion_dag = DAG(
    dag_id = "gtfs_ingestion_dag",
    description = 'Get GTFS data from various sources, process them and store them in a database',
    tags = ['gtfs', 'download', 'files', 'data', 'ingestion', 'database'],
    catchup = False,
    schedule_interval =  dag_scheduler,
    start_date = days_ago(1),
    doc_md = """
    # GTFS Data Ingestion DAG
    This DAG is responsible for downloading GTFS data from various sources, processing the data, and storing it in a database.
    The DAG includes tasks for downloading files, checking their existence, loading them into dataframes, transforming the data, and ingesting it into the database.
    """
)



# Download files
get_gtfs_files = PythonOperator(
    task_id = 'get_gtfs_files',
    dag = gtfs_ingestion_dag,
    python_callable = get_gtfs_files,
    op_kwargs = {'gtfs_url': gtfs_url,
                 'gtfs_storage_path':gtfs_storage_path},
    retries = 3,
    retry_delay = datetime.timedelta(seconds=300),
    on_failure_callback=None,
    on_success_callback=None,
    trigger_rule='dummy',
    doc_md = """
    # Download GTFS Files
    This task downloads GTFS files from the specified URL and stores them in the designated storage path.
    """
    )



# Start parallel tasks for each file
file_sensors = []
df_loaders = []
data_transformers = []
data_ingesters = []


for file_name in expected_files:

    # Sensors: Check if the files are downloaded
    file_sensor = FileSensor(
        task_id=f"check_{file_name}",
        dag=gtfs_ingestion_dag, 
        fs_conn_id="fs_default",
        filepath=f'{gtfs_storage_path}{file_name}',
        poke_interval=60,
        timeout=180,
        mode='reschedule',
        trigger_rule='all_success',
        doc_md=f"""
        # Check File: {file_name}
        This task checks if the file {file_name} has been downloaded to the storage path.
        """
    )
    file_sensors.append(file_sensor)


    # Load the files into DF
    df_loader = PythonOperator(
        task_id=f'load_{file_name}',
        dag=gtfs_ingestion_dag,
        python_callable=load_df_from_file,
        op_kwargs={'filepath': gtfs_storage_path + file_name},
        retries=1,
        retry_delay=datetime.timedelta(seconds=90),
        on_failure_callback=None,
        on_success_callback=None,
        trigger_rule='none_failed',
        doc_md=f"""
        # Load File: {file_name}
        This task loads the file {file_name} into a dataframe for further processing.
        """
    )
    df_loaders.append(df_loader)



    # Remove .txt extension; this will be used as the base name for the file
    file_base_name = file_name.split('.')[0]  


    # Transform data
    data_transformer = PythonOperator(
        task_id=f'transform_{file_base_name}',
        dag=gtfs_ingestion_dag,
        python_callable=data_cleaner,
        provide_context=True,
        op_kwargs={'file': file_base_name},
        retries=1,
        retry_delay=datetime.timedelta(seconds=30),
        on_failure_callback=None,
        on_success_callback=None,
        trigger_rule='none_failed',
        doc_md=f"""
        # Transform Data: {file_base_name}
        This task transforms the data in the file {file_base_name} to prepare it for ingestion into the database.
        """
    )
    data_transformers.append(data_transformer)


    # Ingest data into the database
    data_ingestion= PythonOperator(
        task_id=f'ingest_{file_base_name}',
        dag=gtfs_ingestion_dag,
        python_callable=ingest_gtfs_data_to_database,
        provide_context=True,
        op_kwargs={'file': file_base_name},
        retries=3,
        retry_delay=datetime.timedelta(seconds=120),
        on_failure_callback=None,
        on_success_callback=None,
        trigger_rule='all_success',
        doc_md=f"""
        # Ingest Data: {file_base_name}
        This task ingests the transformed data from the file {file_base_name} into the database.
        """
    )
    data_ingesters.append(data_ingestion)



# Clear raw files
clear_raw_files = PythonOperator(
    task_id='clear_raw_files',
    dag=gtfs_ingestion_dag,
    python_callable=clear_raw_files,
    op_kwargs={'storage_path': gtfs_storage_path},
    retries=1,
    retry_delay=datetime.timedelta(seconds=180),
    on_failure_callback=None,
    on_success_callback=None,
    trigger_rule='all_done',
    doc_md="""
    # Clear Raw Files
    This task clears all raw files stored by the get_gtfs_files task.
    """
)



# DEPENDENCIES
get_gtfs_files >> file_sensors

for file_sensor, df_loader, data_transformer, data_ingestion in zip(file_sensors, df_loaders, data_transformers, data_ingesters):
    file_sensor >> df_loader >> data_transformer >> data_ingestion

data_ingesters >> clear_raw_files