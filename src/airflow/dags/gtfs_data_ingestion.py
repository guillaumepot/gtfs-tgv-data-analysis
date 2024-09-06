"""
DAG GTFS Ingestion
"""


# LIB
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.filesystem import FileSensor


import datetime
import os

# Task functions
from gtfs_data_ingestion_functions import get_gtfs_files, load_gtfs_data_from_file, data_cleaner, ingest_gtfs_data_to_database
from common_functions import clear_raw_files, load_url


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
    # WIP
            """)




### TASKS ###

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
    # WIP
    """
    )



# Start parrallel tasks for each file
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
        trigger_rule='all_success'
    )
    file_sensors.append(file_sensor)


    # Load the files into DF
    df_loader = PythonOperator(
        task_id=f'load_{file_name}',
        dag=gtfs_ingestion_dag,
        python_callable=load_gtfs_data_from_file,
        op_kwargs={'filepath': gtfs_storage_path + file_name},
        retries=1,
        retry_delay=datetime.timedelta(seconds=90),
        on_failure_callback=None,
        on_success_callback=None,
        trigger_rule='none_failed',
        doc_md="""
        # WIP
        """
    )
    df_loaders.append(df_loader)



    # Remove .txt extension; this will be used as the base name for the file
    file_base_name = file_name.split('.')[0]  


    # Transform datas
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
        doc_md="""
        # WIP
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
        doc_md="""
        # WIP
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