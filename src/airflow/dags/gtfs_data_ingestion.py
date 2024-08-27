"""
DAG GTFS Ingestion
"""


# LIB
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator


import datetime
import os

# task functions
from gtfs_data_ingestion_functions import get_gtfs_files, load_gtfs_data_to_dataframe, ingest_gtfs_data_to_database



# VARS
dag_scheduler = os.getenv('GTFS_INGESTION_SCHEDULER', None)
gtfs_storage_path = "/opt/airflow/storage/gtfs/"
gtfs_url = "https://eu.ftp.opendatasoft.com/sncf/gtfs/export_gtfs_voyages.zip"

# DAG
gtfs_ingestion_dag = DAG(
    dag_id = "gtfs_ingestion_dag",
    description = 'Get GTFS data from various sources, process them and store them in a database',
    tags = ['gtfs', 'ingestion', 'database'],
    catchup = False,
    schedule_interval =  dag_scheduler,
    start_date = days_ago(1),
    doc_md = """
    # WIP
            """)


### TASKS ###

get_gtfs_files = PythonOperator(
    task_id = 'get_gtfs_files',
    dag = gtfs_ingestion_dag,
    python_callable = get_gtfs_files,
    op_kwargs = {'gtfs_url': gtfs_url,
                 'gtfs_storage_path':gtfs_storage_path},
    retries = 3,
    retry_delay = datetime.timedelta(seconds=500),
    on_failure_callback=None,
    on_success_callback=None,
    trigger_rule='dummy',
    doc_md = """
    # WIP
    """
    )


gtfs_routes_loader = PythonOperator(
    task_id = 'gtfs_routes_loader',
    dag = gtfs_ingestion_dag,
    python_callable = load_gtfs_data_to_dataframe,
    op_kwargs = {'gtfs_filepath': gtfs_storage_path + 'routes.txt',
                 'file': 'routes'},
    retries = 2,
    retry_delay = datetime.timedelta(seconds=60),
    on_failure_callback=None,
    on_success_callback=None,
    trigger_rule='all_success',
    doc_md = """
    # WIP
    """
    )


gtfs_routes_ingestion = PythonOperator(
    task_id = 'gtfs_routes_ingestion',
    dag = gtfs_ingestion_dag,
    python_callable = ingest_gtfs_data_to_database,
    provide_context=True,
    op_kwargs = {'table': 'routes_gtfs'},
    retries = 3,
    retry_delay = datetime.timedelta(seconds=120),
    on_failure_callback=None,
    on_success_callback=None,
    trigger_rule='all_success',
    doc_md = """
    # WIP
    """
    )

# Tasks:
# calendar_dates
# stops
# stop times
# trips
# For each task: load and insert data into the database



# DEPENDENCIES
get_gtfs_files >> gtfs_routes_loader >> gtfs_routes_ingestion