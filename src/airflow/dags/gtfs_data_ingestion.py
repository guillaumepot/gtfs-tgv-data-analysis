"""

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
from gtfs_data_ingestion_functions import get_gtfs_rt_data, transform_feed, push_feed_data_to_db



# VARS
dag_scheduler = os.getenv('GTFS_INGESTION_SCHEDULER', None)


# DAG
gtfs_ingestion_dag = DAG(
    dag_id = "gtfs_ingestion_dag",
    description = 'Get GTFS & GTFS-RT data from different sources, process them and store them in a database',
    tags = ['gtfs', 'gtfs-rt', 'ingestion', 'database'],
    catchup = False,
    schedule_interval =  dag_scheduler,
    start_date = days_ago(1),
    doc_md = """
            # WIP
            """)




### TASKS ###


# GTFS RT TASKS
get_feed_gtfs_rt = PythonOperator(
    task_id = 'get_feed_gtfs_rt',
    dag = gtfs_ingestion_dag,
    python_callable = get_gtfs_rt_data,
    op_kwargs = {'gtfs_rt_url':"https://proxy.transport.data.gouv.fr/resource/sncf-tgv-gtfs-rt-trip-updates"},
    retries = 2,
    retry_delay = datetime.timedelta(seconds=30),
    on_failure_callback=None,
    on_success_callback=None,
    trigger_rule='dummy',
    doc_md = """
    # WIP
    """)


transform_feed_gtfs_rt = PythonOperator(
    task_id = 'transform_feed_gtfs_rt',
    dag = gtfs_ingestion_dag,
    python_callable = transform_feed,
    op_kwargs = {'feed_json': "{{ task_instance.xcom_pull(task_ids='get_feed_gtfs_rt') }}"},
    #retries = 0,
    #retry_delay = datetime.timedelta(seconds=30),
    on_failure_callback=None,
    on_success_callback=None,
    trigger_rule='all_success',
    doc_md = """
    # WIP
    """)


push_trip_data_to_db = PythonOperator(
    task_id = 'push_trip_data_to_db',
    dag = gtfs_ingestion_dag,
    python_callable = push_feed_data_to_db,
    op_kwargs = {'feed_data': "{{ task_instance.xcom_pull(task_ids='transform_feed_gtfs_rt')[0] }}",
                 'table': 'trips_gtfs_rt'},
    retries = 3,
    retry_delay = datetime.timedelta(seconds=20),
    on_failure_callback=None,
    on_success_callback=None,
    trigger_rule='all_success',
    doc_md = """
    # WIP
    """)


push_stop_times_data_to_db = PythonOperator(
    task_id = 'push_stop_times_data_to_db',
    dag = gtfs_ingestion_dag,
    python_callable = push_feed_data_to_db,
    op_kwargs = {'feed_data': "{{ task_instance.xcom_pull(task_ids='transform_feed_gtfs_rt')[1] }}",
                 'table': 'stop_time_update_gtfs_rt'},
    retries = 3,
    retry_delay = datetime.timedelta(seconds=20),
    on_failure_callback=None,
    on_success_callback=None,
    trigger_rule='all_success',
    doc_md = """
    # WIP
    """)


# GTFS TASKS




# DEPENDENCIES
get_feed_gtfs_rt >> transform_feed_gtfs_rt >> (push_trip_data_to_db, push_stop_times_data_to_db)