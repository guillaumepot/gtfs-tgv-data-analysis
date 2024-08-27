"""
DAG GTFS Real-Time Ingestion
"""


# LIB
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator


import datetime
import os

# task functions
from gtfs_rt_data_ingestion_functions import get_gtfs_rt_data, transform_feed, push_feed_data_to_db



# VARS
dag_scheduler = os.getenv('GTFS_RT_INGESTION_SCHEDULER', None)


# DAG
gtfs_rt_ingestion_dag = DAG(
    dag_id = "gtfs_rt_ingestion_dag",
    description = 'Get GTFS-RT data, process them and store them in a database',
    tags = ['gtfs-rt', 'ingestion', 'database'],
    catchup = False,
    schedule_interval =  dag_scheduler,
    start_date = days_ago(1),
    doc_md = """
    # DAG Documentation: GTFS RT Ingestion DAG

    ## Description
    The `gtfs_rt_ingestion_dag` is designed to fetch, process, and store GTFS-RT data into a database. This DAG consists of multiple tasks that handle different stages of the data ingestion process.

    ## DAG Configuration
    - **DAG ID:** `gtfs_rt_ingestion_dag`
    - **Description:** Get GTFS-RT data, process them and store them in a database.
    - **Tags:** `gtfs-rt`, `ingestion`, `database`
    - **Catchup:** `False`
    - **Schedule Interval:** `{{ dag_scheduler }}`
    - **Start Date:** `{{ days_ago(1) }}`
    - **Documentation:**
        ```markdown
        # WIP
        ```

    ## Tasks

    ### 1. Get Feed GTFS RT
    - **Task ID:** `get_feed_gtfs_rt`
    - **Operator:** `PythonOperator`
    - **Function:** `get_gtfs_rt_data`
    - **Parameters:**
    - `gtfs_rt_url`: "https://proxy.transport.data.gouv.fr/resource/sncf-tgv-gtfs-rt-trip-updates"
    - **Retries:** `2`
    - **Retry Delay:** `30 seconds`
    - **Trigger Rule:** `dummy`
    - **Documentation:**
        ```markdown
        ### Task Documentation: get_feed_gtfs_rt

        This task is responsible for fetching GTFS-RT data from the specified URL and processing it.

        **Parameters:**
        - `gtfs_rt_url` (str): The URL to fetch the GTFS-RT data from. In this case, it is set to "https://proxy.transport.data.gouv.fr/resource/sncf-tgv-gtfs-rt-trip-updates".

        **Retries:**
        - The task will retry 2 times in case of failure, with a delay of 30 seconds between retries.

        **Trigger Rule:**
        - The task uses the 'dummy' trigger rule.

        **Callbacks:**
        - No specific callbacks are defined for success or failure.

        **Functionality:**
        - The task calls the `get_gtfs_rt_data` function to fetch and process the GTFS-RT data, converting it into a JSON string.
        ```

    ### 2. Transform Feed GTFS RT
    - **Task ID:** `transform_feed_gtfs_rt`
    - **Operator:** `PythonOperator`
    - **Function:** `transform_feed`
    - **Provide Context:** `True`
    - **Trigger Rule:** `all_success`
    - **Documentation:**
        ```markdown
        ### Task Documentation: transform_feed_gtfs_rt

        This task is responsible for transforming the GTFS Real-Time feed data into a list of trip data and stop times data.

        **Parameters:**
        - `provide_context` (bool): When set to True, Airflow will pass a set of keyword arguments that can be used in the function. This includes the `task_instance` which is used to pull XCom values.

        **Trigger Rule:**
        - The task uses the 'all_success' trigger rule, meaning it will only run if all upstream tasks have succeeded.

        **Callbacks:**
        - No specific callbacks are defined for success or failure.

        **Functionality:**
        - The task calls the `transform_feed` function to process the GTFS Real-Time feed data.
        - It retrieves the feed data from XCom, converts it from JSON string to dictionary, and processes it to extract trip data and stop times data.
        - The processed data is returned as two lists: `all_trip_data` and `all_stop_times_data`.
        ```

    ### 3. Push Trip Data to DB
    - **Task ID:** `push_trip_data_to_db`
    - **Operator:** `PythonOperator`
    - **Function:** `push_feed_data_to_db`
    - **Provide Context:** `True`
    - **Parameters:**
    - `table`: `trips_gtfs_rt`
    - **Retries:** `3`
    - **Retry Delay:** `20 seconds`
    - **Trigger Rule:** `all_success`
    - **Documentation:**
        ```markdown
        ### Task Documentation: push_trip_data_to_db

        This task is responsible for pushing trip data to the `trips_gtfs_rt` table in the PostgreSQL database.

        **Parameters:**
        - `table` (str): The name of the table to push data to. In this case, it is set to 'trips_gtfs_rt'.

        **Retries:**
        - The task will retry 3 times in case of failure, with a delay of 20 seconds between retries.

        **Trigger Rule:**
        - The task uses the 'all_success' trigger rule, meaning it will only run if all upstream tasks have succeeded.

        **Callbacks:**
        - No specific callbacks are defined for success or failure.

        **Functionality:**
        - The task calls the `push_feed_data_to_db` function to push trip data to the PostgreSQL database.
        - It retrieves the trip data from XCom and inserts or updates the data in the `trips_gtfs_rt` table.
        ```

    ## Task Dependencies
    The tasks are executed in the following order:
    1. `get_feed_gtfs_rt` >> `transform_feed_gtfs_rt` >> `push_trip_data_to_db`
            """)


### TASKS ###

get_feed_gtfs_rt = PythonOperator(
    task_id = 'get_feed_gtfs_rt',
    dag = gtfs_rt_ingestion_dag,
    python_callable = get_gtfs_rt_data,
    op_kwargs = {'gtfs_rt_url':"https://proxy.transport.data.gouv.fr/resource/sncf-tgv-gtfs-rt-trip-updates"},
    retries = 2,
    retry_delay = datetime.timedelta(seconds=30),
    on_failure_callback=None,
    on_success_callback=None,
    trigger_rule='dummy',
    doc_md = """
    ### Task Documentation: get_feed_gtfs_rt

    This task is responsible for fetching GTFS-RT data from the specified URL and processing it.

    **Parameters:**
    - `gtfs_rt_url` (str): The URL to fetch the GTFS-RT data from. In this case, it is set to "https://proxy.transport.data.gouv.fr/resource/sncf-tgv-gtfs-rt-trip-updates".

    **Retries:**
    - The task will retry 2 times in case of failure, with a delay of 30 seconds between retries.

    **Trigger Rule:**
    - The task uses the 'dummy' trigger rule.

    **Callbacks:**
    - No specific callbacks are defined for success or failure.

    **Functionality:**
    - The task calls the `get_gtfs_rt_data` function to fetch and process the GTFS-RT data, converting it into a JSON string.
    """
    )


transform_feed_gtfs_rt = PythonOperator(
    task_id = 'transform_feed_gtfs_rt',
    dag = gtfs_rt_ingestion_dag,
    python_callable = transform_feed,
    provide_context=True,
    #retries = 0,
    #retry_delay = datetime.timedelta(seconds=30),
    on_failure_callback=None,
    on_success_callback=None,
    trigger_rule='all_success',
    doc_md = """
    ### Task Documentation: transform_feed_gtfs_rt

    This task is responsible for transforming the GTFS Real-Time feed data into a list of trip data and stop times data.

    **Parameters:**
    - `provide_context` (bool): When set to True, Airflow will pass a set of keyword arguments that can be used in the function. This includes the `task_instance` which is used to pull XCom values.

    **Trigger Rule:**
    - The task uses the 'all_success' trigger rule, meaning it will only run if all upstream tasks have succeeded.

    **Callbacks:**
    - No specific callbacks are defined for success or failure.

    **Functionality:**
    - The task calls the `transform_feed` function to process the GTFS Real-Time feed data.
    - It retrieves the feed data from XCom, converts it from JSON string to dictionary, and processes it to extract trip data and stop times data.
    - The processed data is returned as two lists: `all_trip_data` and `all_stop_times_data`.
    """
    )


push_trip_data_to_db = PythonOperator(
    task_id = 'push_trip_data_to_db',
    dag = gtfs_rt_ingestion_dag,
    python_callable = push_feed_data_to_db,
    provide_context=True,
    op_kwargs = {'table': 'trips_gtfs_rt'},
    retries = 3,
    retry_delay = datetime.timedelta(seconds=20),
    on_failure_callback=None,
    on_success_callback=None,
    trigger_rule='all_success',
    doc_md = """
    ### Task Documentation: push_trip_data_to_db

    This task is responsible for pushing trip data to the `trips_gtfs_rt` table in the PostgreSQL database.

    **Parameters:**
    - `table` (str): The name of the table to push data to. In this case, it is set to 'trips_gtfs_rt'.

    **Retries:**
    - The task will retry 3 times in case of failure, with a delay of 20 seconds between retries.

    **Trigger Rule:**
    - The task uses the 'all_success' trigger rule, meaning it will only run if all upstream tasks have succeeded.

    **Callbacks:**
    - No specific callbacks are defined for success or failure.

    **Functionality:**
    - The task calls the `push_feed_data_to_db` function to push trip data to the PostgreSQL database.
    - It retrieves the trip data from XCom and inserts or updates the data in the `trips_gtfs_rt` table.
    """
    )



# DEPENDENCIES
get_feed_gtfs_rt >> transform_feed_gtfs_rt >> push_trip_data_to_db