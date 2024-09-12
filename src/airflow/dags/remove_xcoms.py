"""
DAG CLEANUP XCOMs
"""


# LIB
from airflow import DAG
from airflow.models import XCom
from airflow.utils.dates import days_ago
from airflow.utils.db import provide_session
from airflow.operators.python_operator import PythonOperator


import os



# VARS
dag_scheduler = os.getenv('REMOVE_XCOMS_SCHEDULER', None)



# DAG
gtfs_ingestion_dag = DAG(
    dag_id = "clean_xcoms_dag",
    description = 'Clean all XOMs from the database',
    tags = ['xcoms', 'clean', 'utils'],
    catchup = False,
    schedule_interval =  dag_scheduler,
    start_date = days_ago(1),
    doc_md = """
    Clean all XComs from the Airflow database using a task.
            """)


@provide_session
def cleanup_xcom(session=None):
    """
    Function to cleanup all XComs from the Airflow database.
    """
    session.query(XCom).delete()
    session.commit()


### TASKS ###

# Remove XCOMs
remove_xcoms = PythonOperator(
    task_id = 'cleanup_all_xcoms',
    dag = gtfs_ingestion_dag,
    python_callable = cleanup_xcom,
    trigger_rule='dummy',
    doc_md = """
    Call a function to cleanup all XComs from the Airflow database.
    """
    )