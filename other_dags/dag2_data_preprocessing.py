# Each dag triggers the next dag, order :
    # dag1 >> dag2 >> dag3 >> dag4 >> dag5 (see task trigger)

# Change env variables (dag_schedule_<dag_name>) in docker-compose_airflow.yaml if needed.
    # Default value : None


"""
Libraries
"""
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator

import datetime
from config import dag_scheduler_preprocessing, dag_scheduler_train_evaluate, alertOnFailure


"""
DAG 2 - Pre-Processing
"""

# DAG
preProcessingDag = DAG(
    dag_id = "preProcessingDag",
    description = 'Paris sportifs - data pre-processing',
    tags = ['mlops', 'paris_sportifs'],
    catchup = False,
    schedule_interval =  dag_scheduler_preprocessing,
    start_date = days_ago(1),
    doc_md = """
            # PreProcessing Dag - Mlops Paris sportifs
            """)


# Task 21
from data_preprocessing_matches import preProcessingPipeline
task_preprocessing = PythonOperator(
    task_id = 'data_preprocessing',
    dag = preProcessingDag,
    python_callable = preProcessingPipeline,
    op_kwargs= {},
    retries = 3,
    retry_delay = datetime.timedelta(seconds=300),
    #on_failure_callback=alertOnFailure,
    doc_md = """
    # Task 21 - Data - preprocessing
    """)



# Task trigger
if dag_scheduler_train_evaluate is None:
    trigger_dag_3 = TriggerDagRunOperator(
        task_id='trigger_next_dag3',
        trigger_rule='all_success',
        trigger_dag_id="TrainEvaluateDag",  # Next dag
        dag=preProcessingDag)

    # Task dependencies
    task_preprocessing >> trigger_dag_3