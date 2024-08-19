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

import datetime
from config import dag_scheduler_model_predictions


"""
DAG 5 - Model predictions
"""

# DAG
modelPredictionsDag = DAG(
    dag_id = "modelPredictionsDag",
    description = 'Paris sportifs - Model Predictions',
    tags = ['mlops', 'paris_sportifs'],
    catchup = False,
    schedule_interval =  dag_scheduler_model_predictions,
    start_date = days_ago(1),
    doc_md = """
            # Model Predictions Dag - Mlops Paris sportifs
            """)


# Task 51
from model_predictions import getPredictionsOnCalendar
task_preprocessing = PythonOperator(
    task_id = 'model_predictions',
    dag = modelPredictionsDag,
    python_callable = getPredictionsOnCalendar,
    op_kwargs= {},
    retries = 3,
    retry_delay = datetime.timedelta(seconds=300),
    #on_failure_callback=alertOnFailure,
    doc_md = """
    # Task 51 - Get model predictions
    """)