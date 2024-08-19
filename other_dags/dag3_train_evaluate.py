# Each dag triggers the next dag, order :
    # dag1 >> dag2 >> dag3 >> dag4 >> dag5 (see task trigger)

# Change env variables (dag_schedule_<dag_name>) in docker-compose_airflow.yaml if needed.
    # Default value : None


"""
Libraries
"""
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator

import datetime
from config import dag_scheduler_train_evaluate, dag_scheduler_modelregistry, alertOnFailure


"""
DAG 3 - Train & Evaluate
"""

# DAG
trainEvaluateDag = DAG(
    dag_id = "TrainEvaluateDag",
    description = 'Paris sportifs - Model train & evaluate',
    tags = ['mlops', 'paris_sportifs'],
    catchup = False,
    schedule_interval =  dag_scheduler_train_evaluate,
    start_date = days_ago(1),
    doc_md = """
            # Train Evaluate Dag - Mlops Paris sportifs
            """)



# Task 31
task_train_evaluate = BashOperator(
    task_id = 'train_evaluate',
    dag = trainEvaluateDag,
    bash_command = 'python3 /opt/airflow/dags/experiment.py',
    retries = 3,
    retry_delay = datetime.timedelta(seconds=300),
    #on_failure_callback=alertOnFailure,
    doc_md = """
    # Task 31 - Model - train & evaluate
    """)



# Task trigger
if dag_scheduler_modelregistry is None:
    trigger_dag_4 = TriggerDagRunOperator(
        task_id='trigger_next_dag4',
        trigger_rule='all_success',
        trigger_dag_id="modelRegistryDag",  # Next dag
        dag=trainEvaluateDag)

    # Task dependencies
    task_train_evaluate >> trigger_dag_4