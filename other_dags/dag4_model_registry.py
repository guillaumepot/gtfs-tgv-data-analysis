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
from config import dag_scheduler_modelregistry, dag_scheduler_model_predictions

"""
DAG 4 - ModelRegistry
"""

# DAG
ModelRegistryDag = DAG(
    dag_id = "modelRegistryDag",
    description = 'Paris sportifs - Model Registry',
    tags = ['mlops', 'paris_sportifs'],
    catchup = False,
    schedule_interval =  dag_scheduler_modelregistry,
    start_date = days_ago(1),
    doc_md = """
            # ModelRegistry Dag - Mlops Paris sportifs
            """)



# Task 41
task_modelRegistry = BashOperator(
    task_id = 'model_registry',
    dag = ModelRegistryDag,
    bash_command = 'python3 /opt/airflow/dags/model_registry.py',
    retries = 3,
    retry_delay = datetime.timedelta(seconds=300),
    #on_failure_callback=alertOnFailure,
    doc_md = """
    # Task 41 - Model - registry
    """)



# Task trigger
if dag_scheduler_model_predictions is None:
    trigger_dag_5 = TriggerDagRunOperator(
        task_id='trigger_next_dag5',
        trigger_rule='all_success',
        trigger_dag_id="modelPredictionsDag",  # Next dag
        dag=ModelRegistryDag)

    # Task dependencies
    task_modelRegistry >> trigger_dag_5