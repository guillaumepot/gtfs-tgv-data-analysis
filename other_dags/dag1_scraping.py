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
from config import dag_scheduler_scraping, dag_scheduler_preprocessing, alertOnFailure


"""
DAG 1 - scraping
"""

# DAG
scraperDag = DAG(
    dag_id = "scraperDag",
    description = 'Paris sportifs - data scraper',
    tags = ['mlops', 'paris_sportifs'],
    catchup = False,
    schedule_interval =  dag_scheduler_scraping,
    start_date = days_ago(1),
    doc_md = """
            # Scraper Dag - Mlops Paris sportifs
            """)


# Task 11
from scrap_bookmakers_odds import scrapOdds
#from common_variables import other_leagues_current_season_url, other_leagues_filename, other_leagues_dictionary
from common_variables import main_leagues_current_season_url, main_leagues_filename, main_leagues_dictionary

# Scrap only main leagues
url = main_leagues_current_season_url
filename = main_leagues_filename
dictionary = main_leagues_dictionary
league = 'main'


task_request_odds = PythonOperator(
    task_id = 'data_request_odds',
    dag = scraperDag,
    python_callable = scrapOdds,
    op_kwargs= {},
    retries = 3,
    retry_delay = datetime.timedelta(seconds=300),
    #on_failure_callback=alertOnFailure,
    doc_md = """
    # Task 11 - Request - odds
    """)



# Task 12
from scrap_match_history import scrapMatchHistoryAndSortDatas
task_request_results = PythonOperator(
    task_id = 'data_request_results',
    dag = scraperDag,
    python_callable = scrapMatchHistoryAndSortDatas,
    op_kwargs= {'url':url,
                'filename':filename,
                'dictionary':dictionary,
                'league':league},
    retries = 3,
    retry_delay = datetime.timedelta(seconds=300),
    #on_failure_callback=alertOnFailure,
    doc_md = """
    # Task 12 - Request - results
    """)



# Task 13
from archive_datas_source import createDataArchive
task_archiving_scraping = PythonOperator(
    task_id = 'data_archiving_scraping',
    dag = scraperDag,
    python_callable = createDataArchive,
    op_kwargs= {},
    retries = 3,
    trigger_rule='all_done',
    retry_delay = datetime.timedelta(seconds=300),
    #on_failure_callback=alertOnFailure,
    doc_md = """
    # Task 13 - Data - Archiving
    """)



# Task trigger
if dag_scheduler_preprocessing is None:
    trigger_dag_2 = TriggerDagRunOperator(
        task_id='trigger_next_dag2',
        trigger_rule='all_success',
        trigger_dag_id="preProcessingDag",  # Next dag
        dag=scraperDag)

    # Task dependencies
    (task_request_odds, task_request_results) >> task_archiving_scraping >> trigger_dag_2

else:
    # Task dependencies
    (task_request_odds, task_request_results) >> task_archiving_scraping