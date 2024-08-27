"""
Contains functions that are used by the tasks in the Airflow dags
"""


# LIB
from airflow.models import TaskInstance
from datetime import datetime, timezone
from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToDict
import json
import logging
import os
import psycopg2
import requests



# COMMON FUNCTIONS
print('Hello World !')