"""
Test DAG - ETL - Get open data files from source
"""

# LIB
import json
import os
import pytest
import sys
from unittest import mock
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.airflow.dags.ETL_files_from_open_data_sources_functions import *

