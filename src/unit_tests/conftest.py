# src/unit_tests/conftest.py
import os
import sys

# Ensure the src directory and dags subdirectory are in the PYTHONPATH
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../airflow/dags')))