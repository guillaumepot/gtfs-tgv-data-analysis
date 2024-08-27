"""
Contains common functions that are used by the tasks in the Airflow dags
"""


# LIB
import os
import psycopg2



# COMMON FUNCTIONS
def connect_to_postgres() -> psycopg2.connect:
    """
    Connects to the database using the provided credentials.

    Returns:
        psycopg2.connect: The connection object representing the connection to the database.
    """

    return psycopg2.connect(user=os.getenv("DATA_PG_USER"),
                            password=os.getenv("DATA_PG_PASSWORD"),
                            dbname=os.getenv("DATA_PG_DB"),
                            host=os.getenv("DATA_PG_HOST"),
                            port=os.getenv("DATA_PG_PORT"))
            
