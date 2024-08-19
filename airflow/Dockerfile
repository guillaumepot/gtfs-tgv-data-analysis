FROM apache/airflow:latest-python3.10


USER root

# Install firefox
RUN apt-get update && apt-get install -y firefox-esr

# COPY files for dags
COPY ./dags /opt/airflow/dags

# Give execute permission to geckodriver
RUN chmod +x /opt/airflow/dags/geckodriver


USER airflow

# Install packages specified in requirements.txt
COPY ./requirements_airflow.txt /app/requirements_airflow.txt
RUN pip install --no-cache-dir -r /app/requirements_airflow.txt


ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow:/opt/airflow/dags"