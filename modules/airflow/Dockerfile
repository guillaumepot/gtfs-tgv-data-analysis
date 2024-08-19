FROM apache/airflow:latest-python3.10


USER root

# Install firefox
RUN apt-get update && apt-get install -y firefox-esr


# Get Geckodriver
COPY ./geckodriver /opt/airflow/geckodriver
RUN chmod +x /opt/airflow/geckodriver


USER airflow

# Install packages specified in requirements.txt
COPY ./requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt