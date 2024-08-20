#!/bin/bash


# Build image
docker build -t airflow_train_delay:lastest .

# Init Airflow
docker-compose -f docker-compose.yaml up airflow-init

# Start containers
docker-compose -f docker-compose.yaml up -d