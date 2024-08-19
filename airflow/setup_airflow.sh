#!/bin/bash

# Init Airflow
docker-compose -f docker-compose.yaml up airflow-init

# Start containers
docker-compose -f docker-compose.yaml up -d