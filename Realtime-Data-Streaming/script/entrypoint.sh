#!/bin/bash

set -e

if [ -e "/opt/airflow/requirements.txt" ]; then
    pip install -r /opt/airflow/requirements.txt --no-cache-dir
fi

if [ ! -f "/opt/airflow/airflow.db" ]; then
    airflow db init && \
    airflow users create \
        --username admin \
        --firstname admin \
        --lastname admin \
        --role Admin \
        --email someone@gmail.com \
        --password admin
fi

exec airflow webserver
