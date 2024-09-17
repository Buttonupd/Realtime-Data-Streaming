#!/bin/bash

set -e

if [ -e "/opt/airflow/requirements.txt" ]; then
    # pip install -r /opt/airflow/requirements.txt
    $(command -v pip) install --user -r requirements.txt
fi

if [ ! -f "/opt/airflow/airflow.db" ]; then
    airflow db migrate && \
    airflow users create \
        --username admin \
        --firstname admin \
        --lastname admin \
        --role Admin \
        --email Dankariuki0101@gmail.com \
        --password admin
fi

$(command -v airflow) db upgrade

exec airflow webserver


