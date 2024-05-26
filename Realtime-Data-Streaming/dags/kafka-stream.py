from datetime import datetime
from airflow import DAG 
from airflow.operators.python import PythonOperator
import json
import requests
from kafka import KafkaProducer
import time

# DAG default args
default_args = {
    'owner':'Buttonupd',
    'start_date' : datetime(2024, 5, 1, 10, 00 )
}

# Function to retrieve data from API

def get_data():
    res = requests.get('http://randomuser.me/api/')
    res = res.json()
    res = res['results'][0]
    return res

def format_data(res):
    data = {}
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['title'] = res['name']['title']
    data['street'] = f"{res['location']['street']['number']} {res['location']['street']['name']}"
    data['city'] = res['location']['city']
    data['state'] = res['location']['state']
    data['country'] = res['location']['country']
    data['postcode'] = res['location']['postcode']
    data['latitude'] = res['location']['coordinates']['latitude']
    data['longitude'] = res['location']['coordinates']['longitude']
    data['timezone'] = res['location']['timezone']['description']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['password'] = res['login']['password']
    data['dob'] = res['dob']['date']
    data['age'] = res['dob']['age']
    data['registered_date'] = res['registered']['date']
    data['registered_age'] = res['registered']['age']
    data['phone'] = res['phone']
    data['cell'] = res['cell']
    data['picture_large'] = res['picture']['large']
    data['picture_medium'] = res['picture']['medium']
    data['picture_thumbnail'] = res['picture']['thumbnail']
    data['nationality'] = res['nat']
    return data

def stream_data():
    res = get_data()
    res = format_data(res=res)
    # print(json.dumps(res, indent=2))
    producer = KafkaProducer(
        bootstrap_servers=['broker:29092'],
        max_block_ms=100000
    )

    producer.send('users_created', json.dumps(res).encode('utf-8'))

# DAG initialization
with DAG('user_automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False
         ) as dag:
    streaming_task = PythonOperator(
        task_id = 'stream_data_from_api',
        python_callable=stream_data
    )

