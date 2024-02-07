import time
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'amal',
    'start_date': datetime(2024, 2, 3, 10, 00)
}


def get_data():
    import requests
    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    res = res['results'][0]
    return res


def format_data(res):
    data = {}
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['adddress'] = f"{str(res['location']['street']['number'])} {res['location']['street']['name']}, " \
                       f"{res['location']['city']}, {res['location']['state']}, {res['location']['country']}"
    data['post_code'] = res['location']['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']
    return data


def stream_data():
    import json
    from kafka import KafkaProducer
    import logging

    start_time = time.time()
    producer = KafkaProducer(bootstrap_servers="broker:29092", max_block_ms=5000)

    while True:
        if time.time() > start_time + 60:
            break
        try:
            raw = get_data()
            data = format_data(raw)
            producer.send('user_details', json.dumps(data).encode('utf-8'))
            print(json.dumps(data, indent=3))
        except Exception as e:
            logging.error(f"an error occurred: {e}")
            continue


with DAG('user_automation',
         default_args=default_args,
         schedule='@daily',
         catchup=False) as dag:
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data,
        dag=dag
    )

# stream_data()
