import json

import airflow
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'Chandra',
    'depends_on_past': False,
    'start_date': days_ago(1),
}

dag_params = {
    'dag_id': 'google_sheet_to_postgres_etl_pipeline',
    'default_args': default_args,
    'schedule_interval': "@daily",
    'catchup': False,
}

with airflow.models.DAG(**dag_params) as dag:
    # Extract data from Google Sheet, transform and load to Postgres Table
    insert_data_task = SimpleHttpOperator(
        task_id='insert_data_task',
        method='POST',
        endpoint='insert-data-function',
        response_check=lambda response: "Success" in response.text,
        log_response=True
    )

    # Delete existing data in GCP Postgres instance.
    delete_existing_data_task = SimpleHttpOperator(
        task_id='delete_existing_data_task',
        method='POST',
        endpoint='delete-data-function',
        response_check=lambda response: "Success" in response.text,
        log_response=True
    )

    # Append new data that was added to google sheet from the previous run
    append_data_task = SimpleHttpOperator(
        task_id='append_data_task',
        method='POST',
        endpoint='append-data-function',
        data=json.dumps({"fetch_from": '{{ prev_execution_date }}'}),
        headers={"Content-Type": "application/json"},
        response_check=lambda response: "Success" in response.text,
        log_response=True
    )

    # Modify existing data in GCP Postgres SQL instance
    modify_existing_data_task = SimpleHttpOperator(
        task_id='modify_existing_data_task',
        method='POST',
        endpoint='modify-data-function',
        response_check=lambda response: "Success" in response.text,
        log_response=True
    )

    insert_data_task >> delete_existing_data_task >> append_data_task >> modify_existing_data_task
