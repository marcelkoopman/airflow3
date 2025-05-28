from __future__ import annotations

import json
import os
import pendulum

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

with DAG(
    "upa-dag",
    default_args={"retries": 2},
    description="DAG tutorial",
    schedule='@hourly',
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["upa"],
) as dag:
   
    def print_hello():
        print("Hello")

    task = PythonOperator(
        task_id='task',
        python_callable=print_hello,
    )

    task2 = PythonOperator(
        task_id='task2',
        python_callable=print_hello,
    )

    task >> task2


