from __future__ import annotations

import json
import os
import pendulum

from airflow.providers.standard.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.sdk import DAG
from datetime import timedelta

with DAG(
    "simple",
    default_args={"retries": 2},
    description="DAG upa",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["tkp"],
) as dag:
   
    def print_task(**kwargs):
       print("hello")
   
    taak1 = PythonOperator(
        task_id='taak1',
        python_callable=print_task,
    )

    taak2 = PythonOperator(
        task_id='taak2',
        python_callable=print_task,
    )

    taak1 >> taak2
