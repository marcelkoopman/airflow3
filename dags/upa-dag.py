from __future__ import annotations

import json
import os
import pendulum

from airflow.providers.standard.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.sdk import DAG


with DAG(
    "upa",
    default_args={"retries": 2},
    description="DAG upa",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["tkp"],
) as dag:
   
    def print_task(**kwargs):
       print("hello")
    
    def validate_XSD_task(**kwargs):
        validation_successful = True  
        if validation_successful:
            return 'process_data'  
        else:
            return 'get_validation_error' 

    def generate_mutaties_task(**kwargs):
        # This function returns a list of strings that will be used to create dynamic tasks
        mutaties = ["mutation_1", "mutation_2", "mutation_3"]
        
        # Push the list to XCom so it can be retrieved by downstream tasks
        kwargs['ti'].xcom_push(key='mutaties_list', value=mutaties)
        return mutaties
    
    def send_to_kafka(**kwargs):
        mutation = kwargs.get('mutation')
        print(f"Sending {mutation} to Kafka")

    extract_XML = PythonOperator(
        task_id='extract_XML',
        python_callable=print_task,
    )
   
    validate_XSD = BranchPythonOperator(
        task_id='validate_XSD',
        python_callable=validate_XSD_task,
    )

    process_data = PythonOperator(
        task_id='process_data',
        python_callable=print_task,
    )

    generate_mutaties = PythonOperator(
        task_id='generate_mutaties',
        python_callable=generate_mutaties_task,  # Fixed to use the actual function
    )

    get_validation_error = PythonOperator(
        task_id='get_validation_error',
        python_callable=lambda **kwargs: print("Validation failed! Handling error."),
    )

    create_retour_bericht = PythonOperator(
        task_id='create_retour_bericht',
        python_callable=print_task,
    )

    # Create the TaskGroup during DAG definition
    with TaskGroup(group_id='send_to_kafka_group') as send_to_kafka_group:
        # Use predefined mutations for the static DAG structure
        # The actual mutations will be retrieved from XCom during execution
        for i, mutation_name in enumerate(["mutation_1", "mutation_2", "mutation_3"]):
            PythonOperator(
                task_id=f'send_to_kafka_{i}',
                python_callable=send_to_kafka,
                op_kwargs={'mutation': f"{{{{ ti.xcom_pull(task_ids='generate_mutaties', key='mutaties_list')[{i}] }}}}"},
            )

    extract_XML >> validate_XSD >> [process_data, get_validation_error] 
    process_data >> generate_mutaties >> send_to_kafka_group >> create_retour_bericht
    get_validation_error >> create_retour_bericht