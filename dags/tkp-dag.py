from __future__ import annotations

import json
import os
import pendulum

from airflow.providers.standard.operators.python import PythonOperator

from airflow.sdk import DAG




with DAG(
    "TKP-dag",
    default_args={"retries": 2},
    description="DAG tutorial",
    schedule='*/5 * * * *',
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["tkp"],
) as dag:
   
    def poll_acknowledgement_files(**kwargs):
        directory = "/opt/airflow/testdata"  # Modify this path as needed
        files = [f for f in os.listdir(directory) if f.endswith('.ack')]
        if files:
            return {"files_found": files}
        else:
            return {"files_found": []}
        
    def transfer_payment_to_bank(**kwargs):
        """
        Transfer payment to the bank
        """
        # Get necessary parameters
        ti = kwargs["ti"]
        
        # Example: Get payment ID and message ID from previous tasks or parameters
        # This could come from the files found in the poll_acknowledgement_files task
        # or from other context variables
        payment_id = kwargs.get("payment_id", "PAYMENT123")
        message_id = kwargs.get("message_id", "MSG456")
        
        # Your payment transfer logic here
        print(f"Transferring payment {payment_id} to the bank. MessageId {message_id}")
        
        # Example: Record the transfer in XCom
        ti.xcom_push(
            key="transfer_result", 
            value={
                "payment_id": payment_id,
                "message_id": message_id,
                "status": "transferred",
                "timestamp": str(pendulum.now())
            }
        )
        return f"Successfully transferred payment {payment_id}"

    poll_ack_files = PythonOperator(
        task_id='polling-acknowledgement-files',
        python_callable=poll_acknowledgement_files,
    )

    transfer_payment_task = PythonOperator(
        task_id="transfer_payment_to_bank",
        python_callable=transfer_payment_to_bank,
        # You can provide parameters to substitute in the doc string
        op_kwargs={
            "payment_id": "{{ ti.xcom_pull(task_ids='polling-acknowledgement-files', key='files_found')[0] if ti.xcom_pull(task_ids='polling-acknowledgement-files', key='files_found') else 'UNKNOWN' }}",
            "message_id": "{{ task_instance_key_str }}"
        },
        # This is the equivalent of your @Job(name = "...")
        doc="Transfer payment {{ params.payment_id }} to the bank. MessageId {{ params.message_id }}",
        params={
            "payment_id": "{{ ti.xcom_pull(task_ids='polling-acknowledgement-files', key='files_found')[0] if ti.xcom_pull(task_ids='polling-acknowledgement-files', key='files_found') else 'UNKNOWN' }}",
            "message_id": "{{ task_instance_key_str }}"
        }
    )
   
    poll_ack_files >> transfer_payment_task 
