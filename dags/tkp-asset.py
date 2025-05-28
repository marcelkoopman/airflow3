from airflow.sdk import DAG
from airflow.datasets import Dataset
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.providers.http.operators.http import HttpOperator
from datetime import datetime

# Define the dataset for the Kafka topic
kafka_dataset = Dataset("kafka://your_kafka_topic")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 26),
}
with DAG('tkp-asset', default_args=default_args, schedule='@daily') as dag:
    consume_kafka = ConsumeFromTopicOperator(
    task_id='consume_kafka',
    topics=['your_kafka_topic'],
    consumer_config={
            'bootstrap.servers': 'your-kafka-bootstrap-server:9092',
            'group.id': 'your_group_id',
            # Add any other Kafka consumer configuration parameters here
        }
    )
    tags=["tkp"],
    kafka_dataset = Dataset("kafka://your_kafka_topic")
    consume_kafka.outlets.append(kafka_dataset)  # Mark dataset as output

    send_http_post = HttpOperator(
        task_id='send_http_post',
        http_conn_id='your_http_connection',
        endpoint='your/api/endpoint',
        method='POST',
        data="{{ task_instance.xcom_pull(task_ids='consume_kafka') }}",
        headers={"Content-Type": "application/json"},
    )

    # Set the dataset as an upstream dependency for the HTTP task
    send_http_post.dataset = kafka_dataset

    consume_kafka >> send_http_post
