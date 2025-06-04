from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'pyspark',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['tkp','local','pyspark'],
)

def run_pyspark_job():
    from pyspark.sql import SparkSession
    
    # Create a local SparkSession
    spark = SparkSession.builder \
        .appName("Airflow PySpark Local") \
        .master("local[*]") \
        .getOrCreate()
    
    # Simple data transformation
    data = [("Alice", 34), ("Bob", 45), ("Catherine", 29)]
    df = spark.createDataFrame(data, ["Name", "Age"])
    
    print("DataFrame content:")
    df.show()
    
    # Stop the Spark session
    spark.stop()

pyspark_task = PythonOperator(
    task_id='run_local_pyspark',
    python_callable=run_pyspark_job,
    dag=dag,
)