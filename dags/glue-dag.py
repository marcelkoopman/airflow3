from datetime import datetime, timedelta
import boto3
import os
import time

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.hooks.base import BaseHook

# Constants
BUCKET_NAME = 'glue-scripts-bucket'
GLUE_JOB_NAME = 'glue_job.py'
GLUE_SCRIPT_LOCAL_PATH = 'scripts/glue/'+GLUE_JOB_NAME
GLUE_SCRIPT_S3_KEY = 'scripts/'+GLUE_JOB_NAME

# Function to get boto3 s3 client
def get_s3_client():
    conn = BaseHook.get_connection("aws_localstack")
    endpoint_url = conn.host
    aws_access_key = conn.login
    aws_secret_key = conn.password
    region = conn.extra_dejson.get('region_name', 'eu-west-1')
    
    print(f"Connecting to Localstack S3 at {endpoint_url}")
    print(f"AWS Access Key: {aws_access_key}")
    print(f"AWS Secret Key: {aws_secret_key[:4]}***") # Print first few chars of secret for debugging
    print(f"Region: {region}")
    
    # Create boto3 client with connection details
    s3_client = boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=region,
        # Disable SSL verification for LocalStack
        verify=False,
        # Configure path-style addressing for LocalStack
        config=boto3.session.Config(
            signature_version='s3v4',
            s3={'addressing_style': 'path'}
        )
    )
    return s3_client

# Add this function to get a properly configured Glue client
def get_glue_client():
    conn = BaseHook.get_connection("aws_localstack")
    endpoint_url = conn.host
    aws_access_key = conn.login
    aws_secret_key = conn.password
    region = conn.extra_dejson.get('region_name', 'eu-west-1')

    print(f"Connecting to Localstack Glue at {endpoint_url}")

    # Create boto3 client with connection details
    glue_client = boto3.client(
        'glue',
        endpoint_url=endpoint_url,
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=region,
        # Disable SSL verification for LocalStack
        verify=False
    )
    return glue_client

# Function to create S3 bucket in Localstack
def create_s3_bucket(**context):
    s3_client = get_s3_client()
    region = BaseHook.get_connection("aws_localstack").extra_dejson.get('region_name', 'eu-west-1')
    
    # Create bucket (with error handling)
    try:
        # Don't use LocationConstraint for us-east-1 (LocalStack default)
        if region == 'us-east-1':
            s3_client.create_bucket(Bucket=BUCKET_NAME)
        else:
            s3_client.create_bucket(
                Bucket=BUCKET_NAME,
                CreateBucketConfiguration={'LocationConstraint': region}
            )
        print(f"Successfully created bucket: {BUCKET_NAME}")
        print(f"Go to {s3_client.meta.endpoint_url}/{BUCKET_NAME}/")
    except s3_client.exceptions.BucketAlreadyExists:
        print(f"Bucket {BUCKET_NAME} already exists")
    except s3_client.exceptions.BucketAlreadyOwnedByYou:
        print(f"Bucket {BUCKET_NAME} already owned by you")
    except Exception as e:
        print(f"Error creating bucket: {str(e)}")
        raise
    
    # List buckets to verify
    response = s3_client.list_buckets()
    print("Existing buckets:")
    for bucket in response['Buckets']:
        print(f"  {bucket['Name']}")
    
    return BUCKET_NAME

# Function to upload file to S3
def upload_file_to_s3(**context):
    s3_client = get_s3_client()
    
    try:
        # Check if the file exists
        if not os.path.exists(GLUE_SCRIPT_LOCAL_PATH):
            raise FileNotFoundError(f"Script file not found: {GLUE_SCRIPT_LOCAL_PATH}")
        
        print(f"Uploading {GLUE_SCRIPT_LOCAL_PATH} to s3://{BUCKET_NAME}/{GLUE_SCRIPT_S3_KEY}")
        
        # Upload the file
        with open(GLUE_SCRIPT_LOCAL_PATH, 'rb') as file_obj:
            s3_client.upload_fileobj(
                Fileobj=file_obj,
                Bucket=BUCKET_NAME,
                Key=GLUE_SCRIPT_S3_KEY
            )
        
        # Verify the upload by listing objects
        response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix='scripts/')
        print("Files in bucket:")
        for obj in response.get('Contents', []):
            print(f"  {obj['Key']} ({obj['Size']} bytes)")
        
        return f"s3://{BUCKET_NAME}/{GLUE_SCRIPT_S3_KEY}"
    except Exception as e:
        print(f"Error uploading file: {str(e)}")
        raise

# Function to run Glue job manually instead of using the operator
def run_glue_job_manually(**context):
    glue_client = get_glue_client()
    s3_client = get_s3_client()
    job_name = GLUE_JOB_NAME.replace('.py', '')
    script_location = f's3://{BUCKET_NAME}/{GLUE_SCRIPT_S3_KEY}'

    try:
        # Check if job already exists
        try:
            glue_client.get_job(JobName=job_name)
            print(f"Job {job_name} already exists, deleting it first")
            glue_client.delete_job(JobName=job_name)
        except glue_client.exceptions.EntityNotFoundException:
            print(f"Job {job_name} does not exist yet, will create it")

        # Create the job
        print(f"Creating Glue job {job_name} with script {script_location}")
        response = glue_client.create_job(
            Name=job_name,
            Role='glue-execution-role',
            GlueVersion='3.0',
            NumberOfWorkers=2,
            WorkerType='G.1X',
            Command={
                'Name': 'glueetl',
                'ScriptLocation': script_location,
                'PythonVersion': '3'
            },
            DefaultArguments={
                '--source_path': f's3://{BUCKET_NAME}/input/',
                '--target_path': f's3://{BUCKET_NAME}/output/',
                '--job-language': 'python'
            }
        )
        print(f"Job created: {response}")
    
        # Start job run
        run_response = glue_client.start_job_run(
            JobName=job_name,
            Arguments={
                '--source_path': f's3://{BUCKET_NAME}/input/',
                '--target_path': f's3://{BUCKET_NAME}/output/'
            }
        )
    
        run_id = run_response['JobRunId']
        print(f"Started job run with ID: {run_id}")

        # Monitor the job (simplified)
        status = 'STARTING'
        while status in ['STARTING', 'RUNNING']:
            time.sleep(10)  # Check every 10 seconds
            run_info = glue_client.get_job_run(JobName=job_name, RunId=run_id)
            status = run_info['JobRun']['JobRunState']
            print(f"Job status: {status}")

        if status == 'SUCCEEDED':
            return run_id
        else:
            error_message = run_info['JobRun'].get('ErrorMessage', 'Unknown error')
            raise Exception(f"Job failed with status {status}: {error_message}")

    except Exception as e:
        print(f"Error running Glue job: {str(e)}")
        raise

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# DAG definition
with DAG(
    "glue",
    default_args=default_args,
    description='ETL workflow using Localstack for AWS services',
    schedule=None,  # Manual trigger only
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['tkp','localstack','glue'],
) as dag:
    
    # Task 1: Create S3 bucket in Localstack
    create_bucket = PythonOperator(
        task_id='create_s3_bucket',
        python_callable=create_s3_bucket,
    )
    
    # Task 2: Upload Glue script to S3 (using custom function instead of operator)
    upload_script = PythonOperator(
        task_id='upload_glue_script',
        python_callable=upload_file_to_s3,
    )

    # Task 3: Create and run Glue job manually instead of using the operator
    run_glue_job = PythonOperator(
        task_id='run_glue_job',
        python_callable=run_glue_job_manually,
    )
    
    # Task 4: Optional - Log job completion
    def log_completion(**context):
        print("Glue job completed successfully!")
        return "Success"
        
    job_completed = PythonOperator(
        task_id='log_completion',
        python_callable=log_completion,
    )
    
    # Set task dependencies
    create_bucket >> upload_script >> run_glue_job >> job_completed