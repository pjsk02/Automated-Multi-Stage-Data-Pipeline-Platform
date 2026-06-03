from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os
import requests
from zipfile import ZipFile
import boto3
import snowflake.connector
from bs4 import BeautifulSoup
from dotenv import load_dotenv


# Load environment variables
load_dotenv()

# Load environment variables
SEC_DATA_URL = "https://www.sec.gov/data-research/sec-markets-data/financial-statement-data-sets"
HEADERS = {
    "User-Agent": "Findata Inc. contact@findata.com",
    "Accept-Encoding": "gzip, deflate",
    "Host": "www.sec.gov"
}

SEC_DATA_DIR = os.path.join(os.getcwd(), "sec_data")
os.makedirs(SEC_DATA_DIR, exist_ok=True)

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

# Initialize S3 Client
s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)

def get_connection():
    return snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )

def get_sec_zip_links():
    """Scrape SEC data page for zip file links."""
    response = requests.get(SEC_DATA_URL, headers=HEADERS)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, 'html.parser')
    return [
        f"https://www.sec.gov{a['href']}" if a['href'].startswith('/') else a['href']
        for a in soup.find_all('a', href=True) if a['href'].endswith('.zip')
    ]

def download_sec_zip(**kwargs):
    """Download a specific SEC ZIP file."""
    ti = kwargs['ti']
    zip_links = ti.xcom_pull(task_ids='scrape_sec_links')

    dag_run = kwargs['dag_run']
    
    # Get year and quarter from DAG run configuration
    selected_year = dag_run.conf.get('year')
    selected_quarter = dag_run.conf.get('quarter')
    
    if not selected_year or not selected_quarter:
        raise ValueError("Year and quarter must be provided in the DAG run configuration")
    
    year_quarter = f"{selected_year}{selected_quarter}"

    zip_url = next((link for link in zip_links if selected_year in link and selected_quarter in link), None)
    
    if not zip_url:
        raise Exception(f"No matching ZIP file found for {year_quarter}")

    zip_filename = os.path.join(SEC_DATA_DIR, zip_url.split("/")[-1])

    response = requests.get(zip_url, headers=HEADERS)
    response.raise_for_status()

    with open(zip_filename, 'wb') as file:
        file.write(response.content)

    ti.xcom_push(key='zip_file', value=zip_filename)
    ti.xcom_push(key='year_quarter', value=year_quarter)

def extract_sec_data(**kwargs):
    """Extract specific files from SEC ZIP."""
    ti = kwargs['ti']
    zip_file = ti.xcom_pull(task_ids='download_sec_zip', key='zip_file')
    year_quarter = ti.xcom_pull(task_ids='download_sec_zip', key='year_quarter')

    quarter_path = os.path.join(SEC_DATA_DIR, year_quarter)
    os.makedirs(quarter_path, exist_ok=True)

    with ZipFile(zip_file, 'r') as zip_ref:
        zip_ref.extractall(quarter_path)

    extracted_files = [os.path.join(quarter_path, f) for f in ["num.txt", "sub.txt", "pre.txt", "tag.txt"]]
    ti.xcom_push(key='extracted_files', value=extracted_files)

def preprocess_data(file_path):
    """Preprocess data: truncate long strings (optional)."""
    with open(file_path, 'r') as file:
        lines = file.readlines()
    processed_lines = []
    for line in lines:
        columns = line.split('\t')
        if len(columns) > 10:  # Assuming FOOTNOTE is the 11th column (index 10)
            columns[10] = columns[10][:512]  # Truncate to 512 characters
        processed_lines.append('\t'.join(columns))
    with open(file_path, 'w') as file:
        file.writelines(processed_lines)

def upload_to_s3(**kwargs):
    """Upload extracted files to S3."""
    ti = kwargs['ti']
    extracted_files = ti.xcom_pull(task_ids='extract_sec_data', key='extracted_files')
    year_quarter = ti.xcom_pull(task_ids='download_sec_zip', key='year_quarter')

    s3_prefix = f"denormalized/{year_quarter}"
    for file in extracted_files:
        preprocess_data(file)
        file_name = os.path.basename(file)
        try:
            s3_client.upload_file(file, S3_BUCKET_NAME, f"{s3_prefix}/{file_name}")
            print(f"Successfully uploaded {file_name} to S3.")
        except Exception as e:
            print(f"Failed to upload {file_name} to S3. Error: {e}")

def load_snowflake(**kwargs):
    """Load raw data from S3 into Snowflake."""
    conn = get_connection()
    cursor = conn.cursor()
    year_quarter = kwargs['ti'].xcom_pull(task_ids='download_sec_zip', key='year_quarter')

    # Ensure the Snowflake stage exists
    stage_creation_query = f"""
    CREATE OR REPLACE STAGE RAW_STAGING.SEC_DATA
        URL = 's3://{S3_BUCKET_NAME}/denormalized/'
        CREDENTIALS = (AWS_KEY_ID = '{AWS_ACCESS_KEY_ID}' AWS_SECRET_KEY = '{AWS_SECRET_ACCESS_KEY}')
        FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = '\\t' SKIP_HEADER = 1);
    """
    try:
        cursor.execute(stage_creation_query)
        print("Stage created successfully.")
    except Exception as e:
        print(f"Error creating stage: {e}")
        return

    # Snowflake copy commands for loading data
    copy_queries = {
        "sub_table": f"""
            COPY INTO RAW_STAGING.sub_table
            FROM @RAW_STAGING.SEC_DATA/{year_quarter}/sub.txt
            FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = '\\t' SKIP_HEADER = 1)
            ON_ERROR = 'CONTINUE';
        """,
        "num_table": f"""
            COPY INTO RAW_STAGING.num_table
            FROM @RAW_STAGING.SEC_DATA/{year_quarter}/num.txt
            FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = '\\t' SKIP_HEADER = 1)
            ON_ERROR = 'CONTINUE';
        """
    }

    for table, query in copy_queries.items():
        try:
            print(f"Running query: {query}")
            cursor.execute(query)
            print(f"Data successfully loaded into {table}")
        except Exception as e:
            print(f"Error loading data into {table}: {e}")

    conn.close()

# Define Airflow DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 14),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'sec_data_denormalized_pipeline',
    default_args=default_args,
    description='End-to-end SEC Data Pipeline',
    schedule_interval=None,
    catchup=False
)

scrape_task = PythonOperator(
    task_id='scrape_sec_links',
    python_callable=get_sec_zip_links,
    dag=dag,
)

download_task = PythonOperator(
    task_id='download_sec_zip',
    python_callable=download_sec_zip,
    dag=dag,
)

extract_task = PythonOperator(
    task_id='extract_sec_data',
    python_callable=extract_sec_data,
    dag=dag,
)

upload_task = PythonOperator(
    task_id='upload_to_s3',
    python_callable=upload_to_s3,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_to_snowflake',
    python_callable=load_snowflake,
    dag=dag,
)

dbt_run_task = BashOperator(
    task_id="run_dbt_transformations",
    bash_command="dbt run --profiles-dir /opt/airflow/dbt_project/",
    dag=dag,
)

# Task dependencies
scrape_task >> download_task >> extract_task >> upload_task >> load_task >> dbt_run_task
