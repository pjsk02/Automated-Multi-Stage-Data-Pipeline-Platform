from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import requests
from zipfile import ZipFile
import time
import random
from bs4 import BeautifulSoup
import pandas as pd
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import snowflake.connector
 
 
# Load environment variables (if needed)
load_dotenv()
 
# SEC Data URL
SEC_DATA_URL = "https://www.sec.gov/data-research/sec-markets-data/financial-statement-data-sets"
 
# Output directory for converted JSON files
EXPORT_DIR = os.path.join(os.getcwd(), "exportfile")
os.makedirs(EXPORT_DIR, exist_ok=True)
SEC_DATA_DIR = os.path.join(os.getcwd(), "sec_data")
os.makedirs(SEC_DATA_DIR, exist_ok=True)


# Fetch AWS credentials from environment variables
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

# Fetch Snowflake credentials from environment variables
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

def get_connection():
    # Connect to Snowflake using the loaded environment variables
    return snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )

 
# User-Agent for requests
HEADERS = {
    "User-Agent": "Your Company Name yourname@example.com",
    "Accept-Encoding": "gzip, deflate",
    "Host": "www.sec.gov"
}
 
# Define the Airflow DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
 
dag = DAG(
    'sec_data_json_processing',
    default_args=default_args,
    description='A DAG to process SEC data',
    schedule_interval=None,  # Manually triggered, no scheduled run.
    catchup=False
)
 
 
# Task 1: Scrape SEC ZIP file links
def get_sec_zip_links(year_quarter):
    try:
        response = requests.get(SEC_DATA_URL, headers=HEADERS)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch the page. Error: {e}")
        return []
 
    soup = BeautifulSoup(response.content, 'html.parser')
    zip_links = [
        f"https://www.sec.gov{a['href']}" if a['href'].startswith('/') else a['href']
        for a in soup.find_all('a', href=True) if a['href'].endswith('.zip')
        and year_quarter in a['href']
    ]
 
    print(f"Found {len(zip_links)} ZIP link(s) for {year_quarter}.")
    return zip_links
 
# Task 2: Download SEC ZIP file
def download_sec_zip(url, output_dir):
    zip_filename = os.path.join(output_dir, url.split("/")[-1])
    print(f"Downloading {zip_filename}...")
 
    for attempt in range(5):
        try:
            response = requests.get(url, headers=HEADERS, allow_redirects=True)
            response.raise_for_status()
            with open(zip_filename, 'wb') as file:
                file.write(response.content)
            print(f"Downloaded {zip_filename}")
            return zip_filename
        except requests.exceptions.RequestException as e:
            wait_time = (2 ** attempt) + random.random()
            print(f"Request failed. Retrying in {wait_time:.2f} seconds...")
            time.sleep(wait_time)
 
    print(f"Failed to download {url} after multiple attempts.")
    return None
 
# Task 3: Extract files from the ZIP file
def extract_files(zip_file, output_dir, year_quarter, files_to_extract):
    quarter_name = zip_file.split("/")[-1].split(".")[0]  # Example: "2013q1"
    quarter_path = os.path.join(output_dir, quarter_name)
    os.makedirs(quarter_path, exist_ok=True)
 
    try:
        with ZipFile(zip_file, 'r') as zip_ref:
            zip_ref.extractall(quarter_path)
        print(f"Extracted files to {quarter_path}")
 
        specific_folder_path = os.path.join(SEC_DATA_DIR, year_quarter)
        if os.path.exists(specific_folder_path):
            print(f"✔ Folder '{year_quarter}' found inside {quarter_path}.")
            
            extracted_files = []
            for file_name in files_to_extract:
                file_path = os.path.join(specific_folder_path, file_name)
                if os.path.exists(file_path):
                    extracted_files.append(file_path)
                    print(f"✔ Extracted {file_name}")
                else:
                    print(f"⚠ File {file_name} not found in {specific_folder_path}.")
            
            return extracted_files
        else:
            print(f"⚠ Folder '{year_quarter}' not found inside {quarter_path}.")
            return None
    except Exception as e:
        print(f"Failed to extract {zip_file}. Error: {e}")
        return None
 
# Task 4: Convert extracted files to JSON and save
def convert_and_save_to_json(extracted_files, export_dir, year_quarter):
    for file_path in extracted_files:
        df = pd.read_table(file_path, delimiter="\t", lineterminator="\n")
        
        # Convert the DataFrame to JSON
        json_data = df.to_json(orient='records', lines=True)
        
        folder_name = year_quarter
        os.makedirs(os.path.join(export_dir, folder_name), exist_ok=True)
 
        # Save JSON file with the same file name (num.json, pre.json, etc.)
        json_file_name = file_path.split("/")[-1].replace(".txt", ".json")
        json_file_path = os.path.join(export_dir, folder_name, json_file_name)
        
        try:
            with open(json_file_path, "w") as json_file:
                json_file.write(json_data)
            print(f"Saved JSON: {json_file_path}")
        except Exception as e:
            print(f"Error writing file {json_file_path}: {e}")
 
# Task 5: Upload files to S3
def upload_to_s3(local_folder, s3_bucket, s3_folder):
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )
 
    for root, dirs, files in os.walk(local_folder):
        for file in files:
            if file in [".DS_Store", "read.htm"]:
                print(f"Skipping {file}, not uploading to S3.")
                continue
 
            local_file_path = os.path.join(root, file)
            relative_path = os.path.relpath(local_file_path, local_folder)
            s3_file_path = os.path.join(s3_folder, os.path.basename(relative_path))
 
            try:
                s3_client.upload_file(local_file_path, s3_bucket, s3_file_path)
                print(f"Uploaded {local_file_path} to s3://{s3_bucket}/{s3_file_path}")
            except ClientError as e:
                print(f"Error uploading {local_file_path} to S3: {e}")
 
# Main function for process
def process_and_upload_to_s3(selected_year, selected_quarter, files_to_extract):
    year_quarter = f"{selected_year}{selected_quarter.lower()}"
    zip_links = get_sec_zip_links(year_quarter)
 
    if not zip_links:
        print("No ZIP file links found.")
        return
 
    matching_zip_url = zip_links[0]
 
    if matching_zip_url:
        print(f"Found ZIP file for {year_quarter}, downloading...")
 
        zip_file = download_sec_zip(matching_zip_url, SEC_DATA_DIR)
        if zip_file:
            extracted_files = extract_files(zip_file, SEC_DATA_DIR, year_quarter, files_to_extract)
            if extracted_files:
                print(f"✔ Successfully processed {year_quarter} and extracted files: {extracted_files}")
                
                convert_and_save_to_json(extracted_files, EXPORT_DIR, year_quarter)
                
                upload_to_s3(EXPORT_DIR, S3_BUCKET_NAME, f"exportfile/{year_quarter}")
            else:
                print(f"❌ No files extracted for {year_quarter}")
        else:
            print(f"❌ Failed to download ZIP file for {year_quarter}")
    else:
        print(f"❌ No matching ZIP file found for {year_quarter}")
 
 
# Function to execute a SQL query
def execute_query(cursor, query, success_message="Query executed successfully."):
    try:
        cursor.execute(query)
        print(f"{success_message}")
    except Exception as e:
        print(f"Error executing query: {e}")
 
# Step 1: Create File Format
def create_file_format(cursor):
    query = """
    CREATE OR REPLACE FILE FORMAT my_json_format
    TYPE = 'JSON';
    """
    execute_query(cursor, query, "File format 'my_json_format' created successfully.")
 
# Step 2: Create Tables
def create_tables(cursor):
    tables = {
        "sub_json_table": """
        CREATE OR REPLACE TABLE sub_json_table (
          json_column VARIANT
        );
        """,
        "tag_json_table": """
        CREATE OR REPLACE TABLE tag_json_table (
          json_column VARIANT
        );
        """,
        "num_json_table": """
        CREATE OR REPLACE TABLE num_json_table (
          json_column VARIANT
        );
        """,
        "pre_json_table": """
        CREATE OR REPLACE TABLE pre_json_table (
          json_column VARIANT
        );
        """
    }
 
    for table_name, query in tables.items():
        execute_query(cursor, query, f"Table '{table_name}' created successfully.")
 
# Step 3: Create External Stages
def create_stages(cursor):
    aws_key_id = AWS_ACCESS_KEY_ID
    aws_secret_key = AWS_SECRET_ACCESS_KEY
 
    stages = ["sub_json_stage", "tag_json_stage", "num_json_stage", "pre_json_stage"]
 
    for stage_name in stages:
        query = f"""
        CREATE OR REPLACE STAGE {stage_name}
        URL = 's3://bigdata2025assignment2/exportfile/'
        FILE_FORMAT = my_json_format
        CREDENTIALS = (AWS_KEY_ID = '{aws_key_id}' AWS_SECRET_KEY = '{aws_secret_key}');
        """
        execute_query(cursor, query, f"External stage '{stage_name}' created successfully.")
 
# Step 4: Load Data from S3 to Snowflake
def load_data(cursor):
    copy_queries = {
        "sub_json_table": """
        COPY INTO sub_json_table (json_column)
        FROM @sub_json_stage
        PATTERN = '.*sub[.]json'
        FILE_FORMAT = (FORMAT_NAME = my_json_format)
        ON_ERROR = 'CONTINUE';
        """,
        "tag_json_table": """
        COPY INTO tag_json_table (json_column)
        FROM @tag_json_stage
        PATTERN = '.*tag[.]json'
        FILE_FORMAT = (FORMAT_NAME = my_json_format)
        ON_ERROR = 'CONTINUE';
        """,
        "num_json_table": """
        COPY INTO num_json_table (json_column)
        FROM @num_json_stage
        PATTERN = '.*num[.]json'
        FILE_FORMAT = (FORMAT_NAME = my_json_format)
        ON_ERROR = 'CONTINUE';
        """,
        "pre_json_table": """
        COPY INTO pre_json_table (json_column)
        FROM @pre_json_stage
        PATTERN = '.*pre[.]json'
        FILE_FORMAT = (FORMAT_NAME = my_json_format)
        ON_ERROR = 'CONTINUE';
        """
    }
 
    for table_name, query in copy_queries.items():
        execute_query(cursor, query, f"Data successfully loaded into '{table_name}'.")
 
# Step 5: Verify Data Count
def verify_data(cursor):
    tables = ["sub_json_table", "tag_json_table", "num_json_table", "pre_json_table"]
 
    for table in tables:
        verify_query = f"SELECT COUNT(*) FROM {table};"
        try:
            cursor.execute(verify_query)
            result = cursor.fetchone()
            print(f"Total rows in '{table}': {result[0]}")
        except Exception as e:
            print(f"Error verifying '{table}': {e}")
 
 
# Task 1: Scrape SEC ZIP file links
def task_get_sec_zip_links(**kwargs):
    ti = kwargs['ti']
    dag_run = kwargs['dag_run']
    
    # Get year and quarter from DAG run configuration
    selected_year = dag_run.conf.get('year')
    selected_quarter = dag_run.conf.get('quarter')
    
    if not selected_year or not selected_quarter:
        raise ValueError("Year and quarter must be provided in the DAG run configuration")
    
    year_quarter = f"{selected_year}{selected_quarter}"

    zip_links = get_sec_zip_links(year_quarter)
    kwargs['ti'].xcom_push(key='zip_links', value=zip_links)
 
# Task 2: Download ZIP file
def task_download_sec_zip(**kwargs):
    ti = kwargs['ti']
    dag_run = kwargs['dag_run']
    
    # Get year and quarter from DAG run configuration
    selected_year = dag_run.conf.get('year')
    selected_quarter = dag_run.conf.get('quarter')
    
    if not selected_year or not selected_quarter:
        raise ValueError("Year and quarter must be provided in the DAG run configuration")
    
    year_quarter = f"{selected_year}{selected_quarter}"

    zip_links = kwargs['ti'].xcom_pull(task_ids='get_sec_zip_links', key='zip_links')
    matching_zip_url = zip_links[0] if zip_links else None
    if matching_zip_url:
        zip_file = download_sec_zip(matching_zip_url, SEC_DATA_DIR)
        kwargs['ti'].xcom_push(key='zip_file', value=zip_file)
        kwargs['ti'].xcom_push(key='year_quarter', value=year_quarter)
 
# Task 3: Extract files from ZIP
def task_extract_files(**kwargs):
    zip_file = kwargs['ti'].xcom_pull(task_ids='download_sec_zip', key='zip_file')
    year_quarter = kwargs['ti'].xcom_pull(task_ids='download_sec_zip', key='year_quarter')
    files_to_extract = ['num.txt', 'sub.txt', 'pre.txt', 'tag.txt']
    extracted_files = extract_files(zip_file, SEC_DATA_DIR, year_quarter, files_to_extract)
    kwargs['ti'].xcom_push(key='extracted_files', value=extracted_files)
 
# Task 4: Convert to JSON
def task_convert_to_json(**kwargs):
    extracted_files = kwargs['ti'].xcom_pull(task_ids='extract_files', key='extracted_files')
    year_quarter = kwargs['ti'].xcom_pull(task_ids='download_sec_zip', key='year_quarter')
    convert_and_save_to_json(extracted_files, EXPORT_DIR, year_quarter)
 
 
# Task 6: Process and upload to S3 (final task)
def task_process_and_upload(**kwargs):
    ti = kwargs['ti']
    dag_run = kwargs['dag_run']
    
    # Get year and quarter from DAG run configuration
    selected_year = dag_run.conf.get('year')
    selected_quarter = dag_run.conf.get('quarter')
    
    if not selected_year or not selected_quarter:
        raise ValueError("Year and quarter must be provided in the DAG run configuration")
    
    year_quarter = f"{selected_year}{selected_quarter}"

    files_to_extract = ['num.txt', 'sub.txt', 'pre.txt', 'tag.txt']
    process_and_upload_to_s3(selected_year, selected_quarter, files_to_extract)
 
 
# Define tasks
t1 = PythonOperator(
    task_id='get_sec_zip_links',
    python_callable=task_get_sec_zip_links,
    provide_context=True,
    dag=dag )
 
t2 = PythonOperator(
    task_id='download_sec_zip',
    python_callable=task_download_sec_zip,
    provide_context=True,
    dag=dag )
 
t3 = PythonOperator(
    task_id='extract_files',
    python_callable=task_extract_files,
    provide_context=True,
    dag=dag )
 
t4 = PythonOperator(
    task_id='convert_to_json',
    python_callable=task_convert_to_json,
    provide_context=True,
    dag=dag )
 
t5 = PythonOperator(
    task_id='process_and_upload_to_s3',
    python_callable=task_process_and_upload,
    provide_context=True,
    dag=dag)
 
 
# Airflow Tasks
def task_create_file_format(**kwargs):
    conn = get_connection()
    cursor = conn.cursor()
    create_file_format(cursor)
    cursor.close()
    conn.close()
 
def task_create_tables(**kwargs):
    conn = get_connection()
    cursor = conn.cursor()
    create_tables(cursor)
    cursor.close()
    conn.close()
 
def task_create_stages(**kwargs):
    conn = get_connection()
    cursor = conn.cursor()
    create_stages(cursor)
    cursor.close()
    conn.close()
 
def task_load_data(**kwargs):
    conn = get_connection()
    cursor = conn.cursor()
    load_data(cursor)
    cursor.close()
    conn.close()
 
def task_verify_data(**kwargs):
    conn = get_connection()
    cursor = conn.cursor()
    verify_data(cursor)
    cursor.close()
    conn.close()
 
 
# Define the Airflow tasks using PythonOperator
t6 = PythonOperator(task_id='create_file_format', python_callable=task_create_file_format, dag=dag)
t7 = PythonOperator(task_id='create_tables', python_callable=task_create_tables, dag=dag)
t8 = PythonOperator(task_id='create_stages', python_callable=task_create_stages, dag=dag)
t9 = PythonOperator(task_id='load_data', python_callable=task_load_data, dag=dag)
t10 = PythonOperator(task_id='verify_data', python_callable=task_verify_data, dag=dag)
 
# Task dependencies
t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8 >> t9 >> t10
 
 
 