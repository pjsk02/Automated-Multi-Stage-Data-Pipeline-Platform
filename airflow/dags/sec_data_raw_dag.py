from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import requests
from zipfile import ZipFile
import time
import random
from bs4 import BeautifulSoup
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
from dotenv import load_dotenv
import snowflake.connector
 
# Load environment variables
load_dotenv()
 
# SEC Data URL
SEC_DATA_URL = "https://www.sec.gov/data-research/sec-markets-data/financial-statement-data-sets"
 
# User-Agent for requests
HEADERS = {
    "User-Agent": "Your Company Name yourname@example.com",
    "Accept-Encoding": "gzip, deflate",
    "Host": "www.sec.gov"
}
 
# Output directory for extracted files
SEC_DATA_DIR = os.path.join(os.getcwd(), "sec_data")
os.makedirs(SEC_DATA_DIR, exist_ok=True)
 
 
# Fetch credentials from environment variables
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
    ]
 
    print(f"Found {len(zip_links)} ZIP links.")
    return zip_links
 
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
 
def extract_specific_files(zip_file, output_dir, year_quarter, files_to_extract):
    quarter_name = zip_file.split("/")[-1].split(".")[0]
    quarter_path = os.path.join(output_dir, quarter_name)
    os.makedirs(quarter_path, exist_ok=True)
 
    try:
        with ZipFile(zip_file, 'r') as zip_ref:
            zip_ref.extractall(quarter_path)
        print(f"Extracted files to {quarter_path}")
 
        specific_folder_path = os.path.join(SEC_DATA_DIR, year_quarter)
        if os.path.exists(specific_folder_path):
            print(f"✔ Folder '{year_quarter}' found inside {quarter_path}.")
            
            files_found = []
            for file_name in files_to_extract:
                file_path = os.path.join(specific_folder_path, file_name)
                if os.path.exists(file_path):
                    files_found.append(file_path)
                    print(f"✔ Found and extracted {file_name}")
                else:
                    print(f"⚠ File {file_name} not found in {specific_folder_path}.")
            
            return files_found
        else:
            print(f"⚠ Folder '{year_quarter}' not found inside {quarter_path}.")
            return None
    except Exception as e:
        print(f"Failed to extract {zip_file}. Error: {e}")
        return None
 
def upload_to_s3(files, s3_bucket, year_quarter):
    s3_prefix = f"sec_data/{year_quarter}"
 
    for file in files:
        file_name = os.path.basename(file)
        s3_path = f"{s3_prefix}/{file_name}"
 
        try:
            print(f"Uploading {file_name} to S3...")
            s3_client.upload_file(file, s3_bucket, s3_path)
            print(f"✔ Successfully uploaded {file_name} to {s3_path}")
        except (NoCredentialsError, ClientError) as e:
            print(f"❌ Failed to upload {file}. Error: {e}")
        except Exception as e:
            print(f"❌ Unexpected error during upload of {file}. Error: {e}")
 
# Airflow DAG definition
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
    'sec_data_raw_processing',
    default_args=default_args,
    description='A DAG to process SEC data',
    schedule_interval=None,  # Manually triggered, no scheduled run.
    catchup=False
)
 
# Task 1: Scrape SEC ZIP links
def task_get_sec_zip_links(**kwargs):
    ti = kwargs['ti']
    print("Fetching SEC ZIP links...")
    zip_links = get_sec_zip_links()
    print(f"ZIP Links fetched: {zip_links}")  # Debug log
    ti.xcom_push(key='zip_links', value=zip_links)
    print("Task 1 completed: Retrieved SEC ZIP links")
 
# Task 2: Download SEC ZIP file
def task_download_sec_zip(**kwargs):
    ti = kwargs['ti']
    dag_run = kwargs['dag_run']
    
    # Get year and quarter from DAG run configuration
    selected_year = dag_run.conf.get('year')
    selected_quarter = dag_run.conf.get('quarter')
    
    if not selected_year or not selected_quarter:
        raise ValueError("Year and quarter must be provided in the DAG run configuration")
    
    year_quarter = f"{selected_year}{selected_quarter}"
    
    zip_links = ti.xcom_pull(task_ids='get_sec_zip_links', key='zip_links')
    print(f"Retrieved ZIP Links: {zip_links}")  # Debug log
    matching_zip_url = next((link for link in zip_links if selected_year in link and selected_quarter in link), None)
    
    if matching_zip_url:
        print(f"Downloading ZIP file for {year_quarter} from {matching_zip_url}...")
        zip_file = download_sec_zip(matching_zip_url, SEC_DATA_DIR)
        if zip_file:
            ti.xcom_push(key='zip_file', value=zip_file)
            ti.xcom_push(key='year_quarter', value=year_quarter)
            print(f"Task 2 completed: Downloaded ZIP file for {year_quarter}")
        else:  
            raise Exception(f"Failed to download ZIP file for {year_quarter}")
    else:
        raise Exception(f"No matching ZIP file found for {year_quarter}")
 
 
# Task 3: Extract specific files
def task_extract_specific_files(**kwargs):
    ti = kwargs['ti']
    zip_file = ti.xcom_pull(task_ids='download_sec_zip', key='zip_file')
    year_quarter = ti.xcom_pull(task_ids='download_sec_zip', key='year_quarter')
    files_to_extract = ['num.txt', 'sub.txt', 'pre.txt', 'tag.txt']
 
    print(f"Extracting specific files for {year_quarter} from {zip_file}...")
    extracted_files = extract_specific_files(zip_file, SEC_DATA_DIR, year_quarter, files_to_extract)
    
    if extracted_files:
        ti.xcom_push(key='extracted_files', value=extracted_files)
        print(f"Task 3 completed: Extracted files for {year_quarter}")
    else:
        raise Exception(f"Failed to extract files for {year_quarter}")
 
# Task 4: Upload files to S3
def task_upload_to_s3(**kwargs):
    ti = kwargs['ti']
    extracted_files = ti.xcom_pull(task_ids='extract_specific_files', key='extracted_files')
    year_quarter = ti.xcom_pull(task_ids='download_sec_zip', key='year_quarter')
 
    if extracted_files:
        print(f"Uploading extracted files to S3 for {year_quarter}...")
        upload_to_s3(extracted_files, S3_BUCKET_NAME, year_quarter)
        print(f"Task 4 completed: Uploaded files to S3 for {year_quarter}")
    else:
        print(f"❌ No extracted files found for upload to S3 for {year_quarter}")
        raise Exception(f"No extracted files found for upload to S3 for {year_quarter}")
    
def execute_query(cursor, query, success_message="Query executed successfully."):
    try:
        cursor.execute(query)
        print(f"{success_message}")
    except Exception as e:
        print(f"Error executing query: {e}")
 
# Step 1: Create File Format
def create_file_format(cursor):
    query = """
    CREATE OR REPLACE FILE FORMAT txt_format
    TYPE = 'CSV'
    FIELD_DELIMITER = '\t'
    RECORD_DELIMITER = '\n'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    ESCAPE_UNENCLOSED_FIELD = None
    SKIP_HEADER = 1
    NULL_IF = ('', 'NULL', 'null')
    ESCAPE = 'NONE'
    TRIM_SPACE = TRUE;
    """
    execute_query(cursor, query, "File format 'txt_format' created successfully.")
 
# Step 2: Create Tables
def create_tables(cursor):
    tables = {
        "sub_table": """
          CREATE OR REPLACE TABLE sub_table (
            adsh STRING(20) NOT NULL,
            cik STRING(10) NOT NULL,
            name STRING(150) NOT NULL,
            sic STRING(50),
            countryba STRING(50),
            stprba STRING(50),
            cityba STRING(30),
            zipba STRING(120),  
            bas1 STRING(120),
            bas2 STRING(120),
            baph STRING(20),
            countryma STRING(50),
            stprma STRING(50),
            cityma STRING(30),
            zipma STRING(120),   
            mas1 STRING(120),    
            mas2 STRING(120),    
            countryinc STRING(50),
            stprinc STRING(50),
            ein STRING(50),
            former STRING(150),
            changed STRING(8),
            afs STRING(10),
            wksi STRING(10),
            fye STRING(20),
            form STRING(20),
            period STRING(20),
            fy STRING(20),
            fp STRING(50),
            filed STRING(50),
            accepted STRING(30),
            prevrpt STRING(50),
            detail STRING(50),
            instance STRING(40),
            nciks STRING(50),
            aciks STRING(200) NULL
          );
        """,
        "tag_table": """
        CREATE OR REPLACE TABLE tag_table (
           tag STRING(256) NOT NULL,
           version STRING(20) NOT NULL,
           custom STRING(20) NOT NULL,  
           abstract STRING(20) NOT NULL,
           datatype STRING(100),
           iord STRING(100),
           crdr STRING(100),
           tlabel STRING(512),
           doc STRING
        );
        """,
        "num_table": """
          CREATE OR REPLACE TABLE num_table (
                adsh STRING(20) NOT NULL,
                tag STRING(256) NOT NULL,
                version STRING(20) NOT NULL,
                ddate STRING(8) NOT NULL,
                qtrs STRING(8) NOT NULL,
                uom STRING(20) NOT NULL,
                segments STRING(1024),
                coreg STRING(256),
                value STRING,
                footnote STRING(512)
            );
        """,
        "pre_table": """
            CREATE OR REPLACE TABLE pre_table (
              adsh STRING(20) NOT NULL,
              report STRING(6) NOT NULL,
              line STRING(6) NOT NULL,
              stmt STRING(20),
              inpth STRING(5),
              rfile STRING(20) NOT NULL,
              tag STRING(256) NOT NULL,
              version STRING(20) NOT NULL,
              plabel STRING(512),
              negating STRING
            );
        """
    }
 
    for table_name, query in tables.items():
        execute_query(cursor, query, f"Table '{table_name}' created successfully.")
 
# Step 3: Create External Stages
def create_stages(cursor):
    aws_key_id = AWS_ACCESS_KEY_ID
    aws_secret_key = AWS_SECRET_ACCESS_KEY
 
    stages = ["sub_stage", "tag_stage", "num_stage", "pre_stage"]
 
    for stage_name in stages:
        query = f"""
        CREATE OR REPLACE STAGE {stage_name}
        URL = 's3://bigdata2025assignment2/sec_data/'
        FILE_FORMAT = txt_format
        CREDENTIALS = (AWS_KEY_ID = '{aws_key_id}' AWS_SECRET_KEY = '{aws_secret_key}');
        """
        execute_query(cursor, query, f"External stage '{stage_name}' created successfully.")
 
# Step 4: Load Data from S3 to Snowflake
def load_data(cursor):
    copy_queries = {
        "sub_table": """
          COPY INTO sub_table (
          adsh, cik, name, sic, countryba, stprba, cityba, zipba, bas1, bas2, baph,
          countryma, stprma, cityma, zipma, mas1, mas2, countryinc, stprinc, ein,
          former, changed, afs, wksi, fye, form, period, fy, fp, filed, accepted,
          prevrpt, detail, instance, nciks, aciks
        )
        FROM @sub_stage
        FILE_FORMAT = txt_format
        PATTERN = '.*sub[.]txt'
        ON_ERROR = 'CONTINUE';
        """,
        "tag_table": """
         COPY INTO tag_table
          FROM @tag_stage
          FILE_FORMAT = (
          TYPE = 'CSV'
          FIELD_DELIMITER = '\t'
          RECORD_DELIMITER = '\n'
          FIELD_OPTIONALLY_ENCLOSED_BY = '"'
          SKIP_HEADER = 1
          ESCAPE_UNENCLOSED_FIELD = None
          NULL_IF = ('NULL', 'null', '')  
          )
          PATTERN = '.*tag[.]txt';
        """,
        "num_table": """
            COPY INTO num_table
            FROM @num_stage
            FILE_FORMAT = (
            TYPE = 'CSV'
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            FIELD_DELIMITER = '\t'
            NULL_IF = ('NULL', 'null', '')
            )
            PATTERN = '.*num[.]txt'
            ON_ERROR = 'CONTINUE';
        """,
        "pre_table": """
            COPY INTO pre_table
            FROM @pre_stage
            FILE_FORMAT = (
            TYPE = 'CSV'
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            FIELD_DELIMITER = '\t'
            NULL_IF = ('NULL', 'null', '')
            ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE  
            )
            PATTERN = '.*pre[.]txt'
            ON_ERROR = 'CONTINUE';
        """
    }
 
    for table_name, query in copy_queries.items():
        execute_query(cursor, query, f"Data successfully loaded into '{table_name}'.")
 
# Step 5: Verify Data Count
def verify_data(cursor):
    tables = ["sub_table", "tag_table", "num_table", "pre_table"]
 
    for table in tables:
        verify_query = f"SELECT COUNT(*) FROM {table};"
        try:
            cursor.execute(verify_query)
            result = cursor.fetchone()
            print(f"Total rows in '{table}': {result[0]}")
        except Exception as e:
            print(f"Error verifying '{table}': {e}")
 
def task_snowflake_setup(**kwargs):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        create_file_format(cursor)
        create_tables(cursor)
        create_stages(cursor)
    finally:
        cursor.close()
        conn.close()
    print("Task 1 completed: Snowflake setup completed.")
 
# Task 2: Load Data from S3 to Snowflake
def task_load_data(**kwargs):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        load_data(cursor)
    finally:
        cursor.close()
        conn.close()
    print("Task 2 completed: Data loaded into Snowflake.")
 
# Task 3: Verify Data Count in Snowflake
def task_verify_data(**kwargs):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        verify_data(cursor)
    finally:
        cursor.close()
        conn.close()
    print("Task 3 completed: Data count verified in Snowflake.")
 
# Define tasks
t1 = PythonOperator(
    task_id='get_sec_zip_links',
    python_callable=task_get_sec_zip_links,
    dag=dag,
)
 
t2 = PythonOperator(
    task_id='download_sec_zip',
    python_callable=task_download_sec_zip,
    dag=dag,
)
 
t3 = PythonOperator(
    task_id='extract_specific_files',
    python_callable=task_extract_specific_files,
    dag=dag,
)
 
t4 = PythonOperator(
    task_id='upload_to_s3',
    python_callable=task_upload_to_s3,
    dag=dag,
)
 
t5 = PythonOperator(
    task_id='snowflake_setup',
    python_callable=task_snowflake_setup,
    dag=dag,
)
 
t6 = PythonOperator(
    task_id='load_data_to_snowflake',
    python_callable=task_load_data,
    dag=dag,
)
 
t7 = PythonOperator(
    task_id='verify_data_in_snowflake',
    python_callable=task_verify_data,
    dag=dag,
)
 
# Set task dependencies
 
 
# Set task dependencies
t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7
has context menu
