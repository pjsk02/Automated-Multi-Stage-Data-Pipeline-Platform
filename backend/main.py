from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import snowflake.connector
from typing import List, Dict, Any
import logging
from dotenv import load_dotenv
import os

# Load environment variables from the .env file
load_dotenv()

app = FastAPI()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Snowflake configuration from environment variables
SNOWFLAKE_CONFIG = {
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA"),
    "role": os.getenv("SNOWFLAKE_ROLE"),
}

class SQLQuery(BaseModel):
    query: str

def get_snowflake_connection():
    return snowflake.connector.connect(**SNOWFLAKE_CONFIG)

def execute_query_in_snowflake(query: str):
    try:
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [col[0] for col in cursor.description]  # Get column names
        result = cursor.fetchall()
        cursor.close()
        conn.close()
        return {"columns": columns, "data": result}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/tables/{data_type}")
async def get_tables(data_type: str) -> Dict[str, List[str]]:
    """
    Return filtered tables based on data type
    """
    # Debugging: log the data_type to ensure it's coming through correctly
    logger.info(f"Requested data_type: {data_type}")
    
    # Define tables based on the selected data type
    if data_type == "Raw":
        filtered_tables = ["NUM_TABLE", "SUB_TABLE", "PRE_TABLE", "TAG_TABLE"]
    elif data_type == "JSON":
        filtered_tables = ["NUM_JSON_TABLE", "PRE_JSON_TABLE", "SUB_JSON_TABLE", "TAG_JSON_TABLE"]
    elif data_type == "Denormalized":
        filtered_tables = ["FACT_BALANCE_SHEET", "FACT_CASH_FLOW", "FACT_INCOME_STATEMENT"]
    else:
        raise HTTPException(status_code=400, detail="Invalid data type")
    
    logger.info(f"Returning tables: {filtered_tables}")
    return {"tables": filtered_tables}

@app.get("/table_data/{table_name}")
async def get_table_data(table_name: str) -> Dict[str, List[Dict[str, Any]]]:
    try:
        with get_snowflake_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT * FROM {table_name} LIMIT 10")
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
            data = [dict(zip(columns, row)) for row in rows]
        return {"data": data}
    except Exception as e:
        logger.error(f"Error fetching data for table {table_name}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error fetching data for table {table_name}: {str(e)}")

@app.post("/execute_query/")
def execute_sql_query(sql_query: SQLQuery):
    result = execute_query_in_snowflake(sql_query.query)
    return result

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
