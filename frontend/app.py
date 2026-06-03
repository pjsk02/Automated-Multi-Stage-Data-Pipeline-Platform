import streamlit as st
import requests
from datetime import datetime
import pandas as pd

API_URL = "https://damg7245-team5-assignment2.onrender.com"  # Ensure this is correct

# Apply custom styles
st.markdown("""
    <style>
        .reportview-container {
            background-color: #F7F7F7;
        }
        .sidebar .sidebar-content {
            background-color: #2E2E2E;
            color: white;
        }
        .streamlit-expanderHeader {
            font-weight: bold;
            color: #FF6347;
        }
        .stButton>button {
            background-color: #FF6347;
            color: white;
            font-weight: bold;
        }
        .stButton>button:hover {
            background-color: #FF4500;
        }
        .stSelectbox>div>div>div {
            border: 2px solid #FF6347;
        }
        .stTextArea>div>textarea {
            border-radius: 10px;
            border: 2px solid #FF6347;
        }
    </style>
""", unsafe_allow_html=True)

st.title("Snowflake SQL Query Executor 🔍")

# Function to fetch available tables from the API based on data type
def fetch_tables(data_type):
    try:
        response = requests.get(f"{API_URL}/tables/{data_type}")
        response.raise_for_status()
        return response.json().get("tables", [])
    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching tables from Snowflake: {str(e)}")
        return []

# Function to fetch table data based on table name
def fetch_table_data(table_name):
    try:
        response = requests.get(f"{API_URL}/table_data/{table_name}")
        response.raise_for_status()
        return response.json().get("data", [])
    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching data for table {table_name}: {str(e)}")
        return []

def execute_query(query):
    try:
        response = requests.post(f"{API_URL}/execute_query/", json={"query": query})
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Error executing query: {str(e)}")
        return None
    
# Sidebar for selecting data type
st.sidebar.title("Filters")
data_types = ["Raw", "JSON", "Denormalized"]
selected_data_type = st.sidebar.selectbox("Select Data Loading Type", data_types)

# Fetch available tables based on selected data type
tables = fetch_tables(selected_data_type)

# User selects the table
if tables:
    selected_table = st.selectbox("Select Table", tables)

    # Once a table is selected, fetch and display its data
    if selected_table:
        table_data = fetch_table_data(selected_table)

        st.write(f"### Data from table: {selected_table}")

        if table_data:
            # Display the table data interactively
            st.dataframe(pd.DataFrame(table_data))
        else:
            st.write("No data available.")
else:
    st.write("No tables available to select.")

# Create SQL query input
query = st.text_area(f"Enter SQL Query for {selected_table if 'selected_table' in locals() else ''}:", height=150)

if st.button("Execute Query"):
    if query.strip():
        # Start time
        start_time = datetime.now()

        # Execute the query with a loading spinner
        with st.spinner("Executing query..."):
            response = execute_query(query)

        # End time
        end_time = datetime.now()

        if response:
            # Calculate execution time (in seconds)
            execution_time = (end_time - start_time).total_seconds()

            st.write("### Query Result:")
            columns = response.get("columns", [])
            result = response.get("data", [])

            if result:
                # Prepare the result as a list of dictionaries where each dictionary contains column names as keys
                formatted_result = [dict(zip(columns, row)) for row in result]
                st.dataframe(pd.DataFrame(formatted_result))

                # Query Details
                with st.expander("Query Details"):
                    st.write(f"**Executed Query:** `{query}`")
                    st.write(f"**Number of Rows Returned:** {len(result)}")
                    st.write(f"**Execution Time:** {execution_time:.4f} seconds")
                    st.write(f"**Start Time:** {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
                    st.write(f"**End Time:** {end_time.strftime('%Y-%m-%d %H:%M:%S')}")

                # Add CSV download button
                csv = pd.DataFrame(formatted_result).to_csv(index=False).encode('utf-8')
                st.download_button(label="Download as CSV", data=csv, file_name="query_results.csv", mime="text/csv")

            else:
                st.write("No results found.")
        else:
            st.error("Error executing query.")
    else:
        st.warning("Please enter a valid SQL query.")
