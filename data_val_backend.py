import streamlit as st
import snowflake.connector
import pandas as pd
import os
from dotenv import load_dotenv
import pyodbc
import toml
# config = toml.load("config.toml")

# Load environment variables from .env file
load_dotenv("credentials.env")

def connect_snowflake(config=None):
    try:
        if config:
            conn = snowflake.connector.connect(
                user=config.get('user'),
                password=config.get('password'),
                account=config.get('account'),
                role=config.get('role'),
                warehouse=config.get('warehouse'),
                database=config.get('database'),
                schema=config.get('schema')
            )
        else:
            conn = snowflake.connector.connect(
                user=os.getenv("SNOWFLAKE_USER"),
                password=os.getenv("SNOWFLAKE_PASSWORD"),
                account=os.getenv("SNOWFLAKE_ACCOUNT"),
                role=os.getenv("SNOWFLAKE_ROLE"),
                warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
                database=os.getenv("SNOWFLAKE_DATABASE"),
                schema=os.getenv("SNOWFLAKE_SCHEMA")
            )
        st.success("Snowflake connection successful!")
        st.session_state['snowflake_connection_success'] = True
        return conn
    except Exception as e:
        st.session_state['snowflake_connection_success'] = False
        st.error(f"Error connecting to Snowflake: {str(e)}")
        return None
def write_env_variables(config):
    lines = []
    if os.path.exists("credentials.env"):
        with open("credentials.env", "r") as f:
            lines = f.readlines()
    preserved_lines = [line for line in lines if not line.startswith("SNOWFLAKE")]
    for key, value in config.items():
        os.environ[key] = value
    preserved_lines.extend([f"SNOWFLAKE_{key.upper()}={value}\n" for key, value in config.items()])
    with open("credentials.env", "w") as f:
        f.writelines(preserved_lines)

#Function to connect to mssql
def connect_mssql(config=None):
    try:
        if config:
            mssql_conn = pyodbc.connect(
                f'DRIVER={{ODBC Driver 17 for SQL Server}};'
                f'SERVER={config["server"]};'
                f'DATABASE={config["database"]};'
                f'UID={config["uid"]};'
                f'PWD={config["password"]}'
            )
        else:
            mssql_conn = pyodbc.connect(
                f'DRIVER={{ODBC Driver 17 for SQL Server}};'
                f'SERVER={os.getenv("MSSQL_SERVER")};'
                f'DATABASE={os.getenv("MSSQL_DATABASE")};'
                f'Trusted_Connection=yes;'
                f'UID={os.getenv("MSSQL_USER")};'
                f'PWD={os.getenv("MSSQL_PASSWORD")}')
        st.success("MSSQL connection successful!")
        st.session_state['mssql_connection_success'] = True
        return mssql_conn
    except Exception as e:
        st.session_state['mssql_connection_success'] = False
        st.error(f"Error connecting to MS SQL Server: {e}")
        return None
def write_mssql_env_variables(mssql_config):
    lines = []
    if os.path.exists("credentials.env"):
        with open("credentials.env", "r") as f:
            lines = f.readlines()
    preserved_lines = [line for line in lines if not line.startswith("MSSQL")]
    for key, value in mssql_config.items():
        os.environ[key] = value
    preserved_lines.extend([f"MSSQL_{key.upper()}={value}\n" for key, value in mssql_config.items()])
    with open("credentials.env", "w") as f:
        f.writelines(preserved_lines)

# Function to fetch MSSQL table metadata
def get_mssql_metadata(mssql_conn, mssql_table_name):
    try:
        cursor = mssql_conn.cursor()
        cursor.execute(f"""SELECT
        *
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{mssql_table_name}'""")
        mssql_metadata = cursor.fetchall()
        mssql_column_names = [desc[0] for desc in cursor.description]
        mssql_metadata = [list(row) for row in mssql_metadata]
        cursor.close()
        return mssql_metadata, mssql_column_names
    except Exception as e:
       st.error(f"Error fetching metadata: {str(e)}")
       return None, None

# Function to fetch Snowflake table metadata
def get_snowflake_metadata(snowflake_conn, snow_table_name):
    try:
        cursor = snowflake_conn.cursor()
        cursor.execute(f"DESC TABLE {snow_table_name}")
        snowflake_metadata = cursor.fetchall()
        snow_column_names = [i[0] for i in cursor.description]
        cursor.close()
        return snowflake_metadata, snow_column_names
    except Exception as e:
       st.error(f"Error fetching metadata: {str(e)}")
       return None, None

# Function to fetch MSSQL table data
def get_mssql_data(mssql_conn, mssql_table_name):
    try:
        cursor = mssql_conn.cursor()
        cursor.execute(f"select * FROM {mssql_table_name}")
        data = cursor.fetchall()
        mssqldata_column_names = [i[0] for i in cursor.description]
        cursor.close()
        data = [list(row) for row in data]
        return data, mssqldata_column_names
    except Exception as e:
        st.error(f"Error fetching data: {str(e)}")
        return None

# Function to fetch Snowflake table data
def get_snowflake_data(snowflake_conn, snow_table_name):
    try:
        cursor = snowflake_conn.cursor()
        cursor.execute(f"SELECT * FROM {snow_table_name} LIMIT 1000")
        data = cursor.fetchall()
        snowdata_columns_names = [i[0] for i in cursor.description]
        cursor.close()
        return data,snowdata_columns_names
    except Exception as e:
        st.error(f"Error fetching data: {str(e)}")
        return None
    
def normalize_value(value):
    replacements = {
        'Yes': 'Y',
        'No': 'N',
        'True': '1',
        'False': '0',
        'None': None,
        '': None,
        '0.0': 0,
        '1.0': 1,
        'NaN': None,
        'nan': None,
        'N/A': None,
        'n/a': None,
        'undefined': None,
        'Infinity': float('inf'),
        '-Infinity': float('-inf')
    }
    if isinstance(value, str):
        value = value.strip().lower()
        for key, replacement in replacements.items():
            if value == key.lower():
                return replacement
        if isinstance(value, str) and value.isdigit():
            return int(value)
# Function to validate schema based on column indices from both mapping document and metadata
def map_validate_schema(mapping_df, snowflake_metadata, mapping_indices, snowflake_metadata_indices):
    validation_results = []
    for index, row in mapping_df.iterrows():
        for map_idx, meta_idx in zip(mapping_indices, snowflake_metadata_indices):
            expected_value = row.iloc[map_idx]
            try:
                actual_value = snowflake_metadata[index][meta_idx]
                expected_value = normalize_value(expected_value)
                actual_value = normalize_value(actual_value)
                match = (expected_value == actual_value)
            except IndexError:
                actual_value = "Index out of range"
                match = False
            validation_results.append({
                'row_index': index,
                'mapping_index': map_idx,
                'metadata_index': meta_idx,
                'expected_value': expected_value,
                'actual_value': actual_value,
                'match': match
            })
    return validation_results

# Function to validate schema based on column indices from both mssql_metadata and snowflake_metadata
def meta_validate_schema(mssql_metadata, snowflake_metadata, mssql_metadata_indices, snowflake_metadata_indices):
    validation_results = []
    for index, row in enumerate(mssql_metadata):
        for src_idx, tgt_idx in zip(mssql_metadata_indices, snowflake_metadata_indices):
            expected_value = normalize_value(row[src_idx])
            try:
                actual_value = normalize_value(snowflake_metadata[index][tgt_idx])
                match = (expected_value == actual_value)
            except IndexError:
                actual_value = "Index out of range"
                match = False
            validation_results.append({
                'row_index': index,
                'mssql_metadata_index': src_idx,
                'snowflake_metadata_index': tgt_idx,
                'expected_value': expected_value,
                'actual_value': actual_value,
                'match': match
            })
    return validation_results

# Function to validate data between MSSQL and Snowflake
def validate_data(source_data, target_data, source_column_names, target_column_names):
    validation_results = []
    for src_row, tgt_row in zip(source_data, target_data):
        for src_col, tgt_col in zip(source_column_names, target_column_names):
            src_idx = source_column_names.index(src_col)
            tgt_idx = target_column_names.index(tgt_col)
            expected_value = src_row[src_idx]
            actual_value = tgt_row[tgt_idx]
            match = (expected_value == actual_value)
            validation_results.append({
                'source_column': src_col,
                'target_column': tgt_col,
                'source_value': expected_value,
                'target_value': actual_value,
                'match': match
            })
    return validation_results
