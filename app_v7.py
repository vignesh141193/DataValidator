import streamlit as st
import snowflake.connector
import pandas as pd
import os
from dotenv import load_dotenv
import pyodbc
import toml
config = toml.load("config.toml")

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

# Streamlit app
def main():
    st.title("Data Validation Tool")
    if 'mssql_connection_success' not in st.session_state and 'snowflake_connection_success' not in st.session_state:
        st.session_state['mssql_connection_success'] = False
        st.session_state['snowflake_connection_success'] = False
    col1, col2 = st.columns(2)
    with col1:
        source_db = st.selectbox('Select Source', ['Mapping Doc', 'MSSQL'])
    with col2:
        target_db = st.selectbox('Select Target', ['Snowflake'])
    col3, col4 = st.columns(2)
    # Connect to MSSQL 
    with col3:    
        if source_db == 'MSSQL':
            Mssql_Connection_type = st.radio("Connection Type", ('New', 'Existing'), key='mssql_connection_type')
            if Mssql_Connection_type == 'New':
                with st.sidebar:
                    config = {
                        'driver' : '{ODBC Driver 17 for SQL Server}',
                        'server': st.text_input('MSSQL Server', key='mssql_server'),
                        'database': st.text_input('MSSQL Database', key='mssql_database'),
                        'uid': st.text_input('MSSQL User', key='mssql_user'),
                        'password': st.text_input('MSSQL Password', type='password', key='mssql_password')
                    }
                if st.button('Connect and Save', key='connect_save_mssql'):
                    write_mssql_env_variables(config)
                    mssql_conn = connect_mssql(config)
                    if mssql_conn:
                        st.session_state['mssql_conn'] = mssql_conn
            elif Mssql_Connection_type == 'Existing':
                mssql_conn = connect_mssql()
                if mssql_conn:
                    st.session_state['mssql_conn'] = mssql_conn
    # Upload Mapping Document
        elif source_db == 'Mapping Doc':
            uploaded_file = st.file_uploader("Choose a CSV file", type="csv")
            if uploaded_file:
                try:
                    mapping_df = pd.read_csv(uploaded_file)
                    st.write("Mapping Document:")
                    st.dataframe(mapping_df)
                    st.write(f"The column count in Mapping Doc : {len(mapping_df)}")
                    st.session_state['mapping_df'] = mapping_df
                except pd.errors.EmptyDataError:
                    st.error("The uploaded file is empty.")
                except pd.errors.ParserError:
                    st.error("Failed to parse the CSV file. Please check the file format.")
                except Exception as e:
                    st.error(f"Unexpected error: {str(e)}")
    # Connect to Snowflake
    with col4:
        if target_db == 'Snowflake':
            Connection_type = st.radio("Connection Type", ('New', 'Existing'))
            if Connection_type == 'New':
                with st.sidebar:
                    config = {
                        'account': st.text_input('Snowflake Account', key='snowflake_account'),
                        'user': st.text_input('Snowflake User', key='snowflake_user'),
                        'password': st.text_input('Snowflake Password', type='password', key='snowflake_password'),
                        'role': st.text_input('Snowflake Role', key='snowflake_role'),
                        'warehouse': st.text_input('Snowflake Warehouse', key='snowflake_warehouse'),
                        'database': st.text_input('Snowflake Database', key='snowflake_database'),
                        'schema': st.text_input('Snowflake Schema', key='snowflake_schema')
                    }
                if st.button('Connect and Save'):
                    write_env_variables(config)
                    snowflake_conn = connect_snowflake(config)
                    if snowflake_conn:
                        st.session_state['snowflake_conn'] = snowflake_conn
            elif Connection_type == 'Existing':
                st.write("Connecting using saved connection details")
                snowflake_conn = connect_snowflake()
                if snowflake_conn:
                    st.session_state['snowflake_conn'] = snowflake_conn
    # Validation Type
    validation_type = st.radio("Select Validation Type", ('Schema Validation', 'Data Validation'))
    if validation_type == 'Schema Validation':
        if 'mssql_conn' in  st.session_state and st.session_state['mssql_connection_success'] :
            if 'mssql_conn' in st.session_state:
                mssql_conn = st.session_state['mssql_conn']
                try:
                    cursor = mssql_conn.cursor()
                    cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
                    tables = cursor.fetchall()
                    table_names = [table[0] for table in tables]
                    mssql_table_name = st.selectbox('Select MSSQL Table', table_names)
                except Exception as e:
                    st.error(f"Error fetching table names: {str(e)}")
            if st.button('Fetch Metadata from MS SQL Server', key='fetch_metadata_mssql'):
                mssql_conn = st.session_state['mssql_conn']
                if mssql_conn is not None:
                    try:
                        mssql_metadata, mssql_column_names = get_mssql_metadata(mssql_conn, mssql_table_name)
                        if mssql_metadata:
                            mssql_metadata_df = pd.DataFrame(mssql_metadata, columns=mssql_column_names)
                            st.write(f"The column count in mssql Metadata: {len(mssql_metadata_df)}")
                            st.write("MS SQL Server Table Metadata:")
                            st.dataframe(mssql_metadata_df)
                            st.session_state['mssql_metadata'] = mssql_metadata
                            st.session_state['mssql_column_names'] = mssql_column_names
                    except Exception as e:
                        st.error(f"Error fetching metadata: {str(e)}")
                
        if 'snowflake_conn'in st.session_state and st.session_state['snowflake_connection_success']:
            try:
                snowflake_conn = st.session_state['snowflake_conn']
                cursor = snowflake_conn.cursor()
                cursor.execute("SHOW TABLES")
                tables = cursor.fetchall()
                table_names = [table[1] for table in tables]
                snow_table_name = st.selectbox('Select Snowflake Table', table_names)
            except Exception as e:
                st.error(f"Error fetching table names: {str(e)}")
            if st.button('Fetch Metadata from Snowflake', key='fetch_metadata_snowflake'):
                    snowflake_conn = st.session_state['snowflake_conn']
                    if snowflake_conn is not None:
                        try:
                            snowflake_metadata, snow_column_names = get_snowflake_metadata(snowflake_conn, snow_table_name)
                            if snowflake_metadata:
                                snowflake_metadata_df = pd.DataFrame(snowflake_metadata, columns=snow_column_names)
                                st.write(f"The column count in Metadata: {len(snowflake_metadata_df)}")
                                st.write("Snowflake Table Metadata:")
                                st.dataframe(snowflake_metadata_df)
                                st.session_state['snowflake_metadata'] = snowflake_metadata
                                st.session_state['snow_column_names'] = snow_column_names
                        except Exception as e:
                            st.error(f"Error fetching metadata: {str(e)}")
        if 'snowflake_conn' in st.session_state and 'snowflake_metadata' in st.session_state:
            snowflake_conn = st.session_state['snowflake_conn']
            snowflake_metadata = st.session_state['snowflake_metadata']
            snow_column_names = st.session_state['snow_column_names']
            if source_db == 'Mapping Doc' and 'mapping_df' in st.session_state:
                mapping_df = st.session_state['mapping_df']
                st.write("Select Columns for Validation:")
                mapping_indices = st.multiselect(
                    'Select Columns from Mapping Document',
                    options=range(len(mapping_df.columns)),
                    format_func=lambda x: mapping_df.columns[x]
                )
                metadata_indices = st.multiselect(
                    'Select Columns from Metadata',
                    options=range(len(snow_column_names)),
                    format_func=lambda x: snow_column_names[x]
                )
                if st.button('Validate Schema', key='validate_schema_mapping') and validation_type == 'Schema Validation':
                    validation_results = map_validate_schema(mapping_df, snowflake_metadata, mapping_indices, metadata_indices)
                    validation_df = pd.DataFrame(validation_results)
                    st.write("Validation Results:")
                    st.dataframe(validation_df)
            elif source_db == 'MSSQL' and 'mssql_conn' in st.session_state:
                mssql_conn = st.session_state['mssql_conn']       
                if 'mssql_metadata' in st.session_state:
                    mssql_metadata = st.session_state['mssql_metadata']
                    mssql_column_names = st.session_state['mssql_column_names']
                    st.write("Select Columns for Validation:")
                    mssql_metadata_indices = st.multiselect(
                        'Select Columns from mssql Metadata',
                        options=range(len(mssql_column_names)),
                        format_func=lambda x: mssql_column_names[x]
                    )
                    snowflake_metadata_indices = st.multiselect(
                        'Select Columns from snowflake Metadata',
                        options=range(len(snow_column_names)),
                        format_func=lambda x: snow_column_names[x]
                    )
                    if st.button('Validate Schema') and validation_type == 'Schema Validation':
                        validation_results = meta_validate_schema(mssql_metadata, snowflake_metadata, mssql_metadata_indices, snowflake_metadata_indices)
                        validation_df = pd.DataFrame(validation_results)
                        st.write("Validation Results:")
                        st.dataframe(validation_df)
    elif validation_type == 'Data Validation':
        if 'mssql_conn' in  st.session_state and st.session_state['mssql_connection_success'] :
            if 'mssql_conn' in st.session_state:
                mssql_conn = st.session_state['mssql_conn']
                try:
                    cursor = mssql_conn.cursor()
                    cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
                    tables = cursor.fetchall()
                    table_names = [table[0] for table in tables]
                    mssql_table_name = st.selectbox('Select MSSQL Table', table_names)
                except Exception as e:
                    st.error(f"Error fetching table names: {str(e)}")
            if st.button('Fetch Data from MS SQL Server', key='fetch_data_mssql'):
                mssql_conn = st.session_state['mssql_conn']
                if mssql_conn is not None:
                    try:
                        mssql_data, mssqldata_column_names = get_mssql_data(mssql_conn, mssql_table_name)
                        if mssql_data:
                            mssql_data_df = pd.DataFrame(mssql_data, columns=mssqldata_column_names)
                            rowcount, colcount = mssql_data_df.shape
                            st.write(f"The row count in mssql Metadata: {rowcount}")
                            st.write(f"The column count in mssql Metadata: {colcount}")
                            st.write("MS SQL Server Table data:")
                            st.dataframe(mssql_data_df)
                            st.session_state['mssql_data'] = mssql_data
                            st.session_state['mssqldata_column_names'] = mssqldata_column_names
                    except Exception as e:
                        st.error(f"Error fetching data: {str(e)}")
        if 'snowflake_conn'in st.session_state and st.session_state['snowflake_connection_success']:
            try:
                snowflake_conn = st.session_state['snowflake_conn']
                cursor = snowflake_conn.cursor()
                cursor.execute("SHOW TABLES")
                tables = cursor.fetchall()
                table_names = [table[1] for table in tables]
                snow_table_name = st.selectbox('Select Snowflake Table', table_names)
            except Exception as e:
                st.error(f"Error fetching table names: {str(e)}")
            if st.button('Fetch data from Snowflake', key='fetch_data_snowflake'):
                    snowflake_conn = st.session_state['snowflake_conn']
                    if snowflake_conn is not None:
                        try:
                            snowflake_data, snowdata_column_names = get_snowflake_data(snowflake_conn, snow_table_name)
                            if snowflake_data:
                                snowflake_data_df = pd.DataFrame(snowflake_data, columns=snowdata_column_names)
                                rowcount, colcount = snowflake_data_df.shape
                                st.write(f"The row count in table: {rowcount}")
                                st.write(f"The column count in table: {colcount}")
                                st.write("Snowflake Table data:")
                                st.dataframe(snowflake_data_df)
                                st.session_state['snowflake_data'] = snowflake_data
                                st.session_state['snowdata_column_names'] = snowdata_column_names
                        except Exception as e:
                                    st.error(f"Error fetching data: {str(e)}")
        if 'snowflake_conn' in st.session_state and 'mssql_conn' in st.session_state and 'mssql_data' in st.session_state and 'snowflake_data' in st.session_state:
            snowflake_data = st.session_state['snowflake_data']
            snowdata_column_names = st.session_state['snowdata_column_names']
            mssql_data = st.session_state['mssql_data']
            mssqldata_column_names = st.session_state['mssqldata_column_names']
            st.write("Select Columns for Data Validation:")
            mssql_data_indices = st.multiselect(
            'Select Columns from MSSQL Data',
            options=range(len(mssqldata_column_names)),
            format_func=lambda x: mssqldata_column_names[x]
            )
            snowflake_data_indices = st.multiselect(
            'Select Columns from Snowflake Data',
            options=range(len(snowdata_column_names)),
            format_func=lambda x: snowdata_column_names[x]
            )
        if st.button('Validate Selected Data', key='validate_selected_data') and validation_type == 'Data Validation':
            if len(mssql_data_indices) != len(snowflake_data_indices):
                st.error("The number of selected columns from MSSQL and Snowflake must be the same.")
            else:
                selected_mssql_data = [[row[idx] for idx in mssql_data_indices] for row in mssql_data]
                selected_snowflake_data = [[row[idx] for idx in snowflake_data_indices] for row in snowflake_data]
                selected_mssql_columns = [mssqldata_column_names[idx] for idx in mssql_data_indices]
                selected_snowflake_columns = [snowdata_column_names[idx] for idx in snowflake_data_indices]
                validation_results = validate_data(selected_mssql_data, selected_snowflake_data, selected_mssql_columns, selected_snowflake_columns)
                validation_df = pd.DataFrame(validation_results)
                st.write("Validation Results:")
                st.dataframe(validation_df)

if __name__ == "__main__":
    main()
