import streamlit as st
import data_val_backend as bk
import pandas as pd
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
                    bk.write_mssql_env_variables(config)
                    mssql_conn = bk.connect_mssql(config)
                    if mssql_conn:
                        st.session_state['mssql_conn'] = mssql_conn
            elif Mssql_Connection_type == 'Existing':
                mssql_conn = bk.connect_mssql()
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
                    bk.write_env_variables(config)
                    snowflake_conn = bk.connect_snowflake(config)
                    if snowflake_conn:
                        st.session_state['snowflake_conn'] = snowflake_conn
            elif Connection_type == 'Existing':
                st.write("Connecting using saved connection details")
                snowflake_conn = bk.connect_snowflake()
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
                        mssql_metadata, mssql_column_names = bk.get_mssql_metadata(mssql_conn, mssql_table_name)
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
                            snowflake_metadata, snow_column_names = bk.get_snowflake_metadata(snowflake_conn, snow_table_name)
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
                    validation_results = bk.map_validate_schema(mapping_df, snowflake_metadata, mapping_indices, metadata_indices)
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
                        validation_results = bk.meta_validate_schema(mssql_metadata, snowflake_metadata, mssql_metadata_indices, snowflake_metadata_indices)
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
                        mssql_data, mssqldata_column_names = bk.get_mssql_data(mssql_conn, mssql_table_name)
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
                            snowflake_data, snowdata_column_names = bk.get_snowflake_data(snowflake_conn, snow_table_name)
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
                validation_results = bk.validate_data(selected_mssql_data, selected_snowflake_data, selected_mssql_columns, selected_snowflake_columns)
                validation_df = pd.DataFrame(validation_results)
                st.write("Validation Results:")
                st.dataframe(validation_df)

if __name__ == "__main__":
    main()
