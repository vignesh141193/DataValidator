import streamlit as st
import snowflake.connector
import pandas as pd
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv("credentials.env")

# Function to connect to Snowflake
def connect_snowflake():
    try:
        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            role=os.getenv("SNOWFLAKE_ROLE"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
        )
        st.session_state['connection_success'] = True
        st.success("Snowflake connection successful!")
        return conn
    except Exception as e:
        st.session_state['connection_success'] = False
        st.error(f"Error connecting to Snowflake: {str(e)}")
        return None

# Function to fetch Snowflake table metadata
def get_snowflake_metadata(conn, table_name):
    try:
        cursor = conn.cursor()
        cursor.execute(f"DESC TABLE {table_name}")
        metadata = cursor.fetchall()
        column_names = [i[0] for i in cursor.description]
        cursor.close()
        return metadata, column_names
    except Exception as e:
       st.error(f"Error fetching metadata: {str(e)}")
       return None, None

# Function to validate schema based on column indices from both mapping document and metadata
def validate_schema(mapping_df, metadata, mapping_indices, metadata_indices):
    validation_results = []
    for index, row in mapping_df.iterrows():
        for map_idx, meta_idx in zip(mapping_indices, metadata_indices):
            expected_value = row.iloc[map_idx]
            try:
                actual_value = metadata[index][meta_idx]
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

# Streamlit app
def main():
    st.title("QuickSchemaCheck")
    if 'connection_success' not in st.session_state:
        st.session_state['connection_success'] = False

    # Custom CSS for color scheme
    st.markdown(
        """
        <style>
        .stApp {
            background-color: #1A1A24;
            color: #FFE600;
        }
        .stButton>button {
            background-color: #FFE600;
            color: #1A1A24;
        }
        </style>
        """,
        unsafe_allow_html=True
    )

    # Display credentials loaded from .env
    st.write("Snowflake credentials loaded from .env file.")
    
    # Snowflake table name input
    st.write("## Step 1: Connect to Snowflake table")
    table_name = st.text_input('Snowflake Table Name')
    # Step 1: Connect to Snowflake
    if st.button('Connect to Snowflake'):
        if not  table_name:
            st.error("Please enter the table name")
        else:
            conn = connect_snowflake()
            if conn:
                st.session_state['conn'] = conn

    # Step 2: Upload Mapping Document
    if st.session_state['connection_success']:
        st.write("## Step 2: Upload Mapping Document")
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
                
    # Step 3: Fetch Metadata
        if 'mapping_df' and 'conn' in st.session_state and st.button('Fetch Metadata'):
            conn =  st.session_state['conn'] 
            if conn is not None:
                try:
                    metadata, column_names = get_snowflake_metadata(conn, table_name)
                    if metadata:
                        metadata_df = pd.DataFrame(metadata, columns=column_names)
                        st.write(f"The column count in Metadata: {len(metadata_df)}")
                        st.write("Snowflake Table Metadata:")
                        st.dataframe(metadata_df)
                        st.session_state['metadata'] = metadata
                        st.session_state['column_names'] = column_names
                except Exception as e:
                    st.error(f"Error fetching metadata: {str(e)}")
                finally:
                    conn.close()

    # Step 4: Validate Schema
    if 'metadata' in st.session_state and 'mapping_df' in st.session_state:
        metadata = st.session_state['metadata']
        column_names = st.session_state['column_names']
        mapping_df = st.session_state['mapping_df']
        
        st.write("Select Columns for Validation:")
        mapping_indices = st.multiselect(
            'Select Columns from Mapping Document',
            options=range(len(mapping_df.columns)),
            format_func=lambda x: mapping_df.columns[x]
        )
        metadata_indices = st.multiselect(
            'Select Columns from Metadata',
            options=range(len(column_names)),
            format_func=lambda x: column_names[x]
        )
        
        if st.button('Validate Schema'):
            validation_results = validate_schema(mapping_df, metadata, mapping_indices, metadata_indices)
            validation_df = pd.DataFrame(validation_results)
            st.write("Validation Results:")
            st.dataframe(validation_df)

if __name__ == "__main__":
    main()
