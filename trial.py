import streamlit as st
import snowflake.connector
import pandas as pd

# Function to connect to Snowflake
def connect_snowflake(config):
    conn = snowflake.connector.connect(
        user=config['user'],
        password=config['password'],
        account=config['account'],
        role=config['role'],
        warehouse=config['warehouse'],
        database=config['database'],
        schema=config['schema']
    )
    return conn

# Function to fetch Snowflake table metadata
def get_snowflake_metadata(conn, table_name):
    cursor = conn.cursor()
    cursor.execute(f"DESC TABLE {table_name}")
    metadata = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]
    cursor.close()
    return metadata, column_names

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
    st.title("Schema Validation App")

    # Snowflake connection details
    config = {
        'account': st.text_input('Snowflake Account'),
        'user': st.text_input('Snowflake User'),
        'password': st.text_input('Snowflake Password', type='password'),
        'role': st.text_input('Snowflake Role'),
        'warehouse': st.text_input('Snowflake Warehouse'),
        'database': st.text_input('Snowflake Database'),
        'schema': st.text_input('Snowflake Schema'),
        'table': st.text_input('Snowflake Table Name')
    }

    # Upload mapping document
    uploaded_file = st.file_uploader("Choose a CSV file", type="csv")
    
    if uploaded_file is not None:
        mapping_df = pd.read_csv(uploaded_file)
        st.write("Mapping Document:")
        st.write(mapping_df)

        if st.button('Fetch Metadata'):
            conn = connect_snowflake(config)
            metadata, column_names = get_snowflake_metadata(conn, config['table'])
            
            # Show metadata in a table with dynamically fetched column names
            metadata_df = pd.DataFrame(metadata, columns=column_names)
            st.write("Snowflake Table Metadata:")
            st.write(metadata_df)
            
            # Store metadata and mapping_df in session state
            st.session_state['metadata'] = metadata
            st.session_state['column_names'] = column_names
            st.session_state['mapping_df'] = mapping_df
            
            conn.close()

    if 'metadata' in st.session_state and 'mapping_df' in st.session_state:
        metadata = st.session_state['metadata']
        column_names = st.session_state['column_names']
        mapping_df = st.session_state['mapping_df']
        
        # Get column indices for validation from both mapping document and metadata
        mapping_indices_input = st.text_input('Enter column indices for validation from the mapping document (comma-separated)', '0')
        metadata_indices_input = st.text_input('Enter column indices for validation from the metadata (comma-separated)', '0')
        
        mapping_indices = list(map(int, mapping_indices_input.split(',')))
        metadata_indices = list(map(int, metadata_indices_input.split(',')))

        if st.button('Validate Schema'):
            validation_results = validate_schema(mapping_df, metadata, mapping_indices, metadata_indices)
            validation_df = pd.DataFrame(validation_results)

            st.write("Validation Results:")
            st.write(validation_df)

if __name__ == "__main__":
    main()
