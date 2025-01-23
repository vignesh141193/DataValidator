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
    cursor.close()
    return metadata


# Streamlit app
def main():
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

    if st.button('Fetch Metadata'):
            conn = connect_snowflake(config)
            metadata = get_snowflake_metadata(conn, config['table'])
            
            # Show metadata in a table without specifying column names
            metadata_df = pd.DataFrame(metadata)
            st.write("Snowflake Table Metadata:")
            st.write(metadata_df)

if __name__ == "__main__":
    main()
