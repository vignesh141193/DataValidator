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
    st.success("Snowflake connection successfull")
    return conn
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

    if st.button('connect'):
        conn = connect_snowflake(config)
if __name__ == "__main__":
    main()
