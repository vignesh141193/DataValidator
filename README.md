# QuickSchemaCheck

QuickSchemaCheck is a Streamlit application for connecting to a Snowflake database and validating table schemas based on a mapping document. This script uses the `streamlit`, `snowflake.connector`, and `pandas` libraries.

## Features

- Connect to a Snowflake database
- Upload and display a mapping document (CSV file)
- Fetch and display Snowflake table metadata
- Validate schema based on column indices from both the mapping document and the metadata

## Prerequisites

Make sure you have the following installed on your system:

- Python 3.6+
- pip (Python package installer)
- A Snowflake account

## Installation

1. Clone the repository:

    ```bash
    git clone https://github.com/yourusername/quickSchemaCheck.git
    cd quickSchemaCheck
    ```

2. Create a virtual environment and activate it:

    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
    ```

3. Install the required packages:

    ```bash
    pip install -r requirements.txt
    ```

## Setting Up Environment Variables

Before running the app, set up the environment variables for your Snowflake credentials to avoid hardcoding sensitive information. Create a `.env` file in the root directory of your project with the following content:

    ```bash
    SNOWFLAKE_USER=<YOUR_SNOWFLAKE_USER>
    SNOWFLAKE_PASSWORD=<YOUR_SNOWFLAKE_PASSWORD>
    SNOWFLAKE_ACCOUNT=<YOUR_SNOWFLAKE_ACCOUNT>
    SNOWFLAKE_ROLE=<YOUR_SNOWFLAKE_ROLE>
    SNOWFLAKE_WAREHOUSE=<YOUR_SNOWFLAKE_WAREHOUSE>
    SNOWFLAKE_DATABASE=<YOUR_SNOWFLAKE_DATABASE>
    SNOWFLAKE_SCHEMA=<YOUR_SNOWFLAKE_SCHEMA>
    ```

## Running the App

1. Activate your virtual environment (if not already activated).

2. Run the Streamlit app:

    ```bash
    streamlit run app.py
    ```

3. Open your web browser and go to `http://localhost:8501` to view the app.

## Usage

### Step 1: Connect to Snowflake

1. Enter your Snowflake connection details in the text input fields.
2. Click the "Connect" button to establish a connection to the Snowflake database.

### Step 2: Upload Mapping Document

1. Upload a CSV file containing the mapping document.
2. The uploaded document will be displayed as a data frame.

### Step 3: Fetch Snowflake Table Metadata

1. Click the "Fetch Metadata" button to retrieve the metadata of the specified Snowflake table.
2. The fetched metadata will be displayed as a data frame.

### Step 4: Validate Schema

1. Select columns for validation from both the mapping document and the metadata.
2. Click the "Validate Schema" button to perform schema validation.
3. The validation results will be displayed as a data frame.

## Contributing

If you'd like to contribute to this project, please fork the repository and use a feature branch. Pull requests are warmly welcome.

## License

This project is licensed under the MIT License.

## Acknowledgments

- Thanks to the contributors of the `streamlit`, `snowflake.connector`, and `pandas` libraries for their excellent tools.


