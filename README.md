# Life Insurance Data Engineering Project

## Data Engineer: Rafsan Bhuiyan

This project aims to create a clean, normalized database from a given life insurance policy dataset and build a RESTful API to interact with it. The data is provided by multiple data providers with varying priorities, and the goal is to prioritize the data by provider when conflicts arise.

The dataset contains various details about life insurance policies, including effective dates, issue and maturity dates, death benefits, and carrier details. Each policy can cover one or two insured individuals.
 
# IDE and Softwares

- Pycharm
- MySQLWorkbench

# Packages and Tools

- Makefile
- Pipfile
- Pipenv
- Python 3.11
- pyyaml
- pandas
- pymysql
- numpy
- flask
- requests
- flask-restx

## What is included in the eng_takehome_Rafsan_complete zipped folder?

- CREATE_TABLE_STATEMENTS.sql
- EER_SCHEMA_DIAGRAM.png
- life_policy_proj

## Tasks:
- Design a SQL database schema to store clean data from the dataset, matching the required tables. Bonus points for setting up, loading data, and tearing down the database as part of the pipeline process.

- Build an ETL pipeline in Python to stage, transform, and load the data into the final schema. The pipeline should preserve as much information from the raw data as possible.

- Design a RESTful API using Flask with endpoints to:
    - Retrieve policy information given a policy number
    - Count unique policies from a given carrier
    - Retrieve a list of all policies for a person regardless of their position on the policy
    - Count all policies linked to a given data provider code.
    
- The pipeline and API should be executable and testable locally.

- In the final submission, provide a Markdown file describing your process for each step, with instructions and examples of how to use the pipeline.

- Submission should exclude the data. Send the zipped project files to the provided email addresses.

# Process Description:

## Source Codes:

### create_db.py
- This script, create_db.py, creates a MySQL database named life_policy_db and three tables: policy_normalized, policy_holders, and policy_surplus_records. It establishes a connection to the database using connection parameters from a YAML file. The script executes SQL queries to create the database and tables, and it should be run with the necessary modules and a valid params.yml file.
    - Dependencies:
        - MySQLConnector Module
        - utility Module

### transform_and_load.py
- This script processes data from an input CSV file, performs transformations, deduplications, and inserts the cleaned data into tables in a MySQL database named life_policy_db. It establishes a connection to the database, stages and transforms the data using various functions, and inserts the processed data into corresponding tables. The script should be run with the required modules and a valid params.yml file in the same directory.
    - Dependencies
        - MySQLConnector Module
        - utility Module
        - process functions
        - transformation functions
        
### flask_api.py
- This script creates a Flask API with several endpoints for managing life policies. It establishes a connection to a MySQL database using connection parameters from a YAML file. The API endpoints retrieve policy information, policy count for a specific carrier, policies associated with a person, and policy count from a specific data provider. The script should be run as the main script, and the API runs on port 8000 in debug mode.  

### policy_info_app.py
- This script interacts with the API endpoints from the functions.api_functions module. It prompts the user for input, retrieves policy information for a given policy number, prints the policy info, retrieves the policy count for a given carrier name, prints the carrier policy count, retrieves policies associated with a given person name, prints the person's policies, and retrieves the policy count from a specific data provider code, printing the data provider policy count.

### drop_database.py
- This script establishes a connection to a MySQL database using connection parameters from a YAML file. It selects the specified database and then drops it. The script prints a message indicating a successful database drop. Finally, the database connection is closed.

## Modules and Functions:

### core.utily.py
- The provided code includes two utility functions: load_yaml() and get_resouce(). The load_yaml() function loads YAML data from a file using the yaml module and returns the loaded data. The get_resouce() function retrieves a resource file by constructing the file path and opening the file. It then uses a parser function to extract the resource data and returns it.

### functions.api_functions.py
- The provided code defines several functions that make HTTP requests to the API endpoints of a server running on http://127.0.0.1:8000.
- The get_policy_info() function retrieves policy information for a given policy number by making a GET request to the /policy-info/{policy_number} endpoint.
- The get_carrier_policy_count() function retrieves the count of policies for a specific carrier by making a GET request to the /carrier-policy-count/{carrier_name} endpoint.
- The get_person_policies() function retrieves the policies associated with a specific person by making a GET request to the /person-policies/{person_name} endpoint.
- The get_data_provider_policy_count() function retrieves the count of policies from a specific data provider by making a GET request to the /data-provider-policies/{data_provider_code} endpoint.
- Each function extracts the relevant data from the JSON response and returns it.

### functions.process.py
- The provided code includes three functions:
- generate_random_alphanumeric(length): Generates a random alphanumeric string of a specified length.
- assign_policyholder_id(df, username_col_str, id_col_str): Assigns a unique ID to each policy holder based on their primary policy holder name in a DataFrame.
- dedup_policy(df, name, dob, priority): Deduplicates policies based on policy holder name, date of birth, and priority, keeping the policy provided by the highest priority provider.
These functions can be used for generating random alphanumeric strings, assigning unique IDs to policy holders, and deduplicating policies based on specific criteria.

### functions.transformation.py
The provided code includes two functions:
- process_raw_data(df, map_dict, col_order): Renames columns, replaces NaN values with None, assigns unique IDs to policy holders based on their primary names, and reorders columns in a raw DataFrame. The function prepares the DataFrame for further processing.
- process_policy_holder_data(df, map_dict, select_col): Renames columns in a policy holder DataFrame using a mapping dictionary and selects a subset of columns based on a specified list. The function prepares the DataFrame for further processing.
These functions are used to transform and prepare raw data and policy holder data, respectively.

### service.MySQLConnector
- The provided code defines a `MySQLConnector` class for connecting to a MySQL server, creating and dropping databases, running arbitrary SQL queries, inserting data into tables, and retrieving data using various queries for a Restful API.

The class includes the following methods:

- `__init__(self, host, user, password)`: Initializes the MySQLConnector instance with the connection parameters.

- `connect_to_server(self)`: Connects to the MySQL server using the provided connection parameters.

- `create_database(self, database)`: Creates a database.

- `connect_to_db(self, database)`: Connects to a specific database.

- `run_query(self, query)`: Runs an arbitrary SQL query.

- `insert_data(self, table_name, df)`: Inserts data from a DataFrame into a specified table.

- `drop_database(self, database)`: Drops a database.

- `close_connection(self)`: Closes the MySQL connection.

- `get_policy_info(self, policy_number)`: Retrieves policy information for a specific policy number.

- `get_carrier_policy_count(self, carrier_name)`: Retrieves the count of policies for a specific carrier.

- `get_person_policies(self, person_name)`: Retrieves the policies associated with a specific person.

- `get_data_provider_policy_count(self, data_provider_code)`: Retrieves the count of policies from a specific data provider.

These methods handle database connections, SQL queries, data insertion, and data retrieval operations for the Restful API.


# Step by step instructions to set up the environment and to install any dependencies. For a Python project, this might be:

- Open MySQLWorkbench and Setup Local Instance
    - My Configuration (Macbook Pro)
        - Connection Method: Standard (TCP/I)
        - Hostname: localhost
        - Port: 3306
        - Username: root
        - Password: [Enter Here]
        - Default Schema: None <- We will create it via Python + SQL

- Open Pycharm or preferred IDE
    - **COPY AND PASTE THE 'life_policy_data.csv' in **src** DIRECTORY**
    - Refer to MakeFile for Step-by-Step Sequesce (Important: Some .py files can be ran directly through terminal using 'make [command here]'. Example: 'make create_db' or 'make drop_database')
    - Please do not run the following commands using makefile cmds (USE THE PYCHARM OR OTHER IDE UI INSTEAD)
        - flask_api
        - policy_info_app
        
- RUN THE FOLLOWING SCRIPTS IN SEQUENCE 

- ** Terminal/Shell CMD: > make setup **
- create_db.py              <-- Run via UI or make cmd
- transform_and_load.py     <-- Run via UI or make cmd
- flask_api.py              <-- Run via UI 
    - ** In termial: Click on Link to See SwaggerHub Documentation **
- policy_info_app.py        <-- Run via UI / ** API CALLS VIA TERMINAL **
    - ** Use the Interactive Prompt in Terminal to Type Inputs and Get Results **
- drop_database.py          <-- Run via UI or make cmd
    

