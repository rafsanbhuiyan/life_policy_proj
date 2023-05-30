# Preston Ventures Data Engineering Project

## Data Engineer: Rafsan Bhuiyan

This project aims to create a clean, normalized database from a given life insurance policy dataset and build a RESTful API to interact with it. The data is provided by multiple data providers with varying priorities, and the goal is to prioritize the data by provider when conflicts arise.

The dataset contains various details about life insurance policies, including effective dates, issue and maturity dates, death benefits, and carrier details. Each policy can cover one or two insured individuals.
 
# IDE and Softwares
- Pycharm
- MySQLWorkbench

# Packages and Tools
- Pipenv
- Python 3.11
- pyyaml
- pandas
- pymysql
- numpy
- flask
- requests
- flask-restx

# What is included in the eng_takehome_Rafsan_complete zipped folder?
- CREATE_TABLE_STATEMENTS.sql
- EER_SCHEMA_DIAGRAM.png
- life_policy_proj

# Task:
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
    - Refer to MakeFile for Step-by-Step Sequesce (Important: Some .py files can be ran directly through terminal using 'make [command here]'. Example: 'make create_db' or 'make drop_database')
    - Please do not run the following commands using makefile cmds (USE THE PYCHARM OR OTHER IDE UI INSTEAD)
        - flask_api
        - policy_info_app
        
## RUN THE FOLLOWING SCRIPTS IN SEQUENCE 

- ** Terminal/Shell CMD: > make setup **
- create_db.py              <-- Run via UI or make cmd
- transform_and_load.py     <-- Run via UI or make cmd
- flask_api.py              <-- Run via UI 
    - ** In termial: Click on Link to See SwaggerHub Documentation **
- policy_info_app.py        <-- Run via UI / ** API CALLS VIA TERMINAL **
    - ** Use the Interactive Prompt in Terminal to Type Inputs and Get Results **
- drop_database.py          <-- Run via UI or make cmd
    

