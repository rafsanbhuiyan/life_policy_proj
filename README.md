# Preston Ventures Data Engineering Project

## Data Engineer: Rafsan Bhuiyan

This project aims to create a clean, normalized database from a given life insurance policy dataset and build a RESTful API to interact with it. The data is provided by multiple data providers with varying priorities, and the goal is to prioritize the data by provider when conflicts arise.

The dataset contains various details about life insurance policies, including effective dates, issue and maturity dates, death benefits, and carrier details. Each policy can cover one or two insured individuals.
 
# IDE and Softwares
- Pycharm
- MySQLWorkbench

# 

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

