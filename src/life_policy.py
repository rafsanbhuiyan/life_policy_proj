from core.utility import get_resouce, load_yaml
from functions.process import dedup_policy
from functions.transformation import process_raw_data, process_policy_holder_data
import service.mysql as mysql
import pandas as pd

# CONSTANT VARIABLES
POLICY_DATA_COLUMN_SELECTION = ['number', 'policy_holder_id', 'data_provider_code', 'data_provider_description',
                                'data_provider_priority', 'effective_date', 'issue_date', 'maturity_date',
                                'origination_death_benefit', 'carrier_name']

# Setting Environment Variables
# Parameter File Name
param_file_name = "params.yml"

# get_resources() function pulls variable from YAML file
params = get_resouce(param_file_name, load_yaml)

# Establish Connection
db = mysql.MySQLConnector(params["mysql_creds"]["host"],
                          params["mysql_creds"]["database"],
                          params["mysql_creds"]["user"],
                          params["mysql_creds"]["password"])

# Stage Data

# Ingest Input CSV File and Convert to DataFrame
life_policy_raw_df = pd.read_csv("life_policy_data.csv")

# Transform Data

# Step 1 Transform and Prepare Raw Data
# Rename Columns
str_map_dict = {"name_1": "primary_name", "gender_1": "primary_gender", "birth_date_1": "primary_birth_date",
                "name_2": "secondary_name", "gender_2": "secondary_gender", "birth_date_2": "secondary_birth_date"}
# Rearrange Columns
column_order = ["number", "policy_holder_id", "data_provider_code", "data_provider_description",
                "data_provider_priority", "effective_date", "issue_date",
                "maturity_date", "origination_death_benefit", "carrier_name",
                "primary_name", "primary_gender", "primary_birth_date",
                "secondary_name", "secondary_gender", "secondary_birth_date"]
# Transform raw data using process_raw_data() function
life_policy_raw_df = process_raw_data(df=life_policy_raw_df, map_dict=str_map_dict,
                                      col_order=column_order)
# Step 1 Complete

# Step 2 Process Normalized Data
# Deduplicate by Name and Age, keeping the one provided by the highest priority provider
life_policy_normalized_df = dedup_policy(df=life_policy_raw_df, name="primary_name",
                                         dob="primary_birth_date", priority="data_provider_priority")
# Reset Index
life_policy_normalized_df.reset_index(drop=True, inplace=True)

# Assign subset from life_policy_normalized_df to policy_normalized_df
policy_normalized_df = life_policy_normalized_df[POLICY_DATA_COLUMN_SELECTION]
# Step 2 Complete

# Step 3 Process Policy_Holder Data
# Transform: Get subset of columns from life_policy_normalized_df and assign to policy_holders_df
# Transformation before Insert to policy_holders table in life_policy_db schema

# String Mapping for renaming columns
str_map_dict = {'policy_holder_id': 'id', 'number': 'policy_number'}
# Select Correct Columns for policy_holder dataset
select_columns = ['id', 'policy_number', 'primary_name', 'primary_gender', 'primary_birth_date',
                  'secondary_name', 'secondary_gender', 'secondary_birth_date']

# Process Policy_Holder Data
policy_holders_df = process_policy_holder_data(df=life_policy_normalized_df, map_dict=str_map_dict,
                                               select_col=select_columns)

# Step 4 Append all remaining policies from the raw data, which are not present
# in the 'policy_normalized_df', to the 'policy_surplus_records_df'

# Assign subset from 'life_policy_raw_df' to 'policy_surplus_records_df'
policy_surplus_records_df = life_policy_raw_df[POLICY_DATA_COLUMN_SELECTION]

# Filter policy_surplus_records_df by col number to include policies
# not in 'policy_normalized_df' Dataframe and
# add those surplus to policy_surplus_records_df object

# Convert 'policy_normalized_df['number']' column to list
exclude_policy_ids = list(policy_normalized_df['number'])

# Filter policy_surplus_records_df
policy_surplus_records_df = \
    policy_surplus_records_df[~policy_surplus_records_df['number'].isin(exclude_policy_ids)]

# Reset Index
policy_surplus_records_df.reset_index(drop=True, inplace=True)


# TODO: FOR LATER
# Insert data from life_policy_raw_df to "policy_all_data" table in life_policy_db schema
#db.insert_data("policy_all_data", life_policy_raw_df)

#Close Connection
db.close_connection()
