from core.utility import get_resouce, load_yaml
from functions.life_policy_fucntions import assign_policyholder_id, dedup_policy
import service.mysql as mysql
import pandas as pd

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

# Convert Data to DataFrame
life_policy_raw_df = pd.read_csv("life_policy_data.csv")

# Transform Data

# Step 1 Transform and Prepare Raw Data
# Rename Columns
# Column Mapping
str_map_dict = {"name_1": "primary_name", "gender_1": "primary_gender", "birth_date_1": "primary_birth_date",
                "name_2": "secondary_name", "gender_2": "secondary_gender", "birth_date_2": "secondary_birth_date"}

# Rename column in the DataFrame
life_policy_raw_df.rename(columns=str_map_dict, inplace=True)

# Replace NaN with None
life_policy_raw_df = life_policy_raw_df.where(pd.notnull(life_policy_raw_df), None)

# Create numeric Unique Ids based on Primary Policy_Holder Names using assign_policy_holder_function
life_policy_raw_df = assign_policyholder_id(df=life_policy_raw_df,
                                            username_col_str="primary_name",
                                            id_col_str="policy_holder_id")

# Rearrange Columns
column_order = ["number", "policy_holder_id", "data_provider_code", "data_provider_description",
                "data_provider_priority", "effective_date", "issue_date",
                "maturity_date", "origination_death_benefit", "carrier_name",
                "primary_name", "primary_gender", "primary_birth_date",
                "secondary_name", "secondary_gender", "secondary_birth_date"]

# Reindex the DF
life_policy_raw_df = life_policy_raw_df.reindex(columns=column_order)
# Raw data ready

# Step 2 Transform: Deduplicate by Name and Age, keeping the one provided by the highest priority provider
life_policy_normalized_df = dedup_policy(df=life_policy_raw_df, name="primary_name"
                                         , dob="primary_birth_date", priority="data_provider_priority")

# Reset Index
life_policy_normalized_df.reset_index(drop=True, inplace=True)

# Prepare life_policy_normalized dataset
select_cols = ['number', 'policy_holder_id', 'data_provider_code','data_provider_description',
               'data_provider_priority', 'effective_date', 'issue_date', 'maturity_date',
               'origination_death_benefit', 'carrier_name']

# Step 2: Transformation before Insert to policy_normalized table in life_policy_db schema
# Assign subset from life_policy_normalized_df to policy_normalized_df
policy_normalized_df = life_policy_normalized_df[select_cols]

# Step 3: Get subset of columns from life_policy_normalized_df and assign to policy_holders_df
# Transformation before Insert to policy_holders table in life_policy_db schema

# Rename columns for policy_holder dataset
str_map_dict = {'policy_holder_id': 'id', 'number': 'policy_number'}
policy_holder_df = life_policy_normalized_df.rename(columns=str_map_dict)

# Select Correct Columns for policy_holder dataset
select_cols = ['id', 'policy_number', 'primary_name', 'primary_gender', 'primary_birth_date',
               'secondary_name', 'secondary_gender', 'secondary_birth_date']

policy_holders_df = policy_holder_df[select_cols]

# Step 4 Transform: Append all remaining policies from the raw data, which are not present
# in the 'policy_normalized_df', to the 'policy_surplus_records_df'

#Column Selection
select_cols = ['number', 'policy_holder_id', 'data_provider_code','data_provider_description',
               'data_provider_priority', 'effective_date', 'issue_date', 'maturity_date',
               'origination_death_benefit', 'carrier_name']

policy_surplus_records_df = life_policy_raw_df[select_cols]

# Filter policy_surplus_records_df by col number to include policies
# not in 'policy_normalized_df' Dataframe and
# add those surplus to policy_surplus_records_df object

# Convert 'policy_normalized_df['number']' column to list
exclude_policy_ids = list(policy_normalized_df['number'])

policy_surplus_records_df = \
    policy_surplus_records_df[~policy_surplus_records_df['number'].isin(exclude_policy_ids)]

#TODO: TESTING
#life_policy_normalized_df.to_csv("normalized_test1.csv", index=False)



# TODO: FOR LATER
# Insert data from life_policy_raw_df to "policy_all_data" table in life_policy_db schema
#db.insert_data("policy_all_data", life_policy_raw_df)


# 2 Normalize and dedup raw data




#Close Connection
db.close_connection()
