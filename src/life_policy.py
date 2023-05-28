from core.utility import get_resouce, load_yaml
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

# 1 Transform and Prepare Raw Data
# Rename Columns
# Column Mapping
str_map_dict = {"name_1": "primary_name", "gender_1": "primary_gender", "birth_date_1": "primary_birth_date",
                "name_2": "secondary_name", "gender_2": "secondary_gender", "birth_date_2": "secondary_birth_date"}

# Rename column in the DataFrame
life_policy_raw_df.rename(columns=str_map_dict, inplace=True)

# Replace NaN with None
life_policy_raw_df = life_policy_raw_df.where(pd.notnull(life_policy_raw_df), None)

# Insert data from life_policy_raw_df to 'policy_all_data' table in life_policy_db schema
db.insert_data('policy_all_data', life_policy_raw_df)


# 2 Normalize and dedup raw data




#Close Connection
db.close_connection()
