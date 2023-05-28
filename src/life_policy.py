from core.utility import get_resouce, load_yaml
import service.mysql as mysql
import pandas as pd

# Setting Environment Variables
# Parameter File Name
param_file_name = "params.yml"

# get_resources() function pulls variable from YAML file
params = get_resouce(param_file_name, load_yaml)

# Stage Data

# Convert Data to DataFrame
life_policy_raw_df = pd.read_csv("life_policy_data.csv")

# Tranform Data

# Rename Columns
str_map_dict = {"name1": "primary_name", "gender_1": "primary_gender", "birth_date_1": "primary_birth_date",
                "name_2": "secondary_name", "gender_2": "secondary_gender", "birth_date_2": "secondary_birth_date"}

df
print(life_policy_raw_df.columns)





# Establish Connection
db = mysql.MySQLConnector(params["mysql_creds"]["host"],
                          params["mysql_creds"]["database"],
                          params["mysql_creds"]["user"],
                          params["mysql_creds"]["password"])

