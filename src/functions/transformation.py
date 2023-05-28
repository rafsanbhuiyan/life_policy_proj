import pandas as pd
from functions.process import assign_policyholder_id


# Transform Raw Data
def process_raw_data(df, map_dict, col_order):
    # Rename column in the DataFrame
    df.rename(columns=map_dict, inplace=True)
    # Replace NaN with None
    df = df.where(pd.notnull(df), None)
    # Create numeric Unique Ids based on Primary Policy_Holder Names using assign_policy_holder_function
    df = assign_policyholder_id(df=df,
                                username_col_str="primary_name",
                                id_col_str="policy_holder_id")
    # Reindex the DF
    df = df.reindex(columns=col_order)
    print("Raw Dataframe Ready")

    return df


# Transform Policy Holder Dataframe
def process_policy_holder_data(df, map_dict, select_col):
    # Rename
    df = df.rename(columns=map_dict)
    # Select Columns
    df = df[select_col]

    return df
