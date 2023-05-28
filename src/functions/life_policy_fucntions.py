import random
import string

def generate_random_alphanumeric(length):
    """This function generates random alphanumeric strings"""

    # Define the universe of possible characters (digits + uppercase and lowercase letters)
    chars = string.ascii_letters + string.digits
    # Use random.choices to select 'length' number of characters from the universe
    return ''.join(random.choices(chars, k=length))


def assign_policyholder_id(df, username_col_str, id_col_str):
    """This function assign unique_id based on Primary Policy_Holder Names"""

    # Remove white spaces from the 'username' column
    df[username_col_str] = df[username_col_str].str.strip()

    # Get list of Unique Usernames
    unique_user_names = df[username_col_str].unique()

    # Create a dictionary to map each unique username to a unique ID.
    id_mapping = {username: str.lower(generate_random_alphanumeric(6)) for username in unique_user_names}

    # Create a new column in the DataFrame, 'user_id', by mapping the 'username' column
    # to the IDs in the user_id_mapping dictionary.
    df[id_col_str] = df[username_col_str].map(id_mapping)

    return df


def dedup_policy(df, name, dob, priority):
    """Deduplicate by Name and Age, keeping the one provided by the highest priority provider"""

    # Sort by name birth_date and priority levels in ascending order
    df = df.sort_values(by=[name, dob, priority], ascending=[True, True, True])

    #  Deduplicate by Name and DOB, keeping the one provided by the highest priority provider
    df = df.drop_duplicates(subset=[name, dob], keep='first')

    return df


