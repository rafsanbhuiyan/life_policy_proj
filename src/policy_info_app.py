from functions.api_functions import *

# Prompt the user for input
policy_number = input("Enter policy number: ")
policy_info = get_policy_info(policy_number)
print(policy_info)

carrier_name = input("Enter carrier name: ")
carrier_count = get_carrier_policy_count(carrier_name)
print(f"Carrier policy count: {carrier_count}")

person_name = input("Enter person name: ")
person_policies = get_person_policies(person_name)
print(person_policies)

data_provider_code = input("Enter data provider code: ")
data_provider_count = get_data_provider_policy_count(data_provider_code)
print(f"Data provider policy count: {data_provider_count}")