import requests

base_url = "http://127.0.0.1:5000"


def get_policy_info(policy_number):
    response = requests.get(f"{base_url}/policy-info/{policy_number}")
    data = response.json()
    return data


def get_carrier_policy_count(carrier_name):
    response = requests.get(f"{base_url}/carrier-policy-count/{carrier_name}")
    data = response.json()
    return data['count']


def get_person_policies(person_name):
    response = requests.get(f"{base_url}/person-policies/{person_name}")
    data = response.json()
    return data


def get_data_provider_policy_count(data_provider_code):
    response = requests.get(f"{base_url}/data-provider-policies/{data_provider_code}")
    data = response.json()
    return data['count']