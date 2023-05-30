from flask import Flask, render_template, jsonify
import service.MySQLConnector as mysql
from core.utility import get_resouce, load_yaml

# SQL QUERY TO SELECT DB
SELECT_DB = """USE life_policy_db;"""

app = Flask(__name__)


# Setting Environment Variables
param_file_name = "params.yml"

# get_resources() function pulls variable from YAML file
params = get_resouce(param_file_name, load_yaml)

# Establish Connection to Server
db = mysql.MySQLConnector(params["mysql_creds"]["host"],
                          params["mysql_creds"]["user"],
                          params["mysql_creds"]["password"])

# Connect to DataBase
db.connect_to_db(params["mysql_creds"]["database"])

# SELECT DB
db.run_query(SELECT_DB)

g = db.get_person_policies("Alfred Young")
print(g)

@app.route('/')
def home():
    return "Welcome to the Flask API!"

@app.route('/policy-info/<string:policy_number>', methods=['GET'])
def policy_info(policy_number):
    policy_info = db.get_policy_info(policy_number)
    return jsonify(policy_info)

@app.route('/carrier-policy-count/<string:carrier_name>', methods=['GET'])
def carrier_policy_count(carrier_name):
    count = db.get_carrier_policy_count(carrier_name)
    return jsonify({'count': count})

@app.route('/person-policies/<string:person_name>', methods=['GET'])
def person_policies(person_name):
    policies = db.get_person_policies(person_name)
    return jsonify(policies)

@app.route('/data-provider-policies/<string:data_provider_code>', methods=['GET'])
def data_provider_policies(data_provider_code):
    count = db.get_data_provider_policy_count(data_provider_code)
    return jsonify({'count': count})

if __name__ == '__main__':
    app.run(debug=True)
