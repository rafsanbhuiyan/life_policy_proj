from flask import Flask, jsonify
from flask_restx import Api, Resource

import service.MySQLConnector as mysql
from core.utility import get_resouce, load_yaml

# SQL QUERY TO SELECT DB
SELECT_DB = """USE life_policy_db;"""

app = Flask(__name__)
api = Api(app, version='3.0.0', title='Flask API', description='API endpoints for life policy management',
          default='Life Policy Project')

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


@api.route('/')
class Home(Resource):
    def get(self):
        """
        Welcome message
        """
        return "Welcome to the Flask API!"


@api.route('/policy-info/<string:policy_number>')
class PolicyInfo(Resource):
    def get(self, policy_number):
        """
        Returns policy information for a specific policy number.
        """
        policy_info = db.get_policy_info(policy_number)
        return jsonify(policy_info)


@api.route('/carrier-policy-count/<string:carrier_name>')
class CarrierPolicyCount(Resource):
    def get(self, carrier_name):
        """
        Returns the count of policies for a specific carrier.
        """
        count = db.get_carrier_policy_count(carrier_name)
        return jsonify({'count': count})


@api.route('/person-policies/<string:person_name>')
class PersonPolicies(Resource):
    def get(self, person_name):
        """
        Returns the policies associated with a specific person.
        """
        policies = db.get_person_policies(person_name)
        return jsonify(policies)


@api.route('/data-provider-policies/<string:data_provider_code>')
class DataProviderPolicies(Resource):
    def get(self, data_provider_code):
        """
        Returns the count of policies from a specific data provider.
        """
        count = db.get_data_provider_policy_count(data_provider_code)
        return jsonify({'count': count})


if __name__ == '__main__':
    app.run(port=8000, debug=True)
