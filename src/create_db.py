import service.MySQLConnector as mysql
from core.utility import get_resouce, load_yaml

"""
    create_db.py
    
    This script CREATES the following:
    - Database: life_policy_db
    - Table 1:  policy_normalized
    - Table 2:  policy_holders
    - Table 3:  policy_surplus_records
"""

# SQL QUERIES
DATABASE = "life_policy_db"

SELECT_DB = """USE life_policy_db;"""

CREATE_TBL_POLICY_NORMALIZED = """CREATE TABLE policy_normalized(
                                    number VARCHAR(15) NOT NULL,
                                    policy_holder_id VARCHAR(7) NOT NULL,
                                    data_provider_code VARCHAR(3),
                                    data_provider_description VARCHAR(50),
                                    data_provider_priority INT,
                                    effective_date DATE,
                                    issue_date DATE,
                                    maturity_date DATE,
                                    origination_death_benefit DECIMAL(10,2),
                                    carrier_name VARCHAR(50),
                                    PRIMARY KEY(number)
                                );"""

CREATE_TBL_POLICY_HOLDERS = """CREATE TABLE policy_holders (
                                  id VARCHAR(7) NOT NULL,
                                  policy_number VARCHAR(15) NOT NULL,
                                  primary_name VARCHAR(50),
                                  primary_gender CHAR(1),
                                  primary_birth_date DATE,
                                  secondary_name VARCHAR(50),
                                  secondary_gender CHAR(1),
                                  secondary_birth_date DATE,
                                  PRIMARY KEY(id),
                                  FOREIGN KEY(policy_number) REFERENCES policy_normalized(number)
                                );"""

CREATE_TBL_POLICY_SURPLUS_RECORDS = """CREATE TABLE policy_surplus_records(
                                        number VARCHAR(15) NOT NULL,
                                        policy_holder_id VARCHAR(7) NOT NULL,
                                        data_provider_code VARCHAR(3),
                                        data_provider_description VARCHAR(50),
                                        data_provider_priority INT,
                                        effective_date DATE,
                                        issue_date DATE,
                                        maturity_date DATE,
                                        origination_death_benefit DECIMAL(10,2),
                                        carrier_name VARCHAR(50),
                                        PRIMARY KEY(number),
                                        FOREIGN KEY(policy_holder_id) REFERENCES policy_holders(id)
                                    );"""


def main():
    # Setting Environment Variables
    param_file_name = "params.yml"

    # get_resources() function pulls variable from YAML file
    params = get_resouce(param_file_name, load_yaml)

    # Establish Connection
    db = mysql.MySQLConnector(params["mysql_creds"]["host"],
                              params["mysql_creds"]["user"],
                              params["mysql_creds"]["password"])

    # SQL PROCESSES

    # CREATE DATABASE 'life_policy_db'
    db.create_database(DATABASE)

    # SELECT DATABASE TO USE
    db.run_query(SELECT_DB)

    # CREATE TABLE 'policy_normalized'
    db.run_query(CREATE_TBL_POLICY_NORMALIZED)

    # CREATE TABLE 'policy_holders'
    db.run_query(CREATE_TBL_POLICY_HOLDERS)

    # CREATE TABLE 'policy_surplus_records'
    db.run_query(CREATE_TBL_POLICY_SURPLUS_RECORDS)

if __name__ == '__main__':
    main()
