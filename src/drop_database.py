from core.utility import get_resouce, load_yaml
import service.MySQLConnector as mysql

# SQL QUERY TO SELECT DB
SELECT_DB = """USE life_policy_db;"""


def main():
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

    # Drop Database
    db.drop_database(params["mysql_creds"]["database"])

    #Close Connection
    db.close_connection()

if __name__ == '__main__':
    main()