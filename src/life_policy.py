from core.utility import get_resouce, load_yaml
import service.mysql as mysql

# Parameter File Name
param_file_name = "params.yml"

# Get Resources
params = get_resouce(param_file_name, load_yaml)

db = mysql.MySQLConnector(params["mysql_creds"]["host"],
                          params["mysql_creds"]["database"],
                          params["mysql_creds"]["user"],
                          params["mysql_creds"]["password"])

