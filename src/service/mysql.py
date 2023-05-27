import pymysql.cursors

class MySQLConnector:
    def __init__(self, host, database, user, password):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.connection = self.connect_to_db()

    def connect_to_db(self):
        """Connect to database"""
        try:
            conn = pymysql.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password,
                cursorclass=pymysql.cursors.DictCursor
            )
            print("Connected to MySQL database")
            return conn
        except pymysql.Error as e:
            print(f"Error wile connecting to MySQL DB: {repr(e)}")

    def close_connection(self):
        """Closing Connection"""
        if self.connection:
            self.connection.close()
            print("MySQL Connection Closed")
